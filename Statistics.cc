#include "Statistics.h"

Statistics::Statistics()
{
}

Statistics::~Statistics()
{
    map<string,RelStats*>::iterator itr;
    RelStats * rel = NULL;
    for(itr=statsMap.begin(); itr!=statsMap.end(); itr++)
    {
        rel = itr->second;
        delete rel;
        rel = NULL;
    }
    statsMap.clear();
}

map<string,RelStats*>* Statistics::GetStatsMap()
{
    return &statsMap;
}

Statistics::Statistics(Statistics &copyMe)
{
    map<string,RelStats*> * ptr = copyMe.GetStatsMap();
    map<string,RelStats*>::iterator itr;
    RelStats * rel;
    for(itr=ptr->begin(); itr!=ptr->end(); itr++)
    {
        rel = new RelStats(*itr->second);
        statsMap[itr->first] = rel;
    }
}





void Statistics::AddRel(char *relName, int numTuples)
{
    map<string,RelStats*>::iterator itr;
    itr = statsMap.find(string(relName));
    if (itr == statsMap.end()){
        RelStats * rel = new RelStats(numTuples,string(relName));
        statsMap[string(relName)]=rel;
    }
    else{
        statsMap[string(relName)]->UpdateNoOfTuples(numTuples);
        statsMap[string(relName)]->UpdateGroup(relName,1);
    }
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{

  map<string,RelStats*>::iterator itr;
  itr = statsMap.find(string(relName));
  if(itr!=statsMap.end())
  {
      statsMap[string(relName)]->UpdateAttributes(string(attName),numDistincts);
  }
    
}

#ifdef debug
void Statistics::printRelsAtts()
{
    map<string,RelStats*>::iterator relitr=statsMap.begin();
    for(;relitr!=statsMap.end();relitr++)
    {
        cout<<"\n"<<relitr->first<<" "<<relitr->second->GetNofTuples()<<" "<<relitr->second->GetGroupName();
        map<string,int>::iterator tableitr=relitr->second->GetRelationAttributes()->begin();
        for(;tableitr!=relitr->second->GetRelationAttributes()->end();tableitr++)
        {
            cout<<"\n"<<tableitr->first<<":"<<tableitr->second;
        }
    }
}
#endif

void Statistics::CopyRel(char *oldName, char *newName)
{
  string oldRel=string(oldName);
  string newRel=string(newName);
    
  if(strcmp(oldName,newName)==0)  return;

  map<string,RelStats*>::iterator i;
  i = statsMap.find(newRel);
  if(i!=statsMap.end())
  {
      delete i->second;
      string temp=i->first;
      i++;
      statsMap.erase(temp);

  }

  map<string,RelStats*>::iterator itr;

  itr = statsMap.find(oldRel);
  RelStats * oRel;

  if(itr!=statsMap.end())
  {
      oRel = statsMap[oldRel];
      RelStats* nRel=new RelStats(oRel->GetNofTuples(),newRel);
      
      map<string,int>::iterator attritr;
      for(attritr = oRel->GetRelationAttributes()->begin(); attritr!=oRel->GetRelationAttributes()->end();attritr++)
      {
          string s = newRel + "." + attritr->first;
          nRel->UpdateAttributes(s,attritr->second);
      }
      statsMap[string(newName)] = nRel;
  }
  else
  {
      cerr<<"\n No Relation Exist with the name :"<<oldName<<endl;
      exit(1);
  }
}

void Statistics::Read(char *fromWhere)
{
    FILE *fptr=NULL;
    fptr = fopen(fromWhere,"r");
    char buffer[200];
    while(fptr!=NULL && fscanf(fptr,"%s",buffer)!=EOF)
    {
        if(strcmp(buffer,"BEGIN")==0)
        {
            long numTuples=0;
            char relName[200];
            int groupCount=0;
            char groupName[200];
            fscanf(fptr,"%s %ld %s %d",relName,&numTuples,groupName,&groupCount);
            AddRel(relName  ,numTuples);
            statsMap[string(relName)]->UpdateGroup(groupName,groupCount);
            char attName[200];
            int numDistincts=0;
            fscanf(fptr,"%s",attName);
            while(strcmp(attName,"END")!=0)
            {
                fscanf(fptr,"%d",&numDistincts);
                AddAtt(relName,attName,numDistincts);
                fscanf(fptr,"%s",attName);
            }
        }
    }
}

void Statistics::Write(char *fromWhere)
{
    /*Logic:
     Iterate over the statsMap HashMaps, for each entry(relation) iterate over
     attribs HashMaps to print the numOfTuples, and numOfDistinctValues respectivily
     */

     map<string,RelStats*>::iterator dbitr;
     map<string,int>::iterator tbitr;
     map<string,int> *attrptr;

     FILE *fptr;
     fptr = fopen(fromWhere,"w");
     dbitr = statsMap.begin();

     for(;dbitr!=statsMap.end();dbitr++)
     {
         fprintf(fptr,"BEGIN\n");
         fprintf(fptr,"%s %ld %s %d\n",dbitr->first.c_str(),dbitr->second->GetNofTuples(),dbitr->second->GetGroupName().c_str(),dbitr->second->GetGroupSize());
         attrptr = dbitr->second->GetRelationAttributes();
         tbitr = attrptr->begin();

         for(;tbitr!=attrptr->end();tbitr++)
         {
            fprintf(fptr,"%s %d\n",tbitr->first.c_str(),tbitr->second);
         }
         fprintf(fptr,"END\n");
     }
     fclose(fptr);
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
  double r = Estimate(parseTree,relNames,numToJoin);
  long numTuples =(long)((r-floor(r))>=0.5?ceil(r):floor(r));
  string groupName="";
  int groupSize = numToJoin;
  for(int i=0;i<groupSize;i++)
  {
      groupName = groupName + "," + relNames[i];
  }
  for(int i=0;i<numToJoin;i++)
  {
      statsMap[relNames[i]]->UpdateGroup(groupName,groupSize);
      statsMap[relNames[i]]->UpdateNoOfTuples(numTuples);
  }
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
    /* Logic:
     * Check validity of input parameters
     * Loop throught the AND LIST.
     * For each node of AND LIST which is OR LIST evaluate the Selectivity.
     * Do Product of all Selectivities and multiply result with the Tuple product of relations
     */

    double estTuples=1;
    map<string,long> uniqvallist;
    if(!ErrorCheck(parseTree,relNames,numToJoin,uniqvallist))
    {
      cout<<"\nClass:Statistics Method:Estimate Msg:Input Parameters invalid for Estimation";
      return -1.0;
  }
  else
  {
      string grpName="";
      map<string,long>::iterator tupitr;
      map<string,long> tuplevals;
      int grpSize = numToJoin;
      for(int i=0;i<grpSize;i++)
      {
          grpName = grpName + "," + relNames[i];
      }
      for(int i=0;i<numToJoin;i++)
      {
          tuplevals[statsMap[relNames[i]]->GetGroupName()]=statsMap[relNames[i]]->GetNofTuples();
      }

      estTuples = 1000.0; //Safety purpose so that we dont go out of Double precision
      /*long long int tupleproduct=1;
      for(;tupitr!=tuplevals.end();tupitr++)
      {
          tupleproduct*=tupitr->second;
      }*/
     while(parseTree!=NULL)
     {
         estTuples*=Evaluate(parseTree->left,uniqvallist);
         parseTree=parseTree->rightAnd;
     }
      tupitr=tuplevals.begin();
      for(;tupitr!=tuplevals.end();tupitr++)
      {
          estTuples*=tupitr->second;
      }
      //estTuples = estTuples*tupleproduct;
  }
    estTuples = estTuples/1000.0; //Safety purpose so that we dont go out of Double precision-revert
    return estTuples;
}

double Statistics::Evaluate(struct OrList *orList, map<string,long> &uniqvallist)
{
    
    struct ComparisonOp *comp;
    map<string,double> selectionMap;

    while(orList!=NULL)
    {
        comp=orList->left;
        string key = string(comp->left->value);
        if(selectionMap.find(key)==selectionMap.end())
        {
            selectionMap[key]=0.0;
        }
        if(comp->code == LESS_THAN || comp->code == GREATER_THAN)
        {
            selectionMap[key] = selectionMap[key]+1.0/3;
        }
        else
        {
            string leftKeyVal = string(comp->left->value);
            long max_val = uniqvallist[leftKeyVal];
            if(comp->right->code==NAME)
            {
               string rightKeyVal = string(comp->right->value);
               if(max_val<uniqvallist[rightKeyVal])
                   max_val = uniqvallist[rightKeyVal];
            }
            selectionMap[key] =selectionMap[key] + 1.0/max_val;
        }
        orList=orList->rightOr;
    }

    double selectivity=1.0;
    map<string,double>::iterator itr = selectionMap.begin();
    for(;itr!=selectionMap.end();itr++)
        selectivity*=(1.0-itr->second);
    return (1.0-selectivity);
}

bool Statistics::ErrorCheck(struct AndList *parseTree, char *relNames[], int numToJoin,map<string,long> &uniqvallist)
{
    /*
     Logic:
     * 1.Check if all the attributes in parse tree are part of the
     * relations in RelNames.
     * 2.Check if the all Relnames of partitions used, are tin relNames.
     */
  bool result=true;
  while(parseTree!=NULL && result)
  {
      struct OrList *head=parseTree->left;
      while(head!=NULL && result)
      {
          struct ComparisonOp *ptr = head->left;
          if(ptr->left->code==4 && ptr->code==3 && !ContainsAttrib(ptr->left->value,relNames,numToJoin,uniqvallist))
          {
              cout<<"\n"<< ptr->left->value<<" Does Not Exist";
              result=false;
          }
         if(ptr->right->code==4 && ptr->code==3 && !ContainsAttrib(ptr->right->value,relNames,numToJoin,uniqvallist))
              result=false;
       head=head->rightOr;
      }
      parseTree=parseTree->rightAnd;
  }
  if(!result) return result;

  map<string,int> tmpTable;
  for(int i=0;i<numToJoin;i++)
  {
      string grpname = statsMap[string(relNames[i])]->GetGroupName();
      if(tmpTable.find(grpname)!=tmpTable.end())
          tmpTable[grpname]--;
      else
          tmpTable[grpname] = statsMap[string(relNames[i])]->GetGroupSize()-1;
  }

  map<string,int>::iterator tmpTableItr = tmpTable.begin();
  for(;tmpTableItr!=tmpTable.end();tmpTableItr++)
      if(tmpTableItr->second!=0)
         {
          result=false;
          break;
        }
  return result;
}

bool Statistics::ContainsAttrib(char *value,char *relNames[], int numToJoin,map<string,long> &uniqvallist)
{
    int i=0;
    while(i<numToJoin)
    {
    map<string,RelStats*>::iterator itr=statsMap.find(relNames[i]);
    if(itr!=statsMap.end())
     {
        string key = string(value);
        if(itr->second->GetRelationAttributes()->find(key)!=itr->second->GetRelationAttributes()->end())
        {
            uniqvallist[key]=itr->second->GetRelationAttributes()->find(key)->second;
            return true;
        }
     }
    else
        return false;
    i++;
    }
    return false;
}
