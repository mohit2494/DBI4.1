#include "Statistics.h"

Statistics::Statistics()
{
}

Statistics::Statistics(Statistics &copyMe)
{
    map<string,RelStats*> *ptr = copyMe.GetDbStats();
    map<string,RelStats*>::iterator itr;
    RelStats *tbptr;
    //Iterate over the CopyMe HashMap and copy it over
    for(itr=ptr->begin();itr!=ptr->end();itr++)
    {
        tbptr=new RelStats(*itr->second);
        dbStats[itr->first] = tbptr;
    }
}

Statistics::~Statistics()
{
    map<string,RelStats*>::iterator itr;
    RelStats *tb=NULL;
    //Iterate over the HashMap and delete the tablestat objects and then clear the HashMap
    for(itr=dbStats.begin();itr!=dbStats.end();itr++)
    {
        tb = itr->second;
        delete tb;
        tb=NULL;
    }
    dbStats.clear();
}

void Statistics::AddRel(char *relName, int numTuples)
{
 /*Logic:
  If the HashMap contains the relation, update the no of tuples, otherwise add new entry*/
    map<string,RelStats*>::iterator itr;
    RelStats *tbptr;
    itr = dbStats.find(string(relName));
    if(itr!=dbStats.end())
    {
        dbStats[string(relName)]->UpdateData(numTuples);
        dbStats[string(relName)]->SetGroupDetails(relName,1);
    }
    else
    {
        tbptr= new RelStats(numTuples,string(relName));
        dbStats[string(relName)]=tbptr;
    }
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
/*Logic:
  If the HashMap contains the relation, update the attribs in RelStats, otherwise report error*/
  map<string,RelStats*>::iterator itr;
  itr = dbStats.find(string(relName));
  if(itr!=dbStats.end())
  {
      dbStats[string(relName)]->UpdateData(string(attName),numDistincts);
  }
}

#ifdef debug
void Statistics::printRelsAtts()
{
    map<string,RelStats*>::iterator relitr=dbStats.begin();
    for(;relitr!=dbStats.end();relitr++)
    {
        cout<<"\n"<<relitr->first<<" "<<relitr->second->GetTupleCount()<<" "<<relitr->second->GetGrpName();
        map<string,int>::iterator tableitr=relitr->second->GetTableAtts()->begin();
        for(;tableitr!=relitr->second->GetTableAtts()->end();tableitr++)
        {
            cout<<"\n"<<tableitr->first<<":"<<tableitr->second;
        }
    }
}
#endif

void Statistics::CopyRel(char *oldName, char *newName)
{
  /*Logic:
  If the HashMap contains the old relation, copy it over to the new Relation and insert new relation into dbStats
  Else report error*/
  string oldRel=string(oldName);
  string newRel=string(newName);
  if(strcmp(oldName,newName)==0)  return;

  map<string,RelStats*>::iterator itr2;
  itr2 = dbStats.find(newRel);
  if(itr2!=dbStats.end())
  {
      delete itr2->second;
      string temp=itr2->first;
      itr2++;
      dbStats.erase(temp);

  }

  map<string,RelStats*>::iterator itr;

  itr = dbStats.find(oldRel);
  RelStats *tbptr;

  if(itr!=dbStats.end())
  {
      RelStats* newTable=new RelStats(dbStats[string(oldName)]->fetchNumberOfTuples(),newRel);
      tbptr=dbStats[oldRel];
      map<string,int>::iterator tableiter=tbptr->fetchTableAttributes()->begin();
      for(;tableiter!=tbptr->fetchTableAttributes()->end();tableiter++)
      {
          string temp=newRel+"."+tableiter->first;
          newTable->UpdateData(temp,tableiter->second);
      }
      dbStats[string(newName)] = newTable;
  }
  else
  {
      cout<<"\n Class:Statistics Method:CopyRel Msg: invalid relation name:"<<oldName<<endl;
      exit(1);
  }
}

void Statistics::Read(char *fromWhere)
{
     /*Logic:
      Open the File, Fill int the inmemory HashMaps , by scanning from BEGIN to END for
      each Relation
      If the File doesnt exist or if file is empty quit
     */
    FILE *fptr=NULL;
    fptr = fopen(fromWhere,"r");
    char strRead[200];
    while(fptr!=NULL && fscanf(fptr,"%s",strRead)!=EOF)
    {
        if(strcmp(strRead,"BEGIN")==0)
        {
            int tuplecnt=0;
            char relname[200];
            int grpcnt=0;
            char groupname[200];
            fscanf(fptr,"%s %ld %s %d",relname,&tuplecnt,groupname,&grpcnt);
            AddRel(relname,tuplecnt);
            dbStats[string(relname)]->SetGroupDetails(groupname,grpcnt);
            char attname[200];
            int distcnt=0;
            fscanf(fptr,"%s",attname);
            while(strcmp(attname,"END")!=0)
            {
                fscanf(fptr,"%d",&distcnt);
                AddAtt(relname,attname,distcnt);
                fscanf(fptr,"%s",attname);
            }
        }
    }
}

/*
    function for printing the statistics data structure into statistic.txt
    details will be printed according to the relation, numOfTuples, numOfDistinctValues
*/
void Statistics::Write(char *fromWhere)
{
    map<string,int> *ap;
    map<string,RelStats*>::iterator mi;
    map<string,int>::iterator ti;

    FILE *fptr;
    fptr = fopen(fromWhere,"w");
    
     
    for(mi = dbStats.begin();mi!=dbStats.end();mi++) {
        fprintf(fptr,"BEGIN\n");

        long int tc = mi->second->fetchNumberOfTuples();
        const char * rn = mi->first.c_str();
        const char * gn = strdup(mi->second->fetchGroupName().c_str());
        int gz = mi->second->fetchGroupSize();
        fprintf(fptr,"%s %ld %s %d\n",rn,tc,gn,gz);

        ap = mi->second->fetchTableAttributes();
        for(ti = ap->begin();ti!=ap->end();ti++) {
            const char *f = strdup(ti->first.c_str());
            int s = ti->second;
            fprintf(fptr,"%s %d\n",ti->first.c_str(),ti->second);
        }  

        fprintf(fptr,"END\n");      
    }
    fclose(fptr);  
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
 /*
  Call Estimate , round the result and store in the statistics object;
  */
  double r = Estimate(parseTree,relNames,numToJoin);

  long result =(long)((r-floor(r))>=0.5?ceil(r):floor(r));
  string grpName="";
  int grpSize = numToJoin;
  for(int i=0;i<grpSize;i++)
  {
      grpName = grpName + "," + relNames[i];
  }
  map<string,RelStats*>::iterator itr = dbStats.begin();
  for(int i=0;i<numToJoin;i++)
  {
      dbStats[relNames[i]]->SetGroupDetails(grpName,grpSize);
      dbStats[relNames[i]]->UpdateData(result);
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
    if(!checkParseTreeAndPartition(parseTree,relNames,numToJoin,uniqvallist))
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
          tuplevals[dbStats[relNames[i]]->fetchGroupName()]=dbStats[relNames[i]]->fetchNumberOfTuples();
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
    /*Logic:
     * Rules:
     * </> 1/3
     * = 1/Max(noOfDistinct(R1,A1),noOfDistinct(R2,A2)) , where A1,A2 are join attribs
     */
    struct ComparisonOp *comp;
    map<string,double> attribSelectivity;

    while(orList!=NULL)
    {
        comp=orList->left;
        string key = string(comp->left->value);
        if(attribSelectivity.find(key)==attribSelectivity.end())
        {
            attribSelectivity[key]=0.0;
        }
        if(comp->code==1 || comp->code==2)
        {
            attribSelectivity[key] = attribSelectivity[key]+1.0/3;
        }
        else
        {
            string ulkey = string(comp->left->value);
            long max=uniqvallist[ulkey];
            if(comp->right->code==4)
            {
               string urkey = string(comp->right->value);
               if(max<uniqvallist[urkey])
                   max = uniqvallist[urkey];
            }
            attribSelectivity[key] =attribSelectivity[key] + 1.0/max;
        }
        orList=orList->rightOr;
    }

    double selectivity=1.0;
    map<string,double>::iterator itr = attribSelectivity.begin();
    for(;itr!=attribSelectivity.end();itr++)
        selectivity*=(1.0-itr->second);
   // cout<<"\n selectivity"<<1.0-selectivity;
    return (1.0-selectivity);
}

/*
    This function helps pose a defensive check while estimating,
    whether the CNF passed doesn't use any attribute outside
    the list of attributes passed in relations. 
*/
bool Statistics::checkParseTreeAndPartition(struct AndList *tree, char *relationNames[], int numberOfAttributesJoin,map<string,long> &uniqueValueList)
{
    // boolean for return value
    bool returnValue=true;

    // while tree is not parsed and returnValue is not false
    while(tree!=NULL && returnValue)
    {
        // get the left most orList of parse tree
        struct OrList *orListTop=tree->left;

        // traverse the orList until it becomes null and returnValue is not false
        while(orListTop!=NULL && returnValue)
        {
            // get pointer to the comparison operator
            struct ComparisonOp *cmpPtr = orListTop->left;

            // left of comparison operator should be an attribute and right should be a string
            // check whether the attributes used belong to the relations listed in relationNames
            if(!CheckForAttribute(cmpPtr->left->value,relationNames,numberOfAttributesJoin,uniqueValueList) 
                && cmpPtr->left->code==NAME 
                && cmpPtr->code==STRING) {
                cout<<"\n"<< cmpPtr->left->value<<" Does Not Exist";
                returnValue=false;
            }

            // left of comparison operator should be an attribute and right should be a string
            // check whether the attributes used belong to the relations listed in relationNames
            if(!CheckForAttribute(cmpPtr->right->value,relationNames,numberOfAttributesJoin,uniqueValueList) 
                && cmpPtr->right->code==NAME 
                && cmpPtr->code==STRING) {
                returnValue=false;
            }
            // now move to the right OR after we've seen the left one and keep moving until the end
            orListTop=orListTop->rightOr;
        }
        // after the OR parsing is complete, we'll now go to the right OR until the ANDs end
        tree=tree->rightAnd;
    }

    // if false, return
    if(!returnValue) return returnValue;

    // for number of
    map<string,int> tbl;
    for(int i=0;i<numberOfAttributesJoin;i++)
    {
        string gn = dbStats[string(relationNames[i])]->fetchGroupName();
        if(tbl.find(gn)!=tbl.end())
            tbl[gn]--;
        else
            tbl[gn] = dbStats[string(relationNames[i])]->fetchGroupSize()-1;
    }

    map<string,int>::iterator ti;
    for( ti=tbl.begin();ti!=tbl.end();ti++)
    {
        if(ti->second!=0)
        {
            returnValue=false;
            break;
        }
    }
    return returnValue;
}

bool Statistics::CheckForAttribute(char *value,char *relNames[], int numToJoin,map<string,long> &uniqvallist)
{
    int i=0;
    while(i<numToJoin)
    {
    map<string,RelStats*>::iterator itr=dbStats.find(relNames[i]);
    if(itr!=dbStats.end())
     {
        string key = string(value);
        if(itr->second->fetchTableAttributes()->find(key)!=itr->second->fetchTableAttributes()->end())
        {
            uniqvallist[key]=itr->second->fetchTableAttributes()->find(key)->second;
            return true;
        }
     }
    else
        return false;
    i++;
    }
    return false;
}
