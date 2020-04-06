#ifndef RELATIONSTATS_
#define RELATIONSTATS_
#include "ParseTree.h"
#include <map>
#include <string.h>
using namespace std;
class RelStats{
    
    long noOfTuples;
    string groupName;
    int groupSize;
    map<string,int> attributeMap;
public:
    RelStats(int noOfTuples, string groupName){
        this.noOfTuples = noOfTuples;
        this.groupName = groupName;
        this.groupSize = 1;
    }
    ~RelStats(){
        attributeMap.clear();
    }
    
    RelStats(RelStats &copyMe){
        this.noOfTuples = copyMe.GetNofTuples();
        map<string,int> * ptr = copyMe.GetRelationAtts();
        map<string,int>::iterator itr;
        
        for( itr = ptr->begin(); itr!=ptr->end(); itr++)
        {
            attributeMap[itr->first] = itr->second;
        }
        this.groupSize = copyMe.getGroupSize();
        this.groupName = copyMe.getGroupName();
    }
        
//   Getter Functions
    string getGroupName()
    {
        return groupName;
    }
    int getGroupSize()
    {
        return groupSize;
    }
    map<string,int> * GetRelationAtts()
    {
        return &attributeMap;
    }
    long GetNofTuples()
    {
        return noOfTuples;
    }

// Setter Functions
    void SetGroupDetails(string grpname,int grpcnt)
    {
        groupName = grpname;
        groupSize = grpcnt;
    }
    
}

#endif
