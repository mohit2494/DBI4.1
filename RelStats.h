#ifndef RELATIONSTATS_
#define RELATIONSTATS_
#include "ParseTree.h"
#include <map>
#include <string.h>
#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>
using namespace std;

/*Container for Table Statistics*/
class RelStats
{
private:
    //friend class Statistics;
    long noOfTuples;
    //long relnoOfTuples;
    map<string,int> attribs;
    string groupName; //initially the name of relation=groupName
    int groupSize;//initially the size = 1 , singleton relation

public:
    /*RelStats(long n,long rn,string grpname):noOfTuples(n),relnoOfTuples(rn),groupName(grpname)
    {
        groupSize=1;
    }*/
    RelStats(long n,string grpname):noOfTuples(n),groupName(grpname)
    {
        groupSize=1;
    }
    RelStats(RelStats &copyMe)
    {

         noOfTuples = copyMe.GetTupleCount();
       //  relnoOfTuples = copyMe.GetRelTupleCount();
         map<string,int> *ptr = copyMe.GetTableAtts();
         map<string,int>::iterator itr;
         for(itr = ptr->begin();itr!=ptr->end();itr++)
         {
             attribs[itr->first] = itr->second;
         }
         groupSize = copyMe.groupSize;
         groupName = copyMe.groupName;
    }
    ~RelStats() { attribs.clear();}

    //Update container Data,Overloaded functions
    void UpdateData(int n)
    {
        noOfTuples = n;
    }

    /*  void UpdateData(int n1,int n2)
    {
        noOfTuples = n1;
        //relnoOfTuples = n2;
    }*/

    void UpdateData(string s,int num_distinct)
    {
        attribs[s] = num_distinct;
    }
    //getter method
    map<string,int> * GetTableAtts()
    {
        return &attribs;
    }
    long GetTupleCount()
    {
        return noOfTuples;
    }
     /* long GetRelTupleCount()
    {
        return relnoOfTuples;
    }*/
    string GetGrpName()
    {
        return groupName;
    }
    int GetGrpSize()
    {
        return groupSize;
    }
    void SetGroupDetails(string grpname,int grpcnt)
    {
        groupName = grpname;
        groupSize = grpcnt;
    }
};


#endif
