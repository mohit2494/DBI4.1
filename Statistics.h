#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include "RelStats.h"
#include <map>
#include <string.h>
#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>

using namespace std;


/*Container for Database Statistics*/
class Statistics
{
private:
    map<string,RelStats*> statsMaps;
public:
    Statistics();
    Statistics(Statistics &copyMe);	 // Performs deep copy
    ~Statistics();
    void AddRel(char *relName, int numTuples);
    void AddAtt(char *relName, char *attName, int numDistincts);
    void CopyRel(char *oldName, char *newName);
    void Read(char *fromWhere);
    void Write(char *fromWhere);
    bool ErrorCheck(struct AndList *parseTree, char *relNames[], int numToJoin,map<string,long> &uniqvallist);
    bool ContainsAttrib(char *value,char *relNames[], int numToJoin,map<string,long> &uniqvallist);
    void Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
    double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
    double Evaluate(struct OrList *orList, map<string,long> &uniqvallist);
    void printRelsAtts();
    //getter methods
    map<string,RelStats*>* GetDbStats()
    {
    return &dbStats;
    }

};
#endif
