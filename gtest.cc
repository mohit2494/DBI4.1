#include <string>
#include <fstream>
#include "DBFile.h"
#include "Statistics.h"
#include <gtest/gtest.h>

extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);
extern "C" int yyparse(void);
extern struct AndList *final;

class FilePath{
    public:
    const std::string dbfile_dir = "dbfiles/"; 
    const std::string tpch_dir ="/home/mk/Documents/uf docs/sem 2/Database Implementation/git/tpch-dbgen/"; 
    const std::string catalog_path = "catalog";
};


TEST(QueryTesting, estimate) {
    Statistics s;
    char *relName[] = {"orders"};
	s.AddRel(relName[0],5057879);
	s.AddAtt(relName[0], "order_flag",30);
	s.AddAtt(relName[0], "order_discount",15);
	s.AddAtt(relName[0], "order_delivery_type",70);
	char *cnf = "(order_flag = 'D') AND (order_discount < 0.04 OR order_delivery_type = 'UPS')";
	yy_scan_string(cnf);
	yyparse();
	double result = s.Estimate(final, relName, 1);
    ASSERT_NEAR(57804.3, result, 0.1);
}

TEST(QueryTesting, apply) {
    Statistics s;
    // joining 3 relations
    char *relName[] = {"t1","t2","t3"};
	s.AddRel(relName[0],1700000);
	s.AddAtt(relName[0], "t1_k1",170000);
	s.AddRel(relName[1],180000);
	s.AddAtt(relName[1], "t2_k1",180000);
	s.AddAtt(relName[1], "t2_k2",50);
	
	s.AddRel(relName[2],100);
	s.AddAtt(relName[2], "t3_k1",7);

	char *cnf = "(t1_k1 = t2_k1)";
	yy_scan_string(cnf);
	yyparse();

	// Join the first two relations in relName
	s.Apply(final, relName, 2);
	
	cnf = " (t2_k2 = t3_k1)";
	yy_scan_string(cnf);
	yyparse();
	
	double result = s.Estimate(final, relName, 3);
	ASSERT_NEAR(3400000,result,0.1);
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}