#include <iostream>
#include <time.h>
#include "db.h"

#define NOINDEX 1
#define INDEX 1

using namespace std;
int main(){
	//declear db object
	db mydb;
    double result1, result2, result3, result4, result5;

	//init db
	mydb.init();

	//set temp directory
	mydb.setTempFileDir("temp");

	//Import data
	clock_t tImport = clock();
	mydb.import("data/2006.csv");
    mydb.import("data/2007.csv");
    mydb.import("data/2008.csv");
	double import_time = (double)(clock() - tImport) / CLOCKS_PER_SEC;
    clock_t NoIndex_s = clock();

    for(int i=0;i<NOINDEX;i++){
        result1 = mydb.query("IAH", "JFK");
        result2 = mydb.query("IAH", "LAX");
        result3 = mydb.query("JFK", "LAX");
        result4 = mydb.query("JFK", "IAH");
        result5 = mydb.query("LAX", "IAH");
    }
    printf("%f %f %f %f %f\n", result1, result2, result3, result4, result5);
    double NoIndex = ((double)(clock() - NoIndex_s) / CLOCKS_PER_SEC) / NOINDEX;

	//Create index on one or two columns.
	clock_t tIndex = clock();
    mydb.createIndex();
	double index_time = (double)(clock() - tIndex) / CLOCKS_PER_SEC;

	//Do queries
	//These queries are required in your report.
	//We will do different queries in the contest.
    //Start timing
    clock_t tQuery = clock();
    for(int i=0;i<INDEX;i++){
        result1 = mydb.query("IAH", "JFK");
        result2 = mydb.query("IAH", "LAX");
        result3 = mydb.query("JFK", "LAX");
        result4 = mydb.query("JFK", "IAH");
        result5 = mydb.query("LAX", "IAH");
    }
    printf("%f %f %f %f %f\n", result1, result2, result3, result4, result5);

	//End timing
	double query_time = ((double)(clock() - tQuery) / CLOCKS_PER_SEC) / INDEX;
	
	printf("Time taken for import: %fs\n", import_time);
	printf("Time taken for creating index: %fs\n", index_time);
	printf("Time taken for making no index queries: %.10fs\n", NoIndex);
	printf("Time taken for making queries: %.10fs\n", query_time);

	//Cleanup db object
	mydb.cleanup();
}
