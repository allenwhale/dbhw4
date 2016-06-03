#include <iostream>
#include <time.h>
#include "db.h"

using namespace std;
int main(){
	//declear db object
	db mydb;

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
    double result1 = mydb.query("IAH", "JFK");
    double result2 = mydb.query("IAH", "LAX");
    double result3 = mydb.query("JFK", "LAX");
    double result4 = mydb.query("JFK", "IAH");
    double result5 = mydb.query("LAX", "IAH");
    printf("%f %f %f %f %f\n", result1, result2, result3, result4, result5);
    double NoIndex = (double)(clock() - NoIndex_s) / CLOCKS_PER_SEC;

	//Create index on one or two columns.
	clock_t tIndex = clock();
    mydb.createIndex();
	double index_time = (double)(clock() - tIndex) / CLOCKS_PER_SEC;

	//Do queries
	//These queries are required in your report.
	//We will do different queries in the contest.
    //Start timing
    clock_t tQuery = clock();
    result1 = mydb.query("IAH", "JFK");
    result2 = mydb.query("IAH", "LAX");
    result3 = mydb.query("JFK", "LAX");
    result4 = mydb.query("JFK", "IAH");
    result5 = mydb.query("LAX", "IAH");
    printf("%f %f %f %f %f\n", result1, result2, result3, result4, result5);

	//End timing
	double query_time = (double)(clock() - tQuery) / CLOCKS_PER_SEC;
	
	printf("Time taken for import: %fs\n", import_time);
	printf("Time taken for creating index: %fs\n", index_time);
	printf("Time taken for making no index queries: %fs\n", NoIndex);
	printf("Time taken for making queries: %fs\n", query_time);

	//Cleanup db object
	mydb.cleanup();
}