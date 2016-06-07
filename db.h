#ifndef __DB__H__
#define __DB__H__
//#pragma GCC push_options
//#pragma GCC optimize (3)
#include <bits/stdc++.h>
#define MAX_ROWS (1 << 24)
using namespace std;
//typedef struct row {
    //char Year[5];
    //char Month[2];
    //char DayofMonth[3];
    //char DayOfWeek[2];
    //char DepTime[5];
    //char CRSDepTime[5];
    //char ArrTime[5];
    //char CRSArrTime[5];
    //char UniqueCarrier[4];
    //char FlightNum[4];
    //char TailNum[8];
    //char ActualElapsedTime[4];
    //char CRSElapsedTime[4];
    //char AirTime[4];
    //char ArrDelay[4];
    //char DepDelay[4];
    //char Origin[4];
    //char Dest[4];
    //char Distance[5];
    //char TaxiIn[4];
    //char TaxiOut[4];
    //char Cancelled[4];
    //char CancellationCode[4];
    //char Diverted[4];
    //char CarrierDelay[4];
    //char WeatherDelay[4];
    //char NASDelay[4];
    //char SecurityDelay[4];
    //char LateAircraftDelay[4];
//} __attribute__((packed)) row_t;
typedef struct rawEntry {
    //char Origin[4];
    //char Dest[4];
    int OriginDest;
    int ArrDelay;
} __attribute__((packed)) rawEntry;
class db{
	public:
		void init();                                     //Do your db initialization.
		void setTempFileDir(const std::string&);                //All the files that created by your program should be located under this directory.
		void import(const std::string&);                        //Inport csv files under this directory.
		void createIndex();                              //Create index on one or two columns.
		double query(const std::string&, const std::string&);          //Do the query and return the average ArrDelay of flights from origin to dest.
		void cleanup();                                  //Release memory, close files and anything you should do to clean up your db class.
        string tempFileDir;
        typedef struct offset {
            int index;
            unsigned long offset, num;
        } offset;
        unordered_map<int, vector<offset>> indexOffset;
        bool index;
        rawEntry *rawData;
        int rowIndex;
        int numOfFile;
        int rawEntrySize;
        char *in;
};
#endif
