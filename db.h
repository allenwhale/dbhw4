#ifndef __DB__H__
#define __DB__H__
#pragma GCC push_options
#pragma GCC optimize (3)
#include <bits/stdc++.h>
#define MAX_ROWS (1 << 24)
using namespace std;
typedef struct rawEntry {
    int OriginDest;
    int ArrDelay;
} __attribute__((packed)) rawEntry;
template<class T> class Vector{
    public:
        T *_data;
        unsigned long _max_size, _size;
        Vector(){
            _max_size = 16;
            _size = 0;
            _data = (T*)malloc(sizeof(T) * 16);
        }
        ~Vector(){
            free(_data);
        }
        void push_back(const T &n){
            if(_size >= _max_size){
                _max_size <<= 1;
                _data = (T*)realloc(_data, sizeof(T) * _max_size);
            }
            _data[_size++] = n;
        }
        __attribute__((always_inline)) T& operator [] (int n) {
            return _data[n];
        }
        __attribute__((always_inline)) const T& operator [] (int n) const {
            return _data[n];
        }
        __attribute__((always_inline)) unsigned long size() const {
            return _size;
        }
        __attribute__((always_inline)) void clear() {
            _size = 0;
        }
};
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
        unordered_map<int, Vector<offset>> indexOffset;
        bool index;
        rawEntry *rawData;
        int rowIndex;
        int numOfFile;
        int rawEntrySize;
        char *in;
};
#endif
