//#pragma GCC push_options
//#pragma GCC optimize (3)
#include <bits/stdc++.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include "db.h"
#define strchr(x, c) (char*)rawmemchr((x), (c))
using namespace std;

void db::init(){
    index = false;
    rowIndex = 0;
    numOfFile = 0;
    rawData = new rawEntry[MAX_ROWS + 10];
    memset(rawData, 0, sizeof(rawEntry) * (MAX_ROWS + 10));
    rawEntrySize = sizeof(rawEntry);
    in = new char [sizeof(int) * (MAX_ROWS + 10)];
}

void db::setTempFileDir(const string& dir){
    if(dir.back() != '/')
        tempFileDir = dir + "/";
    else 
        tempFileDir = dir;
}

//__attribute__((optimize("unroll-loops", "inline")))
//__attribute__((always_inline))
inline int Atoi(register const char *s, const char *e){
    register int res = 0;
    bool neg = false;
    if(*s == '-') neg = true, ++s;
    do{
        res = res * 10 + *s - '0';
    }while(++s != e);
    if(neg) return -res;
    return res;
}
//__attribute__((optimize("unroll-loops", "inline")))
//__attribute__((always_inline))
inline int hash_string(const char *origin, int n) {
    //if(n!=3)puts("error"), printf("%d\n", n);
    register int res = 0;
    for(register int i=3;i--;)
        res = res << 5 | (*(origin++) & 0x1F);
    return res;
    //return origin[2] | origin[1] << 5 | origin[0] << 10;
}
void db::import(const string& csvFile){
    struct stat st;
    stat(csvFile.c_str(), &st);
    int fd = open(csvFile.c_str(), O_RDONLY);
    char *inptr = (char*)mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    register char *split = strchr(inptr, '\n') + 1, *last, *ptr, *end = inptr + st.st_size;
    register rawEntry *entry;
    while(split < end){
        last = split, ptr = strchr(last, ',');
        last = ptr + 1, ptr = strchr(last, ',');
        last = ptr + 1, ptr = strchr(last, ',');
        last = ptr + 1, ptr = strchr(last, ',');
        last = ptr + 1, ptr = strchr(last, ',');
        last = ptr + 1, ptr = strchr(last, ',');
        last = ptr + 1, ptr = strchr(last, ',');
        last = ptr + 1, ptr = strchr(last, ',');
        last = ptr + 1, ptr = strchr(last, ',');
        last = ptr + 1, ptr = strchr(last, ',');
        last = ptr + 1, ptr = strchr(last, ',');
        last = ptr + 1, ptr = strchr(last, ',');
        last = ptr + 1, ptr = strchr(last, ',');
        last = ptr + 1, ptr = strchr(last, ',');
        last = ptr + 1, ptr = strchr(last, ',');
        //ArrDelay
        if(*last != 'N'){
            entry = rawData + rowIndex;
            entry->ArrDelay = Atoi(last, ptr);
            //entry->ArrDelay = atoi(last);
            last = ptr + 1, ptr = strchr(last, ',');
            last = ptr + 1, ptr = strchr(last, ',');
            //Origin
            entry->OriginDest = hash_string(last, ptr - last);
            last = ptr + 1, ptr = strchr(last, ',');
            //Dest
            entry->OriginDest = entry->OriginDest << 16 | hash_string(last, ptr - last);
            rowIndex++;
            if(rowIndex == MAX_ROWS){
                fd = open((tempFileDir + to_string(numOfFile++)).c_str(), O_RDWR | O_CREAT, 0755);
                lseek(fd, rawEntrySize * MAX_ROWS + 1, SEEK_SET);
                write(fd, "", 1);
                lseek(fd, 0, SEEK_SET);
                char *outptr = (char*)mmap(NULL, rawEntrySize * MAX_ROWS, PROT_WRITE | PROT_READ, MAP_SHARED, fd, 0);
                memcpy(outptr, (const void*)rawData, rawEntrySize * MAX_ROWS);
                munmap(outptr, rawEntrySize * MAX_ROWS);
                close(fd);
                rowIndex = 0;
            }
        }
        split = strchr(ptr + 20, '\n') + 1;
    }
    munmap(inptr, st.st_size);
    if(rowIndex){
        fd = open((tempFileDir + to_string(numOfFile++)).c_str(), O_RDWR | O_CREAT, 0755);
        lseek(fd, rawEntrySize * rowIndex + 1, SEEK_SET);
        write(fd, "", 1);
        lseek(fd, 0, SEEK_SET);
        char *outptr = (char*)mmap(NULL, rawEntrySize * rowIndex, PROT_WRITE | PROT_READ, MAP_SHARED, fd, 0);
        memcpy(outptr, (const void*)rawData, rawEntrySize * rowIndex);
        munmap(outptr, rawEntrySize * rowIndex);
        close(fd);
        rowIndex = 0;
    }
}

void db::createIndex() {
	//Create index.
    register rawEntry *entry;
    unordered_map<long long, Vector<int>> indexed;
    string fileName;
    for(register int i=numOfFile;i--;){
        fileName = tempFileDir + to_string(i);
        struct stat st;
        stat(fileName.c_str(), &st);
        int fd = open((tempFileDir + to_string(i)).c_str(), O_RDONLY);
        char *inptr = (char*)mmap(0, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);
        int rowCnt = st.st_size / rawEntrySize;
        entry = (rawEntry*)inptr;
        for(register int j=rowCnt;j--;++entry){
            indexed[entry->OriginDest].push_back(entry->ArrDelay);
        }
        munmap(inptr, st.st_size);
        register int totalOffset = 0;
        fd = open(fileName.c_str(), O_RDWR | O_CREAT, 0755);
        lseek(fd, rowCnt * sizeof(int) + 1, SEEK_SET);
        write(fd, "", 1);
        lseek(fd, 0, SEEK_SET);
        register char *outptr = (char*)mmap(0, rowCnt * sizeof(int), PROT_WRITE | PROT_READ, MAP_SHARED, fd, 0);
        for(auto &item: indexed){
            indexOffset[item.first].push_back({i, totalOffset * sizeof(int), item.second.size()});
            memcpy(outptr + totalOffset * sizeof(int), (const void*)(int*)&item.second[0], sizeof(int) * item.second.size());
            //write(fd, (const void*)(int*)&item.second[0], sizeof(int) * item.second.size());
            totalOffset += item.second.size();
            item.second.clear();
        }
        munmap(outptr, totalOffset * sizeof(int));
        close(fd);
    }
    index = true;
}

double db::query(const string& origin, const string& dest){
	//Do the query and return the average ArrDelay of flights from origin to dest.
	//This method will be called multiple times.
    int hashed = hash_string(origin.c_str(), origin.size()) << 16 | hash_string(dest.c_str(), dest.size());
    if(index) {
        register int sum = 0, cnt = 0, *ptr;
        auto &indexOffsetItem = indexOffset[hashed];
        for(register int i=(int)indexOffsetItem.size();i--;){
            offset &off = indexOffsetItem[i];
            int fd = open((tempFileDir + to_string(off.index)).c_str(), O_RDONLY);
            lseek(fd, off.offset, SEEK_SET);
            read(fd, in, off.num * sizeof(int));
            close(fd);
            ptr = ((int*)in) - 1;
            //for(register unsigned long j=off.num;j--;){
                //sum += *(++ptr);
            //}
            int num = off.num;
            if(num & 1){
                sum += *(++ptr);
                num &= ~1;
            }
            for(register int j=num>>1;j--;){
                sum += *(++ptr);
                sum += *(++ptr);
            }
            cnt += off.num;
        }
        //printf("%d %d\n", sum, cnt);
        return (double)sum / (double)cnt;
    } else {
        register int sum = 0, cnt = 0;
        register rawEntry *entry;
        for(register int i=numOfFile;i--;){
            string fileName = tempFileDir + to_string(i);
            struct stat st;
            stat(fileName.c_str(), &st);
            int fd = open(fileName.c_str(), O_RDONLY);
            char *inptr = (char*)mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
            int rowCnt = st.st_size / rawEntrySize;
            entry = (rawEntry*)inptr;
            if(rowCnt & 1){
                if(entry->OriginDest == hashed){
                    sum += entry->ArrDelay;
                    ++cnt;
                }
                rowCnt &= ~1;
                ++entry;
            }
            for(register int j=rowCnt>>1;j--;){
                //printf("%d\n", j);
                if(entry->OriginDest == hashed){
                    sum += entry->ArrDelay;
                    ++cnt;
                }
                ++entry;
                if(entry->OriginDest == hashed){
                    sum += entry->ArrDelay;
                    ++cnt;
                }
                ++entry;
            }
            //for(register int j=rowCnt;j--;++entry){
                ////printf("%d\n", j);
                //if(entry->OriginDest == hashed){
                    //sum += entry->ArrDelay;
                    //++cnt;
                //}
            //}
            close(fd);
            munmap(inptr, st.st_size);
        }
            //printf("%d %d\n", sum, cnt);
        return (double)sum / (double)cnt;
    } 
	return 0; //Remember to return your result.
}

void db::cleanup(){
	//Release memory, close files and anything you should do to clean up your db class.
    for(int i=0;i<numOfFile;i++)
        remove((tempFileDir + "/" + to_string(i)).c_str());
    //printf("%d %d\n", test.size(), indexOffset.size());
    delete [] rawData;
    delete [] in;
}
#undef strchr
