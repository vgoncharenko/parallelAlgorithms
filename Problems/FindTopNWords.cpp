//
//  FindTopNWords.cpp
//  palgs
//
//  Created by Vitaliy on 26.06.2021.
//

#ifndef FindTopNWords_h
#define FindTopNWords_h

#include <string>
#include <unordered_set>
#include <fstream>
#include <streambuf>
#include <utility>
#include <ctype.h>
#include "../Search/TopNModern.cpp"
#include "../Search/TopNOld.cpp"
#include "../measure.cpp"

class FindTopNWords {
private:
    TopNModern<std::string> *topNSearchModern;
    TopNOld *topNSearchOld;
    char* buffer;
    
public:
    FindTopNWords (std::string filePath) {
        auto start = std::chrono::high_resolution_clock::now();
        //std::cin.sync_with_stdio(false);
        //std::cout.sync_with_stdio(false);
        topNSearchModern = new TopNModern<std::string>();
        std::ifstream f(filePath, std::ifstream::in);
        // get pointer to associated buffer object
        std::filebuf* pbuf = f.rdbuf();
        // get file size using buffer's members
        std::size_t size = pbuf->pubseekoff(0, f.end, f.in);
        pbuf->pubseekpos (0, f.in);

        // allocate memory to contain file data
        buffer=new char[size];

        // get file data
        pbuf->sgetn(buffer, size);

        f.close();
        
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
        std::cout << "Read file time:" << (duration.count()/1e9) << std::endl;
    }
    
    std::vector<std::pair<std::string, int>> find_n_modern(int n) {
        return topNSearchModern->getTopN(buffer, n);
    }
    
    std::vector<std::pair<char*, int64_t>> find_n_old(int n) {
        return topNSearchOld->getTopN(buffer, n);
    }
};

void testFindTopNWords(){
    //FindTopNWords f("/Users/vitaliy/Documents/magento/cpp/parallelAlgorithms/data/shakespeare.txt");
    FindTopNWords f("/Users/vitaliy/Documents/magento/cpp/parallelAlgorithms/data/shakespeare_512Mb.txt");
    int n = 10;
    
    std::vector<std::pair<std::string, int>> resultModern;
    measure([&f, n, &resultModern] {
        resultModern = f.find_n_modern(n);
    }, 1, "seconds", "verbose");

    for (auto it = resultModern.rbegin(); it != resultModern.rend(); ++it) {
        std::cout << it->first << " " << it->second << std::endl;
    }
    
    std::vector<std::pair<char*, int64_t>> resultOld;
    measure([&f, n, &resultOld] {
        resultOld = f.find_n_old(n);
    }, 1, "seconds", "verbose");
    
    for (auto it = resultOld.begin(); it != resultOld.end(); ++it) {
        std::cout << it->first << " " << it->second << std::endl;
    }
}

#endif /* FindTopNWords_h */
