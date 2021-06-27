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
#include "../Search/TopN.cpp"
#include "../measure.cpp"

class FindTopNWords {
private:
    TopNModern<std::string> *topNSearch;
    char* buffer;
    
public:
    FindTopNWords (std::string filePath) {
        auto start = std::chrono::high_resolution_clock::now();
        //std::cin.sync_with_stdio(false);
        //std::cout.sync_with_stdio(false);
        topNSearch = new TopNModern<std::string>();
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
    
    std::vector<std::pair<std::string, int>> find_n(int n) {
        return topNSearch->getTopN(buffer, n);
    }
};

void testFindTopNWords(){
    //FindTopNWords f("/Users/vitaliy/Documents/magento/cpp/parallelAlgorithms/data/shakespeare.txt");
    FindTopNWords f("/Users/vitaliy/Documents/magento/cpp/parallelAlgorithms/data/shakespeare_512Mb.txt");
    int n = 10;
    
    std::vector<std::pair<std::string, int>> result;
    measure([&f, n, &result] {
        result = f.find_n(n);
    }, 1, "seconds", "verbose");
    
    for (auto it = result.rbegin(); it != result.rend(); ++it) {
        std::cout << it->first << " " << it->second << std::endl;
    }
}

#endif /* FindTopNWords_h */
