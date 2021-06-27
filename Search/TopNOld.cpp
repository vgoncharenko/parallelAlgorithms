//
//  TopNOld.cpp
//  palgs
//
//  Created by Vitaliy on 27.06.2021.
//
#ifndef TopNOld_h
#define TopNOld_h

#include <utility>
#include <stdlib.h>
#include <cctype>

int cmpByText(const void* s1, const void* s2)
{
    const char** a = (const char**) s1;
    const char** b = (const char**) s2;
    return strcmp(*a, *b);
}

int cmpByFrequency(const void* c1, const void* c2)
{
    const int64_t* a = (const int64_t*) c1;
    const int64_t* b = (const int64_t*) c2;
    
    if (a[1] == b[1]) return 0;
    return a[1] > b[1] ? -1 : 1;
}

class TopNOld {
private:
    enum{WORD_WIDTH = 32, MAX_DICTIONARY_SIZE = 1073741824};
    
    std::pair<char**, int64_t> buildListOfWords(const char* buffer) {
        char** words = new char*[MAX_DICTIONARY_SIZE];
        int64_t i = 0;
        int64_t wordCursor = 0;
        int8_t charCursor = 0;
        words[wordCursor] = new char[WORD_WIDTH];
        while (buffer[i++] != '\0') {
            if (std::isalnum(buffer[i])) {
                words[wordCursor][charCursor] = buffer[i];
                ++charCursor;
            } else {
                if (charCursor == 0) continue;
                
                words[wordCursor][charCursor] = '\0';
                ++wordCursor;
                charCursor = 0;
                words[wordCursor] = new char[WORD_WIDTH];
            }
        }
        if (charCursor != 0) words[wordCursor][charCursor] = '\0';
        
        return std::make_pair(words, ++wordCursor);
    }
    
    auto buildCoutedDictioary(std::pair<char**, int64_t> listOfWords) {
        auto result = new int64_t [listOfWords.second][2];
        int64_t uniqCount = 0;
        
        const char* prev = listOfWords.first[uniqCount];
        result[uniqCount][0] = 0;
        result[uniqCount][1] = 1;
        for (size_t i = 1; i < listOfWords.second; ++i) {
            if (strcmp(listOfWords.first[i], prev) == 0) {
                ++result[uniqCount][1];
            } else {
                prev = listOfWords.first[i];
                ++uniqCount;
                result[uniqCount][0] = i;
                result[uniqCount][1] = 1;
            }
        }
        
        return std::make_pair(result, ++uniqCount);
    }
    
public:
    std::vector<std::pair<char*, int64_t>> getTopN(const char* buffer, const int n) {
        auto wordDictionary = buildListOfWords(buffer);
        qsort(wordDictionary.first, wordDictionary.second, sizeof(char *), cmpByText);

        auto coutedDictionary = buildCoutedDictioary(wordDictionary);
        qsort(coutedDictionary.first, coutedDictionary.second, sizeof(int64_t[2]), cmpByFrequency);
        
        std::vector<std::pair<char*, int64_t>> result;
        for (size_t i = 0; i < n; ++i) {
            result.emplace_back(std::make_pair(wordDictionary.first[coutedDictionary.first[i][0]], coutedDictionary.first[i][1]));
        }
        
        return result;
    }
};

#endif /* TopNOld_h */
