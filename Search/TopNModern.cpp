//
//  TopN.cpp
//  palgs
//
//  Created by Vitaliy on 26.06.2021.
//
#ifndef TopNModern_h
#define TopNModern_h

#include <vector>
#include <string>
#include <unordered_map>
#include <queue>

class ItemComparator {
public:
    template <typename T>
    int operator() (const std::pair<T, int> &first,
                    const std::pair<T, int> &second) {
        return first.second > second.second;
    }
};

template <typename T>
class TopNModern {
private:
    
    std::unordered_map<std::string, int> buildCoutedDictioary(const char* buffer) {
        std::unordered_map<std::string, int> words;
        std::string word;
        int64_t i = 0;
        while (buffer[i++] != '\0') {
            if (std::isalnum(buffer[i])) {
                word += buffer[i];
            } else {
                if (word.empty()) continue;
                if (auto item = words.insert({std::move(word), 1});
                    !item.second) {
                    ++item.first->second;
                }
                word = "";
            }
        }
        if (!word.empty()) {
            if (auto item = words.insert({std::move(word), 1});
                !item.second) {
                ++item.first->second;
            }
        }
        
        return words;
    }
    
    template <typename It>
    auto buildMinHeap(It begin, const It end, const int maxSize) {
        std::priority_queue<std::pair<T, int>, std::vector<std::pair<T, int>>, ItemComparator> q;
        while (begin != end) {
            q.push(std::make_pair(begin->first, begin->second));
            ++begin;
            
            while (q.size() > maxSize) {
                q.pop();
            }
        }
        
        return q;
    }
    
public:
    std::vector<std::pair<T, int>> getTopN(const char* buffer, const int n) {
        auto map = buildCoutedDictioary(buffer);
        auto minHeap = buildMinHeap(map.begin(), map.end(), n);
        
        std::vector<std::pair<T, int>> result;
        while (!minHeap.empty()) {
            auto tmp = minHeap.top();
            minHeap.pop();
            result.push_back(std::make_pair(tmp.first, tmp.second));
        }
        
        return result;
    }
};

#endif /* TopNModern_h */
