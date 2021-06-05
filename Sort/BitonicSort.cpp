//
//  BitonicSort.cpp
//  palgs
//
//  Created by Vitaliy on 01.06.2021.
//
#ifndef BitonicSort_hpp
#define BitonicSort_hpp

#include <iostream>
#include <exception>
#include <complex.h>
#include <future>
#include <chrono>
#include <type_traits>
#include <thread>
#include <mutex>
#include <vector>
#include <stdint.h>
#include <algorithm>
#include <queue>
#include "BlockBarrier.cpp"

auto asc = [](auto const it1, auto const it2) {return *it1 > *it2;};
auto desc = [](auto const it1, auto const it2) {return *it1 < *it2;};

// O(1)
template<typename I, typename cmpFunction>
void compareExchange(I &it1, I &it2, cmpFunction cmp) {
    if (cmp(it1, it2)) {
        auto tmp = *it2;
        *it2 = *it1;
        *it1 = tmp;
    }
}

// O(1)
template<typename I, typename cmpFunction>
void compareExchange(I &it1, I &it2, cmpFunction cmp, bool &swappedFlag) {
    if (cmp(it1, it2)) {
        compareExchange(it1, it2, cmp);
        swappedFlag = true;
    }

}

// O(n)
template<typename I, typename cmpFunction>
void bitonicSplit(I it1, I it2, cmpFunction cmp) {
    I end = it2;
    while (it1 != end) {
        compareExchange(it1, it2, cmp);
        ++it1; ++it2;
    }
}

// O(log2(n)*O(n))
template<typename I, typename cmpFunction>
void bitonicMerge(I begin, const uint64_t n, cmpFunction cmp) {
    for (int i = 1; i <= log2(n); ++i) {
        const int chunk = n/pow(2, i);
        const int doubleChunk = chunk * 2;
        for (int j = 0; j < n/doubleChunk; ++j) {
            bitonicSplit(begin + j*doubleChunk, begin + j*doubleChunk + chunk, cmp);
        }
    }
}

// O(log2(n/2)*O(n*log2(n)))
template<typename I>
void bitonicSort(I begin, const uint64_t n) {
    for (int i = 0; i < log2(n/2); ++i) {
        const int chunk = pow(2, i);
        const int doubleChunk = chunk * 2;
        for (int j = 0; j < n/doubleChunk; ++j) {
            if (j%2) bitonicMerge(begin + j*doubleChunk, doubleChunk, desc);
            else bitonicMerge(begin + j*doubleChunk, doubleChunk, asc);
        }
    }
}

// O(n)
template<typename T>
void compareSplitWithInternalBuffer(T* begin,
                  const uint64_t n,
                  bool directionFlag,
                  BlockBarrier* barrier) {
    T* end = begin + n;
    T* bufferBegin = new T[n];
    T* it = bufferBegin;
    T* curBegin = begin;
    int i = 0, j = 0;

    if (directionFlag) {
        T* endLover = curBegin - 1;
        T* endCur = end - 1;
        while (i+j < n) {
            if (curBegin <= (endCur-i) && *(endCur-i) > *(endLover-j)) {
                *it = *(endCur-i);
                ++i;
            } else {
                *it = *(endLover-j);
                ++j;
            }
            ++it;
        }
        std::reverse(bufferBegin, bufferBegin + n);
    } else {
        while (i+j < n) {
            if (i < n && *(begin+j) > *(end + i)) {
                *it = *(end + i);
                ++i;
            } else {
                *it = *(begin+j);
                ++j;
            }
            ++it;
        }
    }
    //sync with other part, then substitute
    // add one pass to barrier
    barrier->operator++();
    // wait for opposite side to finish compare
    std::unique_lock<std::mutex> blockM1Lock(barrier->m);
    barrier->condition.wait(blockM1Lock, [&barrier] { return barrier->syncAllowed; });

    // perform split
    for (int k = 0; k < n; ++k) {
        *(begin + k) = *(bufferBegin + k);
    }
}

// O(n)
template<typename T>
void compareSplitWithCopy(T* begin,
                  T* bufferBegin,
                  const uint64_t n,
                  bool directionFlag,
                  BlockBarrier* barrier) {
    T* end = begin + n;
    T* it = bufferBegin;
    T* curBegin = begin;
    int i = 0, j = 0;

    if (directionFlag) {
        T* endLover = curBegin - 1;
        T* endCur = end - 1;
        while (i+j < n) {
            if (curBegin <= (endCur-i) && *(endCur-i) > *(endLover-j)) {
                *it = *(endCur-i);
                ++i;
            } else {
                *it = *(endLover-j);
                ++j;
            }
            ++it;
        }
        std::reverse(bufferBegin, bufferBegin + n);
    } else {
        while (i+j < n) {
            if (i < n && *(begin+j) > *(end + i)) {
                *it = *(end + i);
                ++i;
            } else {
                *it = *(begin+j);
                ++j;
            }
            ++it;
        }
    }
    //sync with other part, then substitute
    // add one pass to barrier
    barrier->operator++();
    // wait for opposite side to finish compare
    std::unique_lock<std::mutex> blockM1Lock(barrier->m);
    barrier->condition.wait(blockM1Lock, [&barrier] { return barrier->syncAllowed; });

    // perform split
    for (int k = 0; k < n; ++k) {
        *(begin + k) = *(bufferBegin + k);
    }
}

// O(n)
template<typename T>
void compareSplit(T* begin,
                  T* bufferBegin,
                  const uint64_t n,
                  bool directionFlag) {
    T* end = begin + n;
    T* it = bufferBegin;
    T* curBegin = begin;
    int i = 0, j = 0;

    if (directionFlag) {
        T* endLover = curBegin - 1;
        T* endCur = end - 1;
        while (i+j < n) {
            if (curBegin <= (endCur-i) && *(endCur-i) > *(endLover-j)) {
                *it = *(endCur-i);
                ++i;
            } else {
                *it = *(endLover-j);
                ++j;
            }
            ++it;
        }
        std::reverse(bufferBegin, bufferBegin + n);
    } else {
        while (i+j < n) {
            if (i < n && *(begin+j) > *(end + i)) {
                *it = *(end + i);
                ++i;
            } else {
                *it = *(begin+j);
                ++j;
            }
            ++it;
        }
    }
}

// O(log2(n/2)*O(n*log2(n))) + O(log2(n)*O(n)) ~ O(n*(log2(n))^2)
template<typename I, typename cmpFunction>
void bitoicSortSerial(I vInBegin, const uint64_t n, cmpFunction cmp) {
    bitonicSort(vInBegin, n);
    bitonicMerge(vInBegin, n, cmp);
}

// O(O(log2(n/p/2)*O(n/p*log2(n/p))) + O(log2(n/p)*O(n/p))) + O(log2(p)-1) * O(log2(n) * O(n))
template<typename T>
void bitonicSortParallel(T* begin, const uint64_t n, const uint8_t threadCount) {
    std::function<bool(const T*, const T*)> ascLocal = [](auto const it1, auto const it2) {return *it1 > *it2;};
    std::function<bool(const T*, const T*)> descLocal = [](auto const it1, auto const it2) {return *it1 < *it2;};

        // Sort blocks internally
    // O(O(log2(n/p/2)*O(n/p*log2(n/p))) + O(log2(n/p)*O(n/p)))
    std::vector<std::thread> sortStep;
    for (int i = 0; i < threadCount; ++i) {
        std::thread t{bitoicSortSerial<T*, std::function<bool(const T*, const T*)>>,
                      begin + n/threadCount * i,
                      n/threadCount,
                      (i%2) ? descLocal : ascLocal
        };
        sortStep.push_back(move(t));
    }
    for_each(sortStep.begin(), sortStep.end(), [](std::thread &t) {
        t.join();
    });

    //Perform Only Merges
    // O(log2(n/p/2) * O(n/p/2)) + O(log2(n/p/4) * O(n/p/4)) + ... + O(log2(n) * O(n))
    // with small enough p:
    // O(log2(p)-1) * O(log2(n) * O(n))
    for (int k = 1; k <= log2(threadCount); ++k) {
        const int threads = threadCount / pow(2, k);
        const uint64_t blockSize = n / threads;
        std::vector<std::thread> mergeStep;
        for (int i = 0; i < threads; ++i) {
            std::thread t{bitonicMerge<T*, std::function<bool(const T*, const T*)>>,
                          begin + blockSize * i,
                          blockSize,
                          (i%2) ? descLocal : ascLocal
            };
            mergeStep.push_back(move(t));
        }
        for_each(mergeStep.begin(), mergeStep.end(), [](std::thread &t) {
            t.join();
        });
    }
}

// O(O(log2(n/p/2)*O(n/p*log2(n/p))) + O(log2(n/p)*O(n/p))) + O(log2(p)-1) * O(log2(n) * O(n))
template<typename T>
void testBitonicSortCompareSplitParallel(T* begin, const uint64_t n, const uint8_t threadCount) {
    // Sort blocks internally
    // O(O(log2(n/p/2)*O(n/p*log2(n/p))) + O(log2(n/p)*O(n/p)))
    std::vector<std::thread> sortStep;
    for (int i = 0; i < threadCount; ++i) {
        std::thread t{bitoicSortSerial<T*, std::function<bool(const T*, const T*)>>,
                      begin + n/threadCount * i,
                      n/threadCount,
                      asc
        };
        sortStep.push_back(move(t));
    }
    for_each(sortStep.begin(), sortStep.end(), [](std::thread &t) {
        t.join();
    });

    //Perform Only Compare-Splits
    // O(n/p/2 + n/p/4 + ... + n) ~ O(n)
    T* buffer1 = new T[n];
    T* buffer2 = begin;
    for (int k = 0; k < log2(threadCount); ++k) {
        T* primary = (k%2) ? buffer1 : buffer2;
        T* replica = (k%2) ? buffer2 : buffer1;
        const uint64_t threads = threadCount / pow(2, k);
        const uint64_t blockSize = n / threads;
        std::vector<std::thread> mergeStep;

        for (int i = 0; i < threads; i+=2) {
            std::thread lower{compareSplit<T>,
                              primary + blockSize * i,
                              replica + blockSize * i,
                              blockSize,
                              false
            };
            std::thread upper{compareSplit<T>,
                              primary + blockSize * i + blockSize,
                              replica + blockSize * i + blockSize,
                              blockSize,
                              true
            };
            mergeStep.push_back(move(lower));
            mergeStep.push_back(move(upper));
        }
        for_each(mergeStep.begin(), mergeStep.end(), [](std::thread &t) {
            t.join();
        });
    }
    int k = log2(threadCount);
    if (k%2) {
        begin = buffer1;
    }
    delete [] buffer1;
}

// O(O(log2(n/p/2)*O(n/p*log2(n/p))) + O(log2(n/p)*O(n/p))) + O(log2(p)-1) * O(log2(n) * O(n))
template<typename T>
void testBitonicSortCompareSplitWithCopyParallel(T* begin, const uint64_t n, const uint8_t threadCount) {
    // Sort blocks internally
    // O(O(log2(n/p/2)*O(n/p*log2(n/p))) + O(log2(n/p)*O(n/p)))
    std::vector<std::thread> sortStep;
    for (int i = 0; i < threadCount; ++i) {
        std::thread t{bitoicSortSerial<T*, std::function<bool(const T*, const T*)>>,
                      begin + n/threadCount * i,
                      n/threadCount,
                      asc
        };
        sortStep.push_back(move(t));
    }
    for_each(sortStep.begin(), sortStep.end(), [](std::thread &t) {
        t.join();
    });

    //Perform Only Compare-Splits
    // O(n/p/2 + n/p/4 + ... + n) ~ O(n)
    T* buffer = new T[n];
    for (int k = 0; k < log2(threadCount); ++k) {
        const uint64_t threads = threadCount / pow(2, k);
        const uint64_t blockSize = n / threads;
        auto barrier = new BlockBarrier(threads);
        std::vector<std::thread> mergeStep;

        for (int i = 0; i < threads; i+=2) {
            std::thread lower{compareSplitWithCopy<T>,
                              begin + blockSize * i,
                              buffer + blockSize * i,
                              blockSize,
                              false,
                              std::ref(barrier)
            };
            std::thread upper{compareSplitWithCopy<T>,
                              begin + blockSize * i + blockSize,
                              buffer + blockSize * i + blockSize,
                              blockSize,
                              true,
                              std::ref(barrier)
            };
            mergeStep.push_back(move(lower));
            mergeStep.push_back(move(upper));
        }
        for_each(mergeStep.begin(), mergeStep.end(), [](std::thread &t) {
            t.join();
        });
    }
    delete [] buffer;
}

// O(O(log2(n/p/2)*O(n/p*log2(n/p))) + O(log2(n/p)*O(n/p))) + O(log2(p)-1) * O(log2(n) * O(n))
template<typename T>
void testBitonicSortCompareSplitWithInternalBufferParallel(T* begin, const uint64_t n, const uint8_t threadCount) {
    // Sort blocks internally
    // O(O(log2(n/p/2)*O(n/p*log2(n/p))) + O(log2(n/p)*O(n/p)))
    std::vector<std::thread> sortStep;
    for (int i = 0; i < threadCount; ++i) {
        std::thread t{bitoicSortSerial<T*, std::function<bool(const T*, const T*)>>,
                      begin + n/threadCount * i,
                      n/threadCount,
                      asc
        };
        sortStep.push_back(move(t));
    }
    for_each(sortStep.begin(), sortStep.end(), [](std::thread &t) {
        t.join();
    });

    //Perform Only Compare-Splits
    // O(n/p/2 + n/p/4 + ... + n) ~ O(n)
    for (int k = 0; k < log2(threadCount); ++k) {
        const uint64_t threads = threadCount / pow(2, k);
        const uint64_t blockSize = n / threads;
        auto barrier = new BlockBarrier(threads);
        std::vector<std::thread> mergeStep;

        for (int i = 0; i < threads; i+=2) {
            std::thread lower{compareSplitWithInternalBuffer<T>,
                              begin + blockSize * i,
                              blockSize,
                              false,
                              std::ref(barrier)
            };
            std::thread upper{compareSplitWithInternalBuffer<T>,
                              begin + blockSize * i + blockSize,
                              blockSize,
                              true,
                              std::ref(barrier)
            };
            mergeStep.push_back(move(lower));
            mergeStep.push_back(move(upper));
        }
        for_each(mergeStep.begin(), mergeStep.end(), [](std::thread &t) {
            t.join();
        });
    }
}

template<typename I>
void testBitonicSortSerial(I vInBegin, const uint64_t n) {
    bitoicSortSerial(vInBegin, n, asc);
}

template<typename I>
void testBitonicSortParallel(I vInBegin, const uint64_t n, const uint8_t threadCount) {
    bitonicSortParallel(vInBegin, n, threadCount);
}

#endif /* BitonicSort_hpp */
