//
//  SortAlgorithms.cpp
//  parallelAlgs
//
//  Created by Vitaliy on 31.05.2021.
//

auto getVector(const uint64_t n) {
    auto vector = new float [n];

    for (int i = 0; i < n; ++i) {
        vector[i] = random() / MAXFLOAT;
    }

    return vector;
}

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
class BlockBarrier {
private:
    int count = 0;
    uint64_t infLimit;
public:
    std::condition_variable condition;
    mutable std::mutex m;
    bool syncAllowed = false;

    explicit BlockBarrier (const uint64_t _infLimit) : infLimit(_infLimit) {};

    BlockBarrier& operator++ () {
        {
            std::scoped_lock lk(m);
            ++count;
            if (infLimit > count) return *this;
        }

        if (infLimit <= count) {
            {
                std::scoped_lock lk(m);
                syncAllowed = true;
            }
            condition.notify_all();
        }

        return *this;
    }

    void reset() {
        std::scoped_lock lk(m);
        count = 0;
        syncAllowed = false;
    }
};

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

template<typename I>
void testBubbleSerial(I vInBegin, const uint64_t n){
    for (int i = 0; i < n; ++i) {
        bool swappedFlag = false;
        for (auto it = vInBegin; it < vInBegin + n - i - 1; ++it) {
            auto next = it + 1;
            compareExchange(it, next, asc, std::ref(swappedFlag));
        }
        if (!swappedFlag) break;
    }
}

//template<typename I>
//void testQuickSerial(I vInBegin, const uint64_t n){
//    if (n == 1) return;
//    if (n == 2) {
//        auto next = vInBegin + 1;
//        compareExchange(vInBegin, next, asc);
//        return;
//    }
//    I l = vInBegin;
//    I pivot = vInBegin + n/2;
//    I r = vInBegin + n - 1;
//
//    while (l < r) {
//        if (l+1 == pivot && r-1 == pivot && *l < *pivot && *pivot < *r) {
//            break;
//        }
//        if (*l > *pivot) {
//            std::swap(*l, *pivot);
//        }
//        if (*r < *pivot) {
//            std::swap(*r, *pivot);
//        } else {
//            if (r-1 > pivot) --r;
//        }
//        if (*l < *pivot && *l < *r) {
//            if (l+1 < pivot) ++l;
//        }
//    }
//    testQuickSerial(vInBegin, n/2);
//    testQuickSerial(vInBegin + n/2 + 1, (n%2 ? n/2 : n/2 - 1));
//}

template<typename I>
auto getPivot(I vInBegin, const uint64_t n) {
    return *(vInBegin+(rand() % n));
}

template<typename I>
void testQuickSerial(I vInBegin, const uint64_t n){
    if (n <= 1) return;
    I l = vInBegin;
    I pivot = vInBegin;
    I r = vInBegin + n;

    for (auto it = l+1; it < r; ++it) {
        if (*it <= *pivot) {
            ++l;
            std::swap(*l, *it);
        }
    }
    std::swap(*l, *pivot);
    testQuickSerial(vInBegin, l - pivot);
    testQuickSerial(vInBegin + (l - pivot) + 1, r - (vInBegin + (l - pivot) + 1));
}

/**
 * O(p*(n/p)*log(n/p)) = O(n*log(n/p))
 */
template<typename I>
void testBubbleOddEvenTranspositionParallel(I vInBegin, const uint64_t n, const uint8_t threadCount){
    uint64_t batchSize = n/threadCount/2;
    uint64_t doubleBatchSize = batchSize*2;

    for (int i = 1; i <= threadCount; ++i) {
        std::vector<std::thread> sortStep;
        auto begin = (i%2) ? vInBegin : vInBegin + batchSize;
        for (auto it = begin; it <= begin + (n - doubleBatchSize - ((1-(i%2))*batchSize)); it+=doubleBatchSize) {
            std::thread t{[it, doubleBatchSize] {
                std::sort(it, it + doubleBatchSize);
            }};
            sortStep.push_back(move(t));
        }

        for_each(sortStep.begin(), sortStep.end(), [](std::thread &t) {
            t.join();
        });
    }
}

template<typename T>
struct LocalRearrangementResult
{
    T* S;
    long s_count;
    T* L;
    long l_count;

    LocalRearrangementResult() {
        S = nullptr;
        s_count = 0;
        L = nullptr;
        l_count = 0;
    }

    LocalRearrangementResult(T* _S, long _s_count, T* _L, long _l_count) {
        S = new T [_s_count];
        std::move(_S, _S + _s_count, this->S);
        s_count = _s_count;

        L = new T [_l_count];
        l_count = _l_count;
        std::move(_L, _L + _l_count, this->L);
//        printf("Copy constructor of LocalRearrangementResult");
    }
//
//    LocalRearrangementResult(T&& _Sv1, long _s_count, T&& _Lv1, long _l_count) {
//        S = std::move(_Sv1);
//        s_count = _s_count;
//        L = std::move(_Lv1);
//        l_count = _l_count;
//        printf("Move constructor of LocalRearrangementResult");
//    }
};

template<typename T>
struct GlobalRearrangementResult
{
public:
    T* sBegin;
    T* sTail;
    std::mutex sMutex;

    //T* lBegin;
    T* lTail;
    std::mutex lMutex;
};

typedef float ScalarType;
typedef std::future<LocalRearrangementResult<ScalarType>> LocalRearrangementFuture;
typedef LocalRearrangementResult<ScalarType> ThreadPoolResultSample;

typedef void ThreadPoolCallbackFunction(LocalRearrangementResult<ScalarType> localResult,
                                        std::shared_ptr<GlobalRearrangementResult<ScalarType>> globalResult);
typedef std::future<void> ThreadPoolCallbackFuture;
typedef std::future<void> PersistentThreadPoolFuture;
typedef int ThreadId;
typedef LocalRearrangementFuture ThreadPoolFuture;

class Timeout: public std::exception
{
    virtual const char* what() const throw()
    {
        return "Timeout occurred while waiting for available thread to run.";
    }
} timeoutException;

class NotCopyable: public std::exception
{
    virtual const char* what() const throw()
    {
        return "Instance of not copyable class.";
    }
} notCopyableException;

template<typename I, typename T>
LocalRearrangementResult<ScalarType> quickSortParallelLocalRearrangement(I begin, const uint64_t size, T pivot){
    T* S = new T[size];
    T* L = new T[size];

    I end = begin + size;
    T* curS = S;
    T* curL = L;
    while (begin < end) {
        if (*begin <= pivot) {
            *curS = *begin;
            ++curS;
        } else {
            *curL = *begin;
            ++curL;
        }
        ++begin;
    }

    return LocalRearrangementResult<ScalarType>{S, curS - S, L, curL - L};
}

void quickSortParallelGlobalRearrangement(LocalRearrangementResult<ScalarType> localResult,
                                          std::shared_ptr<GlobalRearrangementResult<ScalarType>> globalResult){
    if (localResult.s_count > 0) {
        std::unique_lock<std::mutex> sLock(globalResult->sMutex);
        auto localBegin = globalResult->sTail;
        globalResult->sTail += localResult.s_count;
        sLock.unlock();
#ifdef MY_NOT_MOVABLE
        std::copy(localResult.S, localResult.S + localResult.s_count, localBegin);
#else
//        localBegin = localResult.S;
        std::move(localResult.S, localResult.S + localResult.s_count, localBegin);
#endif
    }

    if (localResult.l_count > 0) {
        std::unique_lock<std::mutex> lLock(globalResult->lMutex);
        globalResult->lTail -= localResult.l_count;
        auto localBegin = globalResult->lTail;
        lLock.unlock();
#ifdef MY_NOT_MOVABLE
        std::copy(localResult.L, localResult.L + localResult.l_count, localBegin);
#else
//        localBegin = localResult.L;
        std::move(localResult.L, localResult.L + localResult.l_count, localBegin);
#endif
    }
}

template<typename I, typename T>
void quickSortParallelLocalRearrangementWithStoreInGlobal(I begin,
                                                          const uint64_t size,
                                                          T pivot,
                                                          std::shared_ptr<GlobalRearrangementResult<ScalarType>> globalResult){
#ifdef MY_DEBUG
    std::printf("quickSortParallelLocalRearrangementWithStoreInGlobal size:%llu; pivot=%f\n", size, pivot);
#endif
    LocalRearrangementResult<ScalarType> local = quickSortParallelLocalRearrangement(begin, size, pivot);
    quickSortParallelGlobalRearrangement(local, globalResult);
}

struct ThreadSample {
    ThreadId id;
    LocalRearrangementFuture f;
};

struct ThreadCallbackSample {
    ThreadId id;
    ThreadPoolCallbackFuture f;
};

class ThreadPool {
private:
    std::queue<ThreadSample> queue;
    std::queue<ThreadCallbackSample> callbackQueue;
    std::mutex m;
    const uint8_t maxSize;
    std::atomic<int> count = 0;
    std::condition_variable isAvailable;
    std::vector<ThreadPoolResultSample> resultSet;
    ThreadPoolCallbackFunction *callbackFunction;

public:
    explicit ThreadPool (uint8_t _maxSize, ThreadPoolCallbackFunction *_callback)
    : maxSize(_maxSize), callbackFunction(_callback) {
        resultSet = std::vector<ThreadPoolResultSample>(maxSize);
    };

    template<class F, class... Args>
    void runThread(F&& f, int8_t threadId, Args&&... args) {
        std::unique_lock<std::mutex> blockM1Lock(m);
        std::chrono::milliseconds span (100);
        if (isAvailable.wait_for(blockM1Lock, span, [this] { return this->maxSize > this->count; })) {
            count.fetch_add(1, std::memory_order_relaxed);
        } else {
            throw timeoutException;
        }
        blockM1Lock.unlock();
#ifdef MY_DEBUG
        std::printf("Start threadId %d\n", threadId);
#endif
        ThreadPoolFuture fut = std::async(std::launch::async, std::forward<F&&>(f), std::forward<Args&&>(args)...);
        queue.emplace(ThreadSample{threadId, std::move(fut)});
    }

    template<class... callbackArgs>
    void waitForThreads(callbackArgs&&... args) {
        while (!queue.empty()) {
            ThreadSample t = std::move(queue.front());
            queue.pop();
            std::chrono::milliseconds span (100);
            auto res = t.f.wait_for(span);

            if (res == std::future_status::timeout) {
                queue.emplace(std::move(t));
            } else if (res == std::future_status::ready) {
#ifdef MY_DEBUG
                std::printf("Finish threadId %d\n", t.id);
#endif
                resultSet[t.id] = t.f.get();
                this->count.fetch_sub(1, std::memory_order_acq_rel);
                ThreadPoolCallbackFuture fut = std::async(std::launch::async,
                                                          callbackFunction,
                                                          std::forward<ThreadPoolResultSample>(resultSet[t.id]),
                                                          std::forward<callbackArgs&&>(args)...);
                callbackQueue.emplace(ThreadCallbackSample{t.id, std::move(fut)});
#ifdef MY_DEBUG
                std::printf("Start callback threadId %d\n", t.id);
#endif
            }
        }

        while (!callbackQueue.empty()) {
            ThreadCallbackSample t = std::move(callbackQueue.front());
            callbackQueue.pop();
            std::chrono::milliseconds span (100);
            auto res = t.f.wait_for(span);

            if (res == std::future_status::timeout) {
                callbackQueue.emplace(std::move(t));
            } else if (res == std::future_status::ready) {
                t.f.get();
                //resultSet.erase(resultSet.begin() + t.id);
#ifdef MY_DEBUG
                std::printf("Finish callback threadId %d\n", t.id);
#endif
            }
        }
    }
};

struct PersistentThreadSample {
    ThreadId id;
    PersistentThreadPoolFuture f;
};

struct ThreadEvent{
    std::unique_ptr<PersistentThreadSample> threadSample;

    ThreadEvent() = default;

    explicit ThreadEvent (std::unique_ptr<PersistentThreadSample> _threadSample) : threadSample(std::move(_threadSample)){}

};

template<typename ThreadEvent>
class ThreadPoolQueue {
private:
    bool stopQueue = false;
    std::mutex mFront;
    std::mutex mBack;
    std::queue<std::unique_ptr<ThreadEvent>> eventQueue{};
    std::condition_variable eventInQueue;
public:

    std::unique_ptr<ThreadEvent> pullEvent() {
        std::unique_ptr<ThreadEvent> event;
        std::unique_lock<std::mutex> waitLock(mFront);
        eventInQueue.wait(waitLock, [this] { return !this->eventQueue.empty() || this->stopQueue; });
        if (this->stopQueue) {
            return event;
        }
        event = std::move(eventQueue.front());
#ifdef MY_DEBUG
        if (event->threadSample != nullptr)
            std::printf("Pull event from the queue thread# %d\n", event->threadSample->id);
#endif
        eventQueue.pop();
        waitLock.unlock();

        return std::move(event);
    }

    void pushEvent(std::unique_ptr<ThreadEvent> event) {
#ifdef MY_DEBUG
        if (event->threadSample != nullptr)
            std::printf("Push event to the queue thread# %d\n", event->threadSample->id);
#endif
        std::lock_guard<std::mutex> backLock(mBack);
        eventQueue.push(std::move(event));
        eventInQueue.notify_one();
    }

    bool empty() {
        return eventQueue.empty();
    }

    void terminate() {
        stopQueue = true;
        eventInQueue.notify_all();
    }
};

class PersistentThread {
private:
    ThreadPoolQueue<ThreadEvent> *eventQueue;

public:
    bool inProgress = false;
    bool stopSignal = false;

    PersistentThread(const PersistentThread& obj) = delete;
    PersistentThread &  operator=(const  PersistentThread &)  = delete;
    PersistentThread() = default;
    ~PersistentThread() = default;

    explicit PersistentThread(ThreadPoolQueue<ThreadEvent> *_eventQueue)
    : eventQueue(_eventQueue){}

    void run() {
        while (!this->stopSignal) {
            auto event = this->eventQueue->pullEvent();
            if (nullptr == event) return;
            this->inProgress = true;
#ifdef MY_DEBUG
            if (event->threadSample != nullptr)
                std::printf("Process sample for thread# %d\n", int(event->threadSample->id));
#endif
            event->threadSample->f.get();
            this->inProgress = false;
        }
    }
};

class PersistentThreadPool {
private:
    ThreadPoolQueue<ThreadEvent> eventQueue;
    std::vector<PersistentThread*> threadQueue;
    std::vector<std::thread> threads;
    std::mutex m;
    const uint8_t maxSize;
    std::atomic<int> count = 0;
    std::condition_variable isAvailable;
public:
    explicit PersistentThreadPool (uint8_t _maxSize)
            : maxSize(_maxSize) {

        for (int i = 0; i < maxSize; ++i) {
            auto threadInstance = new PersistentThread(&eventQueue);
            std::thread t{[threadInstance]{ threadInstance->run(); }};
            threadQueue.push_back(threadInstance);
            threads.push_back(std::move(t));
        }
    };

    template<class F, class... Args>
    void runThread(F&& func,
                   int8_t threadId,
                   Args&&... args) {
#ifdef MY_DEBUG
        std::printf("Start threadId %d\n", threadId);
#endif
        auto fut = std::async(std::launch::deferred,
                              std::forward<F &&>(func),
                              std::forward<Args &&>(args)...);
        eventQueue.pushEvent(std::make_unique<ThreadEvent>(
                ThreadEvent(std::make_unique<PersistentThreadSample>(
                        PersistentThreadSample{threadId, std::move(fut)}
                ))));
    }

    void waitForThreads() {
        for_each(threadQueue.begin(), threadQueue.end(), [this](const auto &it) {
            std::unique_lock<std::mutex> blockM1Lock(m);
            std::chrono::milliseconds span (10);
            isAvailable.wait_for(blockM1Lock, span, [it] { return it->inProgress; });
        });
    }

    void stopThreads() {
        eventQueue.terminate();
        for_each(threadQueue.begin(), threadQueue.end(), [](const auto &it) {
            it->stopSignal = true;
        });
        for_each(threads.begin(), threads.end(), [](std::thread &t) {
            t.join();
        });
    }

    void terminate() {
        this->waitForThreads();
        this->stopThreads();
    }

    ~PersistentThreadPool () {
        this->terminate();
    }
};

template<typename I>
void testQuickSortWithStableThreadPoolParallel(I vInBegin,
                                               const uint64_t n,
                                               const int8_t threadCount) {
    auto threadPool = std::make_shared<PersistentThreadPool>(threadCount);
    quickSortWithStableThreadPoolParallel(vInBegin, n, threadCount, threadPool);
}

template<typename I>
void quickSortWithStableThreadPoolParallel(I vInBegin,
                                           const uint64_t n,
                                           const int8_t threadCount,
                                           std::shared_ptr<PersistentThreadPool> threadPool){
#ifdef MY_DEBUG
    std::printf("Start testQuickSortParallel N:%llu; threadCount:%d\n", n, threadCount);
#endif
    if (threadCount <= 1) {
        std::sort(vInBegin, vInBegin + n);
        return;
    }
    auto pivot = getPivot(vInBegin, n);
    uint64_t batchSize = n/threadCount;
    auto buffer = new ScalarType[n];
    auto globalRearrangeResult = std::make_shared<GlobalRearrangementResult<ScalarType>>();
    globalRearrangeResult->sBegin = buffer;
    globalRearrangeResult->sTail = buffer;
    //globalRearrangeResult->lBegin = buffer + n;
    globalRearrangeResult->lTail = buffer + n;

    int8_t i = 0;
    auto end = vInBegin + n;
    auto cursor = vInBegin;
    while (cursor < end) {
        int curBatchSize = (2*batchSize < (end - cursor)) ? batchSize : (end - cursor);
        threadPool->runThread(quickSortParallelLocalRearrangementWithStoreInGlobal<I, ScalarType>, i, cursor, curBatchSize, pivot, globalRearrangeResult);
        if (cursor + curBatchSize == end) break;
        ++i;
        cursor += batchSize;
    }

    threadPool->waitForThreads();

    size_t sSize = globalRearrangeResult->sTail - globalRearrangeResult->sBegin;
    int8_t sThreadCount = round(threadCount * (float(sSize) / n));
    std::thread t{quickSortWithStableThreadPoolParallel<ScalarType*>, globalRearrangeResult->sBegin, sSize, sThreadCount, threadPool};
    quickSortWithStableThreadPoolParallel(globalRearrangeResult->lTail, n - sSize, threadCount - sThreadCount, threadPool);
    t.join();
#ifdef MY_NOT_MOVABLE
    std::copy(globalRearrangeResult->sBegin, globalRearrangeResult->sBegin + n, vInBegin);
#else
    vInBegin = globalRearrangeResult->sBegin;
    //std::move(globalRearrangeResult->sBegin, globalRearrangeResult->sBegin + n, vInBegin);
#endif
#ifdef MY_DEBUG
    std::printf("End testQuickSortParallel N:%llu; threadCount:%d\n", n, threadCount);
#endif
}

template<typename I>
void testQuickSortParallel(I vInBegin, const uint64_t n, const int8_t threadCount){
#ifdef MY_DEBUG
    std::printf("Start testQuickSortParallel N:%llu; threadCount:%d\n", n, threadCount);
#endif
    if (threadCount <= 1) {
        std::sort(vInBegin, vInBegin + n);
        return;
    }
    auto pivot = getPivot(vInBegin, n);
    uint64_t batchSize = n/threadCount;
    auto buffer = new ScalarType[n];
    auto globalRearrangeResult = std::make_shared<GlobalRearrangementResult<ScalarType>>();
    globalRearrangeResult->sBegin = buffer;
    globalRearrangeResult->sTail = buffer;
    //globalRearrangeResult->lBegin = buffer + n;
    globalRearrangeResult->lTail = buffer + n;
    auto maxThreadCount = n % threadCount ? threadCount + 1 : threadCount;
    auto threadPool = std::make_unique<ThreadPool>(maxThreadCount, quickSortParallelGlobalRearrangement);

    int8_t i = 0;
    auto end = vInBegin + n;
    auto cursor = vInBegin;
    while (cursor < end) {
        int curBatchSize = (2*batchSize < (end - cursor)) ? batchSize : (end - cursor);
        threadPool->runThread(quickSortParallelLocalRearrangement<I, ScalarType>, i, cursor, curBatchSize, pivot);
        if (cursor + curBatchSize == end) break;
        ++i;
        cursor += batchSize;
    }

    threadPool->waitForThreads(globalRearrangeResult);

    size_t sSize = globalRearrangeResult->sTail - globalRearrangeResult->sBegin;
    int8_t sThreadCount = round(threadCount * (float(sSize) / n));
    std::thread t{testQuickSortParallel<ScalarType*>, globalRearrangeResult->sBegin, sSize, sThreadCount};
    testQuickSortParallel(globalRearrangeResult->lTail, n - sSize, threadCount - sThreadCount);
    t.join();
#ifdef MY_NOT_MOVABLE
    std::copy(globalRearrangeResult->sBegin, globalRearrangeResult->sBegin + n, vInBegin);
#else
    //delete[] vInBegin;
    //vInBegin = globalRearrangeResult->sBegin;
    std::move(globalRearrangeResult->sBegin, globalRearrangeResult->sBegin + n, vInBegin);
#endif
#ifdef MY_DEBUG
    std::printf("End testQuickSortParallel N:%llu; threadCount:%d\n", n, threadCount);
#endif
}

void sanityCheck()
{
//    std::vector<int> input = {3,5,8,9,10,12,14,20,95,90,60,40,35,23,18,0};
//    std::vector<float> input = {10,20,5,9,3,8,12,14,90,0,60,40,23,35,95,18};
//    std::vector<float> input = {223,108,1010,117,18,101,111,115,116,103,105,113,114,10,20,5,9,3,8,12,14,90,0,60,40,23,35,95,106,112,118,119};
//    std::vector<float> input = {10,8,4,1,7,2,11,3,6};
    std::vector<float> input = {7,13,18,2,17,1,14,20,6,10,15,9,3,16,19,4,11,12,5,8};
    std::vector<float> expected(input.size());
    std::copy(input.begin(), input.end(), expected.begin());
    std::sort(expected.begin(), expected.end());
    //testQuickSortWithStableThreadPoolParallel(input.begin(), input.size(), 5);
    verifyVectors(expected, input, input.size());
}

void testVectorSort() {
//    sanityCheck();return;
    uint8_t iterations = 1;
    uint8_t threadsCount = 16;
    auto W = new uint64_t[iterations];
//    W[0] = 1024;
    //W[0] = 8589934592;
//    W[0] = 2147483648;
    W[0] = 1073741824;
//    W[0] = 67108864;
//    Fastest sequential
//    6.10807,
//    Quick parallel with more threads because of ceil:
//    1.38042,
//    Quick parallel with less threads because of round:
//    1.2775,

    uint8_t q = 2;
    for (int i = 1; i < iterations; ++i) {
        W[i] = W[i-1] * q;
    }

    std::vector<std::vector<float>> results(10, std::vector<float>());
    std::vector<std::string> labels(10);
    for (int i = 0; i < iterations; ++i) {
        //float vIn[] = {10,20,5,9,3,8,12,14,90,0,60,40,23,35,95,18,101,111,115,116,103,105,113,114,106,223,108,1010,117,112,118,119};
        int idx=0;
        auto vIn = getVector(W[i]);
#ifdef MY_TEST
        auto expected = new float [W[i]];
        std::copy(vIn, vIn + W[i], expected);
        std::sort(expected, expected + W[i]);
#endif

//        labels[idx] = "Serial\n";
//        auto vOutSerial = new float [W[i]];
//        std::copy(vIn, vIn + W[i], vOutSerial);
//        measure([&W, &vOutSerial, i] {
//            testBitonicSortSerial(vOutSerial, W[i]);
//        }, results[idx++], 10, "seconds", "to_var");
//        verifyVectors(expected, vOutSerial, W[i]);
//        delete [] vOutSerial;

//        labels[idx] = "Bitonic Sort Parallel\n";
//        auto vOutBitonicParallel = new float [W[i]];
//        std::copy(vIn, vIn + W[i], vOutBitonicParallel);
//        measure([&W, &vOutBitonicParallel, threadsCount, i] {
//            testBitonicSortParallel(vOutBitonicParallel, W[i], threadsCount);
//        }, results[idx++], 10, "seconds", "to_var");
//        verifyVectors(expected, vOutBitonicParallel, W[i]);
//        delete [] vOutBitonicParallel;

//        labels[idx] = "Bitonic Sort Parallel Compare-Split Internal Buffer\n";
//        auto vOutBitonicCompareSplitWithInteralBufferParallel = new float [W[i]];
//        std::copy(vIn, vIn + W[i], vOutBitonicCompareSplitWithInteralBufferParallel);
//        measure([&W, &vOutBitonicCompareSplitWithInteralBufferParallel, threadsCount, i] {
//            testBitonicSortCompareSplitWithInternalBufferParallel(vOutBitonicCompareSplitWithInteralBufferParallel, W[i], threadsCount);
//        }, results[idx++], 10, "seconds", "to_var");
//        verifyVectors(expected, vOutBitonicCompareSplitWithInteralBufferParallel, W[i]);
//        delete [] vOutBitonicCompareSplitWithInteralBufferParallel;
//
//        labels[idx] = "Bitonic Sort Parallel Compare-Split With Copy\n";
//        auto vOutBitonicCompareSplitWithCopyParallel = new float [W[i]];
//        std::copy(vIn, vIn + W[i], vOutBitonicCompareSplitWithCopyParallel);
//        measure([&W, &vOutBitonicCompareSplitWithCopyParallel, threadsCount, i] {
//            testBitonicSortCompareSplitWithCopyParallel(vOutBitonicCompareSplitWithCopyParallel, W[i], threadsCount);
//        }, results[idx++], 10, "seconds", "to_var");
//        verifyVectors(expected, vOutBitonicCompareSplitWithCopyParallel, W[i]);
//        delete [] vOutBitonicCompareSplitWithCopyParallel;
//
//        labels[idx] = "Bitonic Sort Parallel Compare-Split Optimized\n";
//        auto vOutBitonicCompareSplitParallel = new float [W[i]];
//        std::copy(vIn, vIn + W[i], vOutBitonicCompareSplitParallel);
//        measure([&W, &vOutBitonicCompareSplitParallel, threadsCount, i] {
//            testBitonicSortCompareSplitParallel(vOutBitonicCompareSplitParallel, W[i], threadsCount);
//        }, results[idx++], 10, "seconds", "to_var");
//        verifyVectors(expected, vOutBitonicCompareSplitParallel, W[i]);
//        delete [] vOutBitonicCompareSplitParallel;

//        labels[idx] = "Bubble Sort Serial\n";
//        auto vOutBubbleSerial = new float [W[i]];
//        std::copy(vIn, vIn + W[i], vOutBubbleSerial);
//        measure([&W, &vOutBubbleSerial, i] {
//            testBubbleSerial(vOutBubbleSerial, W[i]);
//        }, results[idx++], 10, "seconds", "to_var");
//        verifyVectors(expected, vOutBubbleSerial, W[i]);
//        delete [] vOutBubbleSerial;

//        labels[idx] = "Bubble Sort Odd-Even Transposition Parallel\n";
//        auto vOutBubbleOddEvenTranspositionParallel = new float [W[i]];
//        std::copy(vIn, vIn + W[i], vOutBubbleOddEvenTranspositionParallel);
//        measure([&W, &vOutBubbleOddEvenTranspositionParallel, i, threadsCount] {
//            testBubbleOddEvenTranspositionParallel(vOutBubbleOddEvenTranspositionParallel, W[i], threadsCount);
//        }, results[idx++], 1, "seconds", "to_var");
//        //verifyVectors(expected, vOutBubbleOddEvenTranspositionParallel, W[i]);
//        delete [] vOutBubbleOddEvenTranspositionParallel;

/**
 * Very memory efficient:
 *      1) N=1073741824 => mem=4.5Gb and Ts=180.757s
 */
//        labels[idx] = "Fastest sequential \n";
//#ifdef MY_TEST
//        auto vOutFastestSerial = new float [W[i]];
//        std::copy(vIn, vIn + W[i], vOutFastestSerial);
//#else
//        auto vOutFastestSerial = vIn;
//#endif
//        measure([&W, &vOutFastestSerial, i] {
//            std::sort(vOutFastestSerial, vOutFastestSerial+W[i]);
//        }, results[idx++], 1, "seconds", "to_var");
//#ifdef MY_TEST
//        verifyVectors(expected, vOutFastestSerial, W[i]);
//        delete [] vOutFastestSerial;
//#endif

//        labels[idx] = "Quick sequential:\n";
//#ifdef MY_TEST
//        auto vOutQuickSerial = new float [W[i]];
//        std::copy(vIn, vIn + W[i], vOutQuickSerial);
//#else
//        auto vOutQuickSerial = vIn;
//#endif
//        measure([&W, &vOutQuickSerial, i] {
//            testQuickSerial(vOutQuickSerial, W[i]);
//        }, results[idx++], 1, "seconds", "to_var");
//#ifdef MY_TEST
//        verifyVectors(expected, vOutQuickSerial, W[i]);
//        delete [] vOutQuickSerial;
//#endif

/**
 * Very memory unefficient:
 *      1) N=1073741824; p=16 => mem=43Gb; swap=30Gb; and Tp=180.757s;
 */
        labels[idx] = "Quick parallel:\n";
#ifdef MY_TEST
        auto vOutQuickParallel = new float [W[i]];
        std::copy(vIn, vIn + W[i], vOutQuickParallel);
#else
        auto vOutQuickParallel = vIn;
#endif
        measure([&W, &vOutQuickParallel, i, threadsCount] {
            testQuickSortParallel(vOutQuickParallel, W[i], threadsCount);
        }, results[idx++], 1, "seconds", "to_var");
#ifdef MY_TEST
        verifyVectors(expected, vOutQuickParallel, W[i]);
        delete [] vOutQuickParallel;
#endif

//        labels[idx] = "Quick parallel with persistent thread pool:\n";
//#ifdef MY_TEST
//        auto vOutQuickPersistentThreadPoolParallel = new float [W[i]];
//        std::copy(vIn, vIn + W[i], vOutQuickPersistentThreadPoolParallel);
//#else
//        auto vOutQuickPersistentThreadPoolParallel = vIn;
//#endif
//        measure([&W, &vOutQuickPersistentThreadPoolParallel, i, threadsCount] {
//            testQuickSortWithStableThreadPoolParallel(vOutQuickPersistentThreadPoolParallel, W[i], threadsCount);
//        }, results[idx++], 1, "seconds", "to_var");
//#ifdef MY_TEST
//        verifyVectors(expected, vOutQuickPersistentThreadPoolParallel, W[i]);
//        delete [] vOutQuickPersistentThreadPoolParallel;
//#endif

        delete [] vIn;
    }

    for (size_t i=0; i<results.size(); ++i) {
        std::cout << "\n" << labels[i];
        for (auto it: results[i]) {
            std::cout << it << ", ";
        }
    }
}

