//
//  QuickSortNaive.cpp
//  palgs
//
//  Created by Vitaliy on 01.06.2021.
//
#ifndef QuickSortNaive_hpp
#define QuickSortNaive_hpp

#include <iterator>
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

typedef float ScalarType;
typedef std::vector<ScalarType>::iterator I;

template<typename I>
auto getPivot(I vInBegin, const uint64_t n) {
    return *(vInBegin+(rand() % n));
}

struct LocalRearrangementResult
{
    std::vector<ScalarType> S;
    uint64_t s_count;
    std::vector<ScalarType> L;
    uint64_t l_count;
    
    LocalRearrangementResult() {}

    LocalRearrangementResult(size_t size) {
        S = std::vector<ScalarType>(size);
        s_count = 0;
        L = std::vector<ScalarType>(size);
        l_count = 0;
    }

    LocalRearrangementResult(std::vector<ScalarType>&& _S, size_t _s_count, std::vector<ScalarType>&& _L, size_t _l_count)
    : S(std::move(_S)), s_count(_s_count), L(std::move(_L)), l_count(_l_count) {}
    
//    ~LocalRearrangementResult(){
//        delete[] S;
//        delete[] L;
//    }
    void flushData() {
        s_count = 0;
        l_count = 0;
    }
};

struct GlobalRearrangementResult
{
public:
    I sBegin;
    I sTail;
    std::mutex sMutex;

    //T* lBegin;
    I lTail;
    std::mutex lMutex;
    
    GlobalRearrangementResult(I vBegin, size_t size) {
        sBegin = vBegin;
        sTail = vBegin;
        lTail = vBegin + size;
    }
};

typedef std::future<LocalRearrangementResult> LocalRearrangementFuture;

typedef void ThreadPoolCallbackFunction(LocalRearrangementResult &localResult,
                                        std::shared_ptr<GlobalRearrangementResult> globalResult);
typedef std::future<void> ThreadPoolCallbackFuture;
typedef std::future<void> PersistentThreadPoolFuture;
typedef uint64_t UUID;
typedef size_t ThreadId;
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

template<typename T>
LocalRearrangementResult quickSortParallelLocalRearrangement(I begin,
                                                             const uint64_t size,
                                                             T pivot,
                                                             BlockBarrier *barrier){
    auto result = LocalRearrangementResult(size);
    
    auto S = result.S.begin();
    auto L = result.L.begin();
    
    I start = begin;
    I end = start + size;
    auto curS = S;
    auto curL = L;
    while (start < end) {
        if (*start <= pivot) {
            *curS = *start;
            ++curS;
        } else {
            *curL = *start;
            ++curL;
        }
        ++start;
    }
    
    result.s_count = curS - S;
    result.l_count = curL - L;
    
    // sync with other threads, then perform stage 2
    // add one pass to barrier
    barrier->operator++();
    barrier->condition.notify_one();
    
    return result;
}

void quickSortParallelGlobalRearrangement(LocalRearrangementResult &localResult,
                                          std::shared_ptr<GlobalRearrangementResult> globalResult){
    if (localResult.s_count > 0) {
        std::unique_lock<std::mutex> sLock(globalResult->sMutex);
        auto localBegin = globalResult->sTail;
        globalResult->sTail += localResult.s_count;
        sLock.unlock();
#ifdef MY_NOT_MOVABLE
        std::copy(localResult.S, localResult.S + localResult.s_count, localBegin);
#else
        std::copy(localResult.S.begin(), localResult.S.begin() + localResult.s_count, localBegin);
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
        std::copy(localResult.L.begin(), localResult.L.begin() + localResult.l_count, localBegin);
#endif
    }
}

void quickSortParallelLocalRearrangement(I begin,
                                         const uint64_t size,
                                         ScalarType pivot,
                                         LocalRearrangementResult& result,
                                         std::shared_ptr<GlobalRearrangementResult> globalResult){
    auto S = result.S.begin();
    auto L = result.L.begin();
    bool intermediateFlushPossible = false;
    auto resultBufferSize = result.S.size();
    if (size > resultBufferSize) {
        intermediateFlushPossible = true;
    }
    uint64_t sCount = 0, lCount = 0;
    uint64_t i = 0;
    
    I start = begin;
    I end = start + size;
    auto curS = S;
    auto curL = L;
    while (i < size) {
        if (intermediateFlushPossible) {
            if (sCount == resultBufferSize || lCount == resultBufferSize) {
                result.s_count = sCount;
                result.l_count = lCount;
                quickSortParallelGlobalRearrangement(result, globalResult);
                result.flushData();
                curS = S;
                curL = L;
                sCount = 0;
                lCount = 0;
            }
        }
        auto value = *start;
        if (value <= pivot) {
            *curS = value;
            ++curS;
            ++sCount;
        } else {
            *curL = value;
            ++curL;
            ++lCount;
        }
        ++start;
        ++i;
    }
    
    result.s_count = sCount;
    result.l_count = lCount;
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
    std::vector<LocalRearrangementResult> resultSet;
    ThreadPoolCallbackFunction *callbackFunction;

public:
    explicit ThreadPool (uint8_t _maxSize, ThreadPoolCallbackFunction *_callback)
    : maxSize(_maxSize), callbackFunction(_callback) {
        resultSet = std::vector<LocalRearrangementResult>(maxSize);
    };

    template<class F, class... Args>
    void runThread(F&& f, size_t threadId, Args&&... args) {
        std::unique_lock<std::mutex> blockM1Lock(m);
        std::chrono::milliseconds span (2);
        if (isAvailable.wait_for(blockM1Lock, span, [this] { return this->maxSize > this->count; })) {
            count.fetch_add(1, std::memory_order_relaxed);
        } else {
            throw timeoutException;
        }
        blockM1Lock.unlock();
#ifdef MY_DEBUG
        std::printf("Start threadId %zu\n", threadId);
#endif
        ThreadPoolFuture fut = std::async(std::launch::async, std::forward<F&&>(f), std::forward<Args&&>(args)...);
        queue.emplace(ThreadSample{threadId, std::move(fut)});
    }

    template<class... callbackArgs>
    void waitForThreads(BlockBarrier* barrier, callbackArgs&&... args) {
        while (!queue.empty()) {
            ThreadSample t = std::move(queue.front());
            queue.pop();
            std::chrono::milliseconds span (2);
            auto res = t.f.wait_for(span);

            if (res == std::future_status::timeout) {
                queue.emplace(std::move(t));
            } else if (res == std::future_status::ready) {
#ifdef MY_DEBUG
                std::printf("Finish threadId %zu\n", t.id);
#endif
                // wait for all threads to finish 1 stage
                std::unique_lock<std::mutex> blockM1Lock(barrier->m);
                barrier->condition.wait(blockM1Lock, [&barrier] { return barrier->syncAllowed; });
                resultSet[t.id] = t.f.get();
                this->count.fetch_sub(1, std::memory_order_acq_rel);
                ThreadPoolCallbackFuture fut = std::async(std::launch::async,
                                                          callbackFunction,
                                                          std::ref(resultSet[t.id]),
                                                          std::forward<callbackArgs&&>(args)...);
                callbackQueue.emplace(ThreadCallbackSample{t.id, std::move(fut)});
#ifdef MY_DEBUG
                std::printf("Start callback threadId %zu\n", t.id);
#endif
            }
        }

        while (!callbackQueue.empty()) {
            ThreadCallbackSample t = std::move(callbackQueue.front());
            callbackQueue.pop();
            std::chrono::milliseconds span (2);
            auto res = t.f.wait_for(span);

            if (res == std::future_status::timeout) {
                callbackQueue.emplace(std::move(t));
            } else if (res == std::future_status::ready) {
                t.f.get();
                //resultSet.erase(resultSet.begin() + t.id);
#ifdef MY_DEBUG
                std::printf("Finish callback threadId %zu\n", t.id);
#endif
            }
        }
    }
};

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
    //auto buffer = new ScalarType[n];
    auto globalRearrangeResult = std::make_shared<GlobalRearrangementResult>(vInBegin, n);
//    globalRearrangeResult->sBegin = vInBegin;
//    globalRearrangeResult->sTail = vInBegin;
    //globalRearrangeResult->lBegin = buffer + n;
//    globalRearrangeResult->lTail = vInBegin + n;
    auto maxThreadCount = n % threadCount ? threadCount + 1 : threadCount;
    auto threadPool = std::make_unique<ThreadPool>(maxThreadCount, quickSortParallelGlobalRearrangement);
    auto barrier = new BlockBarrier(maxThreadCount);
    
    size_t i = 0;
    auto end = vInBegin + n;
    auto cursor = vInBegin;
    while (cursor < end) {
        int curBatchSize = (2*batchSize < (end - cursor)) ? batchSize : (end - cursor);
        threadPool->runThread(quickSortParallelLocalRearrangement<ScalarType>,
                              i,
                              cursor,
                              curBatchSize,
                              pivot,
                              barrier
        );
        if (cursor + curBatchSize == end) break;
        ++i;
        cursor += batchSize;
    }

    barrier->infLimit = i+1;
    threadPool->waitForThreads(barrier, globalRearrangeResult);

    size_t sSize = globalRearrangeResult->sTail - globalRearrangeResult->sBegin;
    int8_t sThreadCount = round(threadCount * (float(sSize) / n));
    std::thread t{testQuickSortParallel, globalRearrangeResult->sBegin, sSize, sThreadCount};
    testQuickSortParallel(globalRearrangeResult->lTail, n - sSize, threadCount - sThreadCount);
    t.join();
#ifdef MY_NOT_MOVABLE
    std::copy(globalRearrangeResult->sBegin, globalRearrangeResult->sBegin + n, vInBegin);
#else
    //delete[] vInBegin;
    //vInBegin = globalRearrangeResult->sBegin;
    //std::move(globalRearrangeResult->sBegin, globalRearrangeResult->sBegin + n, vInBegin);
    //delete [] buffer;
#endif
#ifdef MY_DEBUG
    std::printf("End testQuickSortParallel N:%llu; threadCount:%d\n", n, threadCount);
#endif
}

#endif /* QuickSortNaive_hpp */
