//
//  QuickSortNaive.cpp
//  palgs
//
//  Created by Vitaliy on 01.06.2021.
//

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

template<typename I>
auto getPivot(I vInBegin, const uint64_t n) {
    return *(vInBegin+(rand() % n));
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
    
//    ~LocalRearrangementResult(){
//        delete[] S;
//        delete[] L;
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

    auto result = LocalRearrangementResult<ScalarType>{S, curS - S, L, curL - L};
    delete [] S;
    delete [] L;
    
    return result;
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
    //auto buffer = new ScalarType[n];
    auto globalRearrangeResult = std::make_shared<GlobalRearrangementResult<ScalarType>>();
    globalRearrangeResult->sBegin = vInBegin;
    globalRearrangeResult->sTail = vInBegin;
    //globalRearrangeResult->lBegin = buffer + n;
    globalRearrangeResult->lTail = vInBegin + n;
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
    //delete [] buffer;
#endif
#ifdef MY_DEBUG
    std::printf("End testQuickSortParallel N:%llu; threadCount:%d\n", n, threadCount);
#endif
}
