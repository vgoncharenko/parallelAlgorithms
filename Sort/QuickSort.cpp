//
//  QuickSort.cpp
//  palgs
//
//  Created by Vitaliy on 01.06.2021.
//
#ifndef QuickSort_hpp
#define QuickSort_hpp

#include "QuickSortNaive.cpp"
#include "BlockBarrier.cpp"
#include "ThreadPool/PersistentThreadPool.cpp"

const float MAX_ASSIGNET_WORK_ON_THREAD = 1.25;
const uint64_t BATCH_SIZE = 134217728; //6; //6000000;
const uint64_t LOCAL_SORT_MAX_BATCH_SIZE = 8388608; //2; //2000000

// O(BATCH_SIZE) + O(n)
void quickSortParallelLocalRearrangementWithStoreInGlobal(I begin,
                                                          const uint64_t size,
                                                          ScalarType pivot,
                                                          std::shared_ptr<GlobalRearrangementResult> globalResult,
                                                          LocalRearrangementResult &localResult
){
#ifdef MY_DEBUG
    std::printf("quickSortParallelLocalRearrangementWithStoreInGlobal size:%llu; pivot=%f\n", size, pivot);
#endif
    quickSortParallelLocalRearrangement(begin, size, pivot, localResult, globalResult);
    quickSortParallelGlobalRearrangement(localResult, globalResult);
}

#ifdef COUNT_COPIES
    static std::atomic<int64_t> copyCounter = 0, nonCopyCounter = 0;
#endif

// split part by taking into account amaunt of recursions
// O(2^(n/BATCH_SIZE) - 1) * (O((n/BATCH_SIZE)/p) * (O(BATCH_SIZE) + O(n))) = O(2^(n/BATCH_SIZE) - 1) * (O(n/p + (n^2)/(p*BATCH_SIZE)))

// local sort part total runs:
// + O(n/(p*LOCAL_SORT_MAX_BATCH_SIZE)) * (O(LOCAL_SORT_MAX_BATCH_SIZE*log2(LOCAL_SORT_MAX_BATCH_SIZE)) + O(LOCAL_SORT_MAX_BATCH_SIZE))
void quickSortWithStableThreadPoolParallel(I vInBegin,
                                           const uint64_t n,
                                           std::shared_ptr<PersistentThreadPool> threadPool,
                                           I bufferBegin,
                                           int8_t originFlag){
#ifdef MY_DEBUG
    std::printf("Start quickSortWithStableThreadPoolParallel N:%llu;\n", n);
#endif
    // O(LOCAL_SORT_MAX_BATCH_SIZE*log2(LOCAL_SORT_MAX_BATCH_SIZE)) + O(LOCAL_SORT_MAX_BATCH_SIZE)
    if (n <= LOCAL_SORT_MAX_BATCH_SIZE) {
        auto begin = vInBegin;
        std::sort(begin, begin + n); //O(LOCAL_SORT_MAX_BATCH_SIZE*log2(LOCAL_SORT_MAX_BATCH_SIZE))
        if (originFlag == 0) {
#ifdef COUNT_COPIES
            ++copyCounter;
#endif
            std::copy(vInBegin, vInBegin + n, bufferBegin); //O(LOCAL_SORT_MAX_BATCH_SIZE)
        }
#ifdef COUNT_COPIES
        else {
            ++nonCopyCounter;
        }
#endif
        return;
    }
    auto pivot = getPivot(vInBegin, n);
    auto globalRearrangeResult = std::make_shared<GlobalRearrangementResult>(bufferBegin, n);
    std::vector<UUID> eventSet;
    
    // O((n/BATCH_SIZE)/p) * (O(BATCH_SIZE) + O(n)) = O(n/p + (n^2)/(p*BATCH_SIZE))
    size_t i = 0;
    auto end = vInBegin + n;
    auto cursor = vInBegin;
    while (cursor < end) {
        UUID eventId = threadPool->generateEventId();
        int curBatchSize = (MAX_ASSIGNET_WORK_ON_THREAD*BATCH_SIZE < (end - cursor))
            ? BATCH_SIZE
            : (end - cursor);
        threadPool->runThread(quickSortParallelLocalRearrangementWithStoreInGlobal,
                              i,
                              eventId,
                              cursor,
                              curBatchSize,
                              pivot,
                              globalRearrangeResult);
        eventSet.push_back(eventId);
        if (cursor + curBatchSize == end) break;
        ++i;
        cursor += BATCH_SIZE;
    }
    threadPool->waitForEvents(eventSet);

    size_t sSize = globalRearrangeResult->sTail - globalRearrangeResult->sBegin;
    std::future<void> smallSide = std::async(std::launch::async,
                                             quickSortWithStableThreadPoolParallel,
                                             globalRearrangeResult->sBegin,
                                             sSize,
                                             threadPool,
                                             vInBegin,
                                             originFlag ^ 1);
    
    quickSortWithStableThreadPoolParallel(globalRearrangeResult->lTail,
                                          n - sSize,
                                          threadPool,
                                          vInBegin + sSize,
                                          originFlag ^ 1);
    
    smallSide.get();
#ifdef MY_DEBUG
    std::printf("End quickSortWithStableThreadPoolParallel N:%llu\n", n);
#endif
}

void testQuickSortWithStableThreadPoolParallel(I vInBegin,
                                               const uint64_t n,
                                               const int8_t threadCount) {
#ifdef MY_VECTOR_VERSION
    auto globalBuffer = std::vector<ScalarType>(n);
    auto globalBufferBegin = globalBuffer.begin();
#else
    auto globalBuffer = new ScalarType [n];
    auto globalBufferBegin = globalBuffer;
#endif
    uint8_t tCount = threadCount > ceil(n*1.0/BATCH_SIZE) ? ceil(n*1.0/BATCH_SIZE) : threadCount;
    auto threadPool = std::make_shared<PersistentThreadPool>(tCount, LOCAL_SORT_MAX_BATCH_SIZE);
    // O
    quickSortWithStableThreadPoolParallel(vInBegin, n, threadPool, globalBuffer, 1);
#ifdef COUNT_COPIES
    std::printf("Copies: %llu\n NoN Copies: %llu\n", copyCounter.load(), nonCopyCounter.load());
#endif
}



#endif /** QuickSort_hpp */
