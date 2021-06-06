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
const float BATCH_SIZE = 67108864;
const float LOCAL_SORT_MAX_BATCH_SIZE = 8388608;

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



void quickSortWithStableThreadPoolParallel(I vInBegin,
                                           const uint64_t n,
                                           std::shared_ptr<PersistentThreadPool> threadPool,
                                           I bufferBegin){
#ifdef MY_DEBUG
    std::printf("Start testQuickSortParallel N:%llu; threadCount:%d\n", n, threadCount);
#endif
    if (n <= LOCAL_SORT_MAX_BATCH_SIZE) {
        auto begin = vInBegin;
        std::sort(begin, begin + n);
        return;
    }
    auto pivot = getPivot(vInBegin, n);
    //uint64_t batchSize = n/threadCount;
    //auto buffer = new ScalarType[n];
    auto globalRearrangeResult = std::make_shared<GlobalRearrangementResult>(bufferBegin, n);
//    globalRearrangeResult->sBegin = vInBegin;
//    globalRearrangeResult->sTail = vInBegin;
    //globalRearrangeResult->lBegin = buffer + n;
//    globalRearrangeResult->lTail = vInBegin + n;
    std::vector<UUID> eventSet;

    int8_t i = 0;
    auto end = vInBegin + n;
    auto cursor = vInBegin;
    while (cursor < end) {
        UUID eventId = threadPool->generateEventId();
        int curBatchSize = (MAX_ASSIGNET_WORK_ON_THREAD*BATCH_SIZE < (end - cursor)) ? BATCH_SIZE : (end - cursor);
        threadPool->runThread(quickSortParallelLocalRearrangementWithStoreInGlobal, i, eventId, cursor, curBatchSize, pivot, globalRearrangeResult);
        eventSet.push_back(eventId);
        if (cursor + curBatchSize == end) break;
        ++i;
        cursor += BATCH_SIZE;
    }
    threadPool->waitForEvents(eventSet);

    size_t sSize = globalRearrangeResult->sTail - globalRearrangeResult->sBegin;

    std::thread t{quickSortWithStableThreadPoolParallel, globalRearrangeResult->sBegin, sSize, threadPool, vInBegin};
    quickSortWithStableThreadPoolParallel(globalRearrangeResult->lTail, n - sSize, threadPool, vInBegin + sSize);
    t.join();
#ifdef MY_NOT_MOVABLE
    std::copy(globalRearrangeResult->sBegin, globalRearrangeResult->sBegin + n, vInBegin);
#else
//    vInBegin = globalRearrangeResult->sBegin;
    // TODO: a lot double copies on each recursion back and forth. Better to return the last actual as a ref o result set.
    std::copy(globalRearrangeResult->sBegin, globalRearrangeResult->sBegin + n, vInBegin);
#endif
#ifdef MY_DEBUG
    std::printf("End testQuickSortParallel N:%llu; threadCount:%d\n", n, threadCount);
#endif
}

void testQuickSortWithStableThreadPoolParallel(I vInBegin,
                                               const uint64_t n,
                                               const int8_t threadCount) {
    auto globalBuffer = std::vector<ScalarType>(n);
    uint8_t tCount = threadCount > n/BATCH_SIZE ? n/BATCH_SIZE : threadCount;
    auto threadPool = std::make_shared<PersistentThreadPool>(
                                                             tCount,
                                                             LOCAL_SORT_MAX_BATCH_SIZE);
    quickSortWithStableThreadPoolParallel(vInBegin, n, threadPool, globalBuffer.begin());
}



#endif /** QuickSort_hpp */
