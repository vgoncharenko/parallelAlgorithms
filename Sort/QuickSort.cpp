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

template<typename T>
void quickSortParallelLocalRearrangementWithStoreInGlobal(I begin,
                                                          const uint64_t size,
                                                          T pivot,
                                                          std::shared_ptr<GlobalRearrangementResult> globalResult,
                                                          BlockBarrier* barrier){
#ifdef MY_DEBUG
    std::printf("quickSortParallelLocalRearrangementWithStoreInGlobal size:%llu; pivot=%f\n", size, pivot);
#endif
    LocalRearrangementResult local = quickSortParallelLocalRearrangement(begin, size, pivot, barrier);
    std::unique_lock<std::mutex> blockM1Lock(barrier->m);
    barrier->condition.wait(blockM1Lock, [&barrier] { return barrier->syncAllowed; });
    quickSortParallelGlobalRearrangement(local, globalResult);
}



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
    //auto buffer = new ScalarType[n];
    auto globalRearrangeResult = std::make_shared<GlobalRearrangementResult>(vInBegin, n);
//    globalRearrangeResult->sBegin = vInBegin;
//    globalRearrangeResult->sTail = vInBegin;
    //globalRearrangeResult->lBegin = buffer + n;
//    globalRearrangeResult->lTail = vInBegin + n;
    auto barier = new BlockBarrier(threadCount);
    std::vector<UUID> eventSet;

    int8_t i = 0;
    auto end = vInBegin + n;
    auto cursor = vInBegin;
    while (cursor < end) {
        UUID eventId = threadPool->generateEventId();
        int curBatchSize = (2*batchSize < (end - cursor)) ? batchSize : (end - cursor);
        threadPool->runThread(quickSortParallelLocalRearrangementWithStoreInGlobal<ScalarType>, i, eventId, cursor, curBatchSize, pivot, globalRearrangeResult, barier);
        eventSet.push_back(eventId);
        if (cursor + curBatchSize == end) break;
        ++i;
        cursor += batchSize;
    }
    barier->infLimit = i+1;
    threadPool->waitForEvents(eventSet);

    size_t sSize = globalRearrangeResult->sTail - globalRearrangeResult->sBegin;
    int8_t sThreadCount = round(threadCount * (float(sSize) / n));
    std::thread t{quickSortWithStableThreadPoolParallel, globalRearrangeResult->sBegin, sSize, sThreadCount, threadPool};
    quickSortWithStableThreadPoolParallel(globalRearrangeResult->lTail, n - sSize, threadCount - sThreadCount, threadPool);
    t.join();
#ifdef MY_NOT_MOVABLE
    std::copy(globalRearrangeResult->sBegin, globalRearrangeResult->sBegin + n, vInBegin);
#else
//    vInBegin = globalRearrangeResult->sBegin;
//    std::move(globalRearrangeResult->sBegin, globalRearrangeResult->sBegin + n, vInBegin);
#endif
#ifdef MY_DEBUG
    std::printf("End testQuickSortParallel N:%llu; threadCount:%d\n", n, threadCount);
#endif
}

void testQuickSortWithStableThreadPoolParallel(I vInBegin,
                                               const uint64_t n,
                                               const int8_t threadCount) {
    auto threadPool = std::make_shared<PersistentThreadPool>(threadCount);
    quickSortWithStableThreadPoolParallel(vInBegin, n, threadCount, threadPool);
}



#endif /** QuickSort_hpp */
