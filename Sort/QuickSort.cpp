//
//  QuickSort.cpp
//  palgs
//
//  Created by Vitaliy on 01.06.2021.
//

#include "QuickSortNaive.cpp"

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


