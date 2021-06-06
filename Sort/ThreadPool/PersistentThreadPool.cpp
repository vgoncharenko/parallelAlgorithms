//
//  PersistentThreadPool.cpp
//  palgs
//
//  Created by Vitaliy on 04.06.2021.
//
#ifndef PersistentThreadPool_hpp
#define PersistentThreadPool_hpp

#include <unordered_map>
#include <vector>
#include <mutex>
#include "Thread/ThreadEvent.cpp"
#include "ThreadPoolQueue.cpp"
#include "PersistentThread.cpp"

class PersistentThreadPool {
private:
    ThreadPoolQueue<ThreadEvent> eventQueue;
    std::unordered_map<UUID, std::shared_ptr<ThreadEvent>> eventFinishMap;
    std::mutex finishMapMutex;
    std::vector<PersistentThread*> threadQueue;
    std::vector<std::thread> threads;
    std::mutex m;
    const uint8_t maxSize;
    std::atomic<int> count = 0;
    std::atomic<int> lastEventId = 0;
public:
    explicit PersistentThreadPool (uint8_t _maxSize,
                                   size_t threadLocalBufferSize): maxSize(_maxSize) {
        for (int i = 0; i < maxSize; ++i) {
            auto threadInstance = new PersistentThread(&eventQueue, threadLocalBufferSize);
            std::thread t{[threadInstance]{ threadInstance->run(); }};
            threadQueue.push_back(threadInstance);
            threads.push_back(std::move(t));
        }
    };

    template<class F, class... Args>
    void runThread(F&& func,
                   ThreadId threadId,
                   UUID eventId,
                   Args&&... args) {
#ifdef MY_DEBUG
        std::printf("Start threadId %d\n", threadId);
#endif
        auto event = std::make_shared<ThreadEvent>(
                eventId,
                std::make_unique<QuickSortPersistentThreadSample>(threadId,
                                                                  std::forward<F&&>(func),
                                                                  std::forward<Args&&>(args)...)
        );
        eventQueue.pushEvent(event);
        eventFinishMap[eventId] = event;
    }

    void waitForEvents(std::vector<UUID> set) {
        for_each(set.begin(), set.end(), [this](const auto &id) {
            std::unique_lock<std::mutex> blockM1Lock(eventFinishMap[id]->m);
            eventFinishMap[id]->finishEventCondition.wait(blockM1Lock, [id, this] { return eventFinishMap[id]->isFinish; });
            eventFinishMap.erase(id);
        });
    }
    
    void waitForEvents() {
        for_each(eventFinishMap.begin(), eventFinishMap.end(), [this](const auto &item) {
            std::unique_lock<std::mutex> blockM1Lock(item.second->m);
            item.second->finishEventCondition.wait(blockM1Lock, [item, this] { return item.second->isFinish; });
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
        this->waitForEvents();
        this->stopThreads();
    }
    
    UUID generateEventId() {
        return lastEventId.fetch_add(1);
    }

    ~PersistentThreadPool () {
        this->terminate();
    }
};

#endif
