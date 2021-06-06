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
        auto event = std::make_shared<ThreadEvent>(
                eventId,
                std::make_unique<QuickSortPersistentThreadSample>(threadId,
                                                                  std::forward<F&&>(func),
                                                                  std::forward<Args&&>(args)...)
        );
        {
            std::lock_guard<std::mutex> lk(finishMapMutex);
            eventFinishMap[eventId] = event;
        }
        eventQueue.pushEvent(event);
    }

    void waitForEvents(std::vector<UUID> set) {
        for_each(set.begin(), set.end(), [this](const auto id) {
            std::shared_ptr<ThreadEvent> event;
            {
                std::lock_guard<std::mutex> lk(finishMapMutex);
                event = eventFinishMap[id];
            }
#ifdef MY_DEBUG
        std::printf("Wait for eventId %llu lock aquier\n", id);
#endif
            std::unique_lock<std::mutex> blockM1Lock(event->m);
#ifdef MY_DEBUG
        std::printf("Wait for eventId %llu lock aquiered\n", id);

            std::chrono::milliseconds span (2000);
            if (event->finishEventCondition
                .wait_for(blockM1Lock, span, [id, event] { return event->isFinish; })) {
                
            } else {
                int r = 10;
            }
#else
            event->finishEventCondition.wait(blockM1Lock, [id, event] { return event->isFinish; });
#endif
#ifdef MY_DEBUG
        std::printf("eventId %llu finished\n", id);
#endif
            //eventFinishMap.erase(id);
            blockM1Lock.unlock();
        });
    }
    
    void waitForEvents() {
        for_each(eventFinishMap.begin(), eventFinishMap.end(), [this](const auto &item) {
#ifdef MY_DEBUG
        std::printf("Wait for eventId %llu lock aquier\n", item.first);
#endif
            std::unique_lock<std::mutex> blockM1Lock(item.second->m);
#ifdef MY_DEBUG
        std::printf("Wait for eventId %llu lock aquiered\n", item.first);
#endif
            item.second->finishEventCondition.wait(blockM1Lock, [item, this] { return item.second->isFinish; });
#ifdef MY_DEBUG
        std::printf("eventId %llu finished\n", item.first);
#endif
            //eventFinishMap.erase(item.first);
            blockM1Lock.unlock();
        });
    }

    void stopThreads() {
#ifdef MY_DEBUG
        std::printf("Persistet thread pool stop threads process start\n");
#endif
        eventQueue.terminate();
        for_each(threadQueue.begin(), threadQueue.end(), [](const auto &it) {
            it->stopSignal = true;
        });
#ifdef MY_DEBUG
        std::printf("All threads recieved stop signal. Start joining.\n");
#endif
        for_each(threads.begin(), threads.end(), [](std::thread &t) {
            t.join();
        });
#ifdef MY_DEBUG
        std::printf("Persistet thread pool stop threads process finish\n");
#endif
    }

    void terminate() {
#ifdef MY_DEBUG
        std::printf("Persistet thread pool terminate process start\n");
#endif
        this->waitForEvents();
        this->stopThreads();
#ifdef MY_DEBUG
        std::printf("Persistet thread pool terminate process finish\n");
#endif
    }
    
    UUID generateEventId() {
        return lastEventId.fetch_add(1);
    }

    ~PersistentThreadPool () {
        this->terminate();
    }
};

#endif
