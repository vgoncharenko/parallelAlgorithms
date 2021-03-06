//
//  PersistentThread.cpp
//  palgs
//
//  Created by Vitaliy on 04.06.2021.
//
#ifndef PersistentThread_hpp
#define PersistentThread_hpp

#include <future>
#include "Thread/ThreadEvent.cpp"
#include "ThreadPoolQueue.cpp"

class PersistentThread {
private:
    ThreadPoolQueue<ThreadEvent> *eventQueue;
    LocalRearrangementResult resultBuffer;
public:
    bool inProgress = false;
    bool stopSignal = false;
    std::mutex m;

    PersistentThread(const PersistentThread& obj) = delete;
    PersistentThread &  operator=(const  PersistentThread &)  = delete;
    PersistentThread() = default;
    ~PersistentThread() = default;

    explicit PersistentThread(ThreadPoolQueue<ThreadEvent> *_eventQueue, size_t localBufferSize)
    : eventQueue(_eventQueue){
        resultBuffer = LocalRearrangementResult(localBufferSize);
    }

    void run() {
        while (!this->stopSignal) {
            auto event = this->eventQueue->pullEvent();
            if (nullptr == event) return;
            this->inProgress = true;
#ifdef MY_DEBUG
            std::printf("Process sample for eventId# %d\n", int(event->id));
#endif
            PersistentThreadPoolFuture fut = std::async(std::launch::deferred,
                                                        event->threadSample->func,
                                                        std::ref(event->threadSample->begin),
                                                        event->threadSample->size,
                                                        std::forward<ScalarType>(event->threadSample->pivot),
                                                        std::ref(event->threadSample->globalResult),
                                                        std::ref(resultBuffer)
                                                        );
            fut.wait();
            this->resetThread();
            this->inProgress = false;
            {
                std::lock_guard<std::mutex> lk(event->m);
                event->isFinish = true;
            }
#ifdef MY_DEBUG
            std::printf("Process sample for eventId# %d finished\n", int(event->id));
#endif
            event->finishEventCondition.notify_one();
        }
    }
    
    void resetThread() {
        resultBuffer.flushData();
    }
};


#endif
