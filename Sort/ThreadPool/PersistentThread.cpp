//
//  PersistentThread.cpp
//  palgs
//
//  Created by Vitaliy on 04.06.2021.
//
#ifndef PersistentThread_hpp
#define PersistentThread_hpp

#include "Thread/ThreadEvent.cpp"
#include "ThreadPoolQueue.cpp"

class PersistentThread {
private:
    ThreadPoolQueue<ThreadEvent> *eventQueue;

public:
    bool inProgress = false;
    bool stopSignal = false;
    std::mutex m;

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
            event->isFinish = true;
            event->finishEventCondition.notify_one();
        }
    }
};


#endif
