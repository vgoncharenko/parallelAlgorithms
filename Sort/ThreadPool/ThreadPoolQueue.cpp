//
//  ThreadPoolQueue.cpp
//  palgs
//
//  Created by Vitaliy on 04.06.2021.
//

#ifndef ThreadPoolQueue_hpp
#define ThreadPoolQueue_hpp

#include "Thread/ThreadEvent.cpp"
#include <mutex>
#include <queue>
#include <memory>

template<typename ThreadEvent>
class ThreadPoolQueue {
private:
    bool stopQueue = false;
    std::mutex mFront;
    std::mutex mBack;
    std::queue<std::shared_ptr<ThreadEvent>> eventQueue{};
    std::condition_variable eventInQueue;
    
    bool empty() {
        return eventQueue.empty();
    }
public:

    std::shared_ptr<ThreadEvent> pullEvent() {
        std::shared_ptr<ThreadEvent> event = nullptr;
        std::unique_lock<std::mutex> waitLock(mFront);
        eventInQueue.wait(waitLock, [this] { return !this->eventQueue.empty() || this->stopQueue; });
        if (this->stopQueue) {
            return event;
        }
        event = eventQueue.front();
#ifdef MY_DEBUG
        std::printf("Pull event from the queue eventId %llu\n", event->id);
#endif
        eventQueue.pop();
        waitLock.unlock();

        return event;
    }

    void pushEvent(std::shared_ptr<ThreadEvent> event) {
#ifdef MY_DEBUG
        std::printf("Push event to the queue eventId %llu\n", event->id);
#endif
        std::lock_guard<std::mutex> backLock(mFront);
        eventQueue.push(event);
        eventInQueue.notify_one();
    }

    void terminate() {
        std::unique_lock<std::mutex> lk(mFront);
        stopQueue = true;
        lk.unlock();
        eventInQueue.notify_all();
    }
};

#endif /* ThreadPoolQueue_hpp */
