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
        if (event->threadSample != nullptr)
            std::printf("Pull event from the queue thread# %d\n", event->threadSample->id);
#endif
        eventQueue.pop();
        waitLock.unlock();

        return event;
    }

    void pushEvent(std::shared_ptr<ThreadEvent> event) {
#ifdef MY_DEBUG
        if (event->threadSample != nullptr)
            std::printf("Push event to the queue thread# %d\n", event->threadSample->id);
#endif
        std::lock_guard<std::mutex> backLock(mBack);
        eventQueue.push(event);
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

#endif /* ThreadPoolQueue_hpp */
