//
//  ThreadEvent.cpp
//  palgs
//
//  Created by Vitaliy on 04.06.2021.
//

#ifndef ThreadEvent_hpp
#define ThreadEvent_hpp

#include "../../QuickSortNaive.cpp"
#include <memory>
#include <unordered_map>
#include <mutex>

struct PersistentThreadSample {
    ThreadId id;
    PersistentThreadPoolFuture f;
    
    explicit PersistentThreadSample (ThreadId _id, PersistentThreadPoolFuture _f)
    : id(_id), f(std::move(_f)) {}
};

struct ThreadEvent{
    UUID id;
    std::unique_ptr<PersistentThreadSample> threadSample;
    std::mutex m;
    std::condition_variable finishEventCondition;
    bool isFinish = false;
    
    ThreadEvent(const ThreadEvent& obj) = delete;
    ThreadEvent &  operator=(const  ThreadEvent &)  = delete;
    ThreadEvent() = default;
    ~ThreadEvent() = default;

    //ThreadEvent() = default;

    explicit ThreadEvent (UUID _id, std::unique_ptr<PersistentThreadSample> _threadSample)
    : id(_id), threadSample(std::move(_threadSample)) {}

};

#endif
