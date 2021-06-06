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

typedef void quickSortParallelLocalRearrangementWithStoreInGlobalFunc(I begin,
                                                                  const uint64_t size,
                                                                  ScalarType pivot,
                                                                  std::shared_ptr<GlobalRearrangementResult> globalResult,
                                                                  LocalRearrangementResult& localBuffer);

//typedef void quickSortParallelLocalRearrangementWithStoreInGlobalFunc(I begin);

struct PersistentThreadSample {
    ThreadId id;
    PersistentThreadPoolFuture f;
    
    explicit PersistentThreadSample (ThreadId _id, PersistentThreadPoolFuture _f)
    : id(_id), f(std::move(_f)) {}
};

struct QuickSortPersistentThreadSample {
    ThreadId id;
    quickSortParallelLocalRearrangementWithStoreInGlobalFunc* func;
    I begin;
    const uint64_t size;
    ScalarType pivot;
    std::shared_ptr<GlobalRearrangementResult> globalResult;
    
    explicit QuickSortPersistentThreadSample (ThreadId _id,
                                              quickSortParallelLocalRearrangementWithStoreInGlobalFunc* _func,
                                              I _begin,
                                              const uint64_t _size,
                                              ScalarType _pivot,
                                              std::shared_ptr<GlobalRearrangementResult> _globalResult)
    : id(_id),
    func(_func),
    begin(_begin),
    size(_size),
    pivot(_pivot),
    globalResult(_globalResult)
    {}
};

struct ThreadEvent{
    UUID id;
    std::unique_ptr<QuickSortPersistentThreadSample> threadSample;
    std::mutex m;
    std::condition_variable finishEventCondition;
    bool isFinish = false;
    
#ifdef MY_DEBUG
    ThreadEvent(const ThreadEvent& obj) {
            std::printf("eventId# %llu copy costruct\n", id);
    }
#else
    ThreadEvent(const ThreadEvent& obj) = delete;
#endif
#ifdef MY_DEBUG
    ThreadEvent &operator=(const  ThreadEvent &) {
            std::printf("eventId# %llu copy assigment\n", id);
    }
#else
    ThreadEvent &operator=(const  ThreadEvent &)  = delete;
#endif
#ifdef MY_DEBUG
    ThreadEvent() {
            std::printf("Def costruct \n");
    }
#else
    ThreadEvent() = default;
#endif
   
#ifdef MY_DEBUG
    ~ThreadEvent() {

            std::printf("eventId# %llu destroyed\n", id);
    }
#else
    ~ThreadEvent() = default;
#endif

    explicit ThreadEvent (UUID _id, std::unique_ptr<QuickSortPersistentThreadSample> _threadSample)
    : id(_id), threadSample(std::move(_threadSample)) {}

};

#endif
