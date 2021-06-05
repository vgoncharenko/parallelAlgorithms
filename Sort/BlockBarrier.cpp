//
//  BlockBarier.cpp
//  palgs
//
//  Created by Vitaliy on 03.06.2021.
//
#ifndef BlockBarrier_h
#define BlockBarrier_h

#include <mutex>
#include <stdint.h>

class BlockBarrier {
private:
    int count = 0;
public:
    int8_t infLimit;
    std::condition_variable condition;
    mutable std::mutex m;
    bool syncAllowed = false;

    explicit BlockBarrier (const uint64_t _infLimit) : infLimit(_infLimit) {};

    BlockBarrier& operator++ () {
        {
            std::scoped_lock lk(m);
            ++count;
            if (infLimit > count) return *this;
        }

        if (infLimit <= count) {
            {
                std::scoped_lock lk(m);
                syncAllowed = true;
            }
            condition.notify_all();
        }

        return *this;
    }

    void reset() {
        std::scoped_lock lk(m);
        count = 0;
        syncAllowed = false;
    }
};
#endif /* BlockBarrier_h */
