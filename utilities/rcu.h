/**                                                                                                      
 * CoRM: Compactable Remote Memory over RDMA
 * 
 * A class for ensuring that each thread does not access the hometable during a grow.
 *
 * Copyright (c) 2020-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 * 
 */

#pragma once

#include "../thread/thread.hpp"

// This is a one-shot object.
class BroadcastDrain {
public:
    typedef void (*timer_rcu_cb)(void *owner);

public:
    static void Drain(int home_thread_id, timer_rcu_cb cb, void *owner);

protected:
    BroadcastDrain(int home_thread_id, timer_rcu_cb cb, void *owner);
    ~BroadcastDrain();

    void Drain();

    static void DrainingDoneOne(BroadcastDrain *_this);

    void SendTmsg(int thread_id);
    bool DrainingDoneOne();

    static void RequestTmsg(mts::thread_msg_t *tmsg);
    static void ResponseTmsg(mts::thread_msg_t *tmsg);

protected:
    const int host_thread_id;

    timer_rcu_cb cb;
    void *owner;

    // threads remaining to drain
    int remaining;
};



void BroadcastDrain::Drain(int home_thread_id, timer_rcu_cb cb, void *owner) {
    BroadcastDrain *rcu = new BroadcastDrain(home_thread_id, cb, owner);
    rcu->Drain();
}

BroadcastDrain::BroadcastDrain(int thread_id, timer_rcu_cb cb, void *owner)
        : host_thread_id(thread_id),
        cb(cb),
        owner(owner),
        remaining(0) {
    // empty
}

BroadcastDrain::~BroadcastDrain() {
    // empty
}

void BroadcastDrain::Drain() {
    int thread_count = mts::num_threads;
    remaining = 1;

    for(int i = 0;i < thread_count;i++) {
        if(i != host_thread_id) {
            SendTmsg(i);
            remaining++;
        }
    }

    DrainingDoneOne(this);
}

void BroadcastDrain::SendTmsg(int thread_id) {
    mts::thread_msg_t *tmsg = new mts::thread_msg_t();
    //Tmsg *ctx = new (tmsg->payload) Tmsg(this, host_thread_id);
    tmsg->cb = &BroadcastDrain::RequestTmsg;
    tmsg->payload[0] = this;
    tmsg->payload[1] = (void*)(uint64_t)host_thread_id;
    mts::send_msg_to_thread_and_notify(thread_id, tmsg);
}

void BroadcastDrain::DrainingDoneOne(BroadcastDrain *_this) {
    if(_this->DrainingDoneOne()) {
        delete _this;
    }
}

bool BroadcastDrain::DrainingDoneOne() {
    bool ret;

    if(--remaining == 0) {
        timer_rcu_cb cb = this->cb;
        void *owner = this->owner;

        cb(owner);

        ret = true;
    } else {
        ret = false;
    }

    return ret;
}

void BroadcastDrain::RequestTmsg(mts::thread_msg_t *tmsg) {
    tmsg->cb = &BroadcastDrain::ResponseTmsg;
    int _resp_thread_id = (int)(uint64_t)tmsg->payload[1];
    mts::send_msg_to_thread_and_notify(_resp_thread_id, tmsg);
}

void BroadcastDrain::ResponseTmsg(mts::thread_msg_t *tmsg) {
     
    BroadcastDrain *_this = (BroadcastDrain*)tmsg->payload[0];
    free(tmsg);

    DrainingDoneOne(_this);
}
