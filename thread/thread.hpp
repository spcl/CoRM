/**                                                                                                      
 * CoRM: Compactable Remote Memory over RDMA
 * 
 * A modifier of a thread to support io events
 *
 * Copyright (c) 2020-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 * 
 */
#pragma once

#include "../worker/generic_worker.hpp"
#include "messenger.hpp"
#include <map>
#include <csignal>
#include "../utilities/debug.h"

#define NOW 0.000000001

  
class Thread: public IOWatcher{
    struct my_io: public ev::io{
        my_io(uint32_t id, io_cb cb, void *ctx): id(id), cb(cb), ctx(ctx){
            //empty
        }
        uint32_t id;
        io_cb cb;
        void *ctx;
    };
// events
    ev::async stopper;         // for termination
    ev::idle main_event;       // main event which is called when is not busy
    ev::timer timer_event;     // call something time to time
    ev::dynamic_loop   loop;   // loop of the thread


    std::map<uint32_t, my_io*>  io_events; // io events
    uint32_t current_io_id;

    ev::async notify;         // for notifying about incoming messeges


 
    const int queue_size = 100;
    const uint32_t _thread_id;

    boost::lockfree::queue<mts::thread_msg_t*> *_msg_queue;
    GenericWorker* const _worker;
    
    std::thread the_thread;



public:
    Thread(uint32_t id, GenericWorker *w);


    ~Thread(){
        text(log_fp,"\t[Thread] Try to destroy worker(%u)\n",_thread_id);
        delete _msg_queue;
        delete _worker;
    
        text(log_fp,"\t\t[Thread] Worker is destroyed\n");
    }

    void Start() {
        this->the_thread = std::thread(&Thread::main_method,this);
    }

    uint32_t GetId() const {
        return _thread_id;
    };

 
    boost::lockfree::queue<mts::thread_msg_t*>* GetMsgQueue() const{
        return _msg_queue;
    };

    ev::async* GetNotify(){
        return &(this->notify);
    };

    ev::dynamic_loop* GetLoop(){
        return &(this->loop);
    };


    void main_cb (ev::idle   &w, int revents){
       
       _worker->main_cb();
       //this->poll_message_cb();
    } 

 
    void main_method(){
        // create async stopper for terminating the tread
         

        this->stopper.set(this->loop);
        this->stopper.set<Thread, &Thread::terminate_cb>(this);
        this->stopper.priority = EV_MAXPRI-1;
        this->stopper.start();


        this->notify.set(this->loop);
        this->notify.set<Thread, &Thread::poll_message_cb>(this);
        this->notify.priority = EV_MAXPRI-1;
        this->notify.start();



        this->timer_event.set(this->loop);
        this->timer_event.set<Thread, &Thread::timer_cb>(this);
        this->timer_event.set(10,50); // after 10  repeat 50
        this->timer_event.priority = EV_MAXPRI-1;
        this->timer_event.start(10,50); // after 10  repeat 50

 

        this->main_event.set(this->loop);
        this->main_event.set<Thread, &Thread::main_cb>(this);
        this->main_event.priority = EV_MAXPRI;
        this->main_event.start(); 

        mts::thread_id = _thread_id;

        this->loop.run(0); 

    }


    void timer_cb  (ev::timer   &w, int revents){
      _worker->sometimes_cb();
      
      w.repeat = 5; // repeat after 5
      w.again();
    } 

    void Stop(){
       text(log_fp,"[Thread] Try stopping %d \n",  this->GetId() );
        this->stopper.send();
        if(this->the_thread.joinable()){
            this->the_thread.join();
        }

    }


    void terminate_cb() {


        for (auto &pair: io_events)
        {       
                pair.second->stop();
                delete pair.second;
        }


        this->stopper.stop();
        this->timer_event.stop();
        this->main_event.stop();
        this->notify.stop();

        this->loop.break_loop(ev::ALL);
        text(log_fp,"[Thread] Thread(%d) is terminated\n",  this->GetId() );

        // print stats
        _worker->print_stats();

    }


    void poll_message_cb() {
        //  text(log_fp, "Poll Message %d \n",  this->GetId() );
        bool found = true;
        while(found){
            
            mts::thread_msg_t* message = nullptr; 
            this->_msg_queue->pop(message);
            found = (message!=nullptr);
            if(found){
                //mts::thread_msg_cb cb = (mts::thread_msg_cb)message->cb;
                message->cb(message);
            } else {
               // text(log_fp,"No message %d \n",  this->GetId() );
            }
        } 

    }

    void io_process (ev::io   &w, int revents){
        my_io& new_d =  static_cast<my_io&>(w);
        text(log_fp,"filed = %d\n", new_d.fd);
        new_d.cb(new_d.id, new_d.ctx);
    }

    void install_io(int fd, io_cb cb, void* ctx ) override{
        my_io *io = new my_io(current_io_id, cb,ctx);
        io_events[current_io_id] = io;
        current_io_id++;
        io->set(this->loop);
        io->set<Thread, &Thread::io_process>(this);
        io->start(fd, ev::READ);
    }

    void stop_io(uint32_t io_id) override{
        auto it = io_events.find(io_id);
        assert(it!=io_events.end());
        delete it->second;
        io_events.erase(it);
    }

    private:
        Thread(const Thread &) = delete;
        void operator=(const Thread &) = delete;

};



namespace mts{
        void RegisterThread(Thread *t){
                reg_mutex.lock();

                    reged_threads++; 
                    msg_queues[t->GetId()] = t->GetMsgQueue();
                    notifies[t->GetId()] = t->GetNotify();   
                    loops[t->GetId()] = t->GetLoop();

                reg_mutex.unlock();
        }

        void SCHEDULE_CALLBACK(int thread_id, ev_tstamp time, thread_delay_cb cb, void *arg) {
            ev_once (*loops[thread_id], 0, 0, time, cb, arg); 
        } 

}


Thread::Thread(uint32_t id, GenericWorker *w): loop(ev::AUTO), _thread_id(id), _worker(w) 
{
    //empty
    this->_msg_queue = new boost::lockfree::queue<mts::thread_msg_t*>(queue_size);
    mts::RegisterThread(this);

}




// it helps to launch all threads.
class LauncherMaster{


public:

    LauncherMaster (uint32_t tot_threads):  current_thread_id(0), num_threads(tot_threads)
    {
      
      instance = this;
      std::signal(SIGINT, LauncherMaster::signal_handler);

      mts::setup_threads(tot_threads);
    }

    ~LauncherMaster(){

        text(log_fp,"[LauncherMaster] Try to destroy all threads\n");
        
        for (auto &iter: threads)
        {
                delete iter;
        }


        text(log_fp,"\t[LauncherMaster] All threads are destroyed\n");

    }


    uint32_t add_worker(GenericWorker *worker){
        assert(current_thread_id<num_threads);
        Thread* t = new Thread(current_thread_id, worker);
        threads.push_back(t);
        worker->set_thread_id(current_thread_id);
        worker->set_io_watcher(t);

        return (current_thread_id++);
    }

    void launch(){
        
        for (uint32_t i=1; i < current_thread_id; ++i) {
            text(log_fp,"\t[LauncherMaster] start thread %u\n",i);
            threads[i]->Start();
        } 
        text(log_fp,"\t[LauncherMaster] start thread 0\n");

        threads[0]->main_method();
    }


private:

    void handler_wraper (int signum)
    {
        
        text(log_fp," Signal %d  detected \n",signum);
        // handling code

        for (uint32_t i=0; i <current_thread_id; ++i) {
            threads[i]->Stop();
        }
        text(log_fp," All threads are stopped\n");
       
    }


    std::vector<Thread*> threads;
    uint32_t current_thread_id;
    const uint32_t num_threads;

  

    static LauncherMaster* instance;

    static void signal_handler(int signum)
    {
        instance->handler_wraper(signum);
    }
};
 
LauncherMaster* LauncherMaster::instance = nullptr;
