/**                                                                                                      
 * CoRM: Compactable Remote Memory over RDMA
 * 
 * Interfaces for an IO-enabled thread
 *
 * Copyright (c) 2020-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 * 
 */
#pragma once 


typedef void (*io_cb)(uint32_t id, void* ctx);
class IOWatcher{
public:
    virtual void install_io(int fd, io_cb cb, void* ctx ) = 0;
    virtual void stop_io(uint32_t io_id) = 0;
    virtual ~IOWatcher() = default;
};


class GenericWorker {


protected:

    uint8_t local_thread_id; 
    IOWatcher* local_io_watcher;
    
public:
    virtual ~GenericWorker() = default;

    // Allocate a block.
    virtual void main_cb() = 0;

    virtual void sometimes_cb() = 0;

    virtual void set_io_watcher(IOWatcher *w){
        this->local_io_watcher = w;
    }

    virtual void set_thread_id(uint8_t id){
        this->local_thread_id = id;
    }

    virtual void print_stats() = 0;

};


