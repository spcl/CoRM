/**                                                                                                      
 * CoRM: Compactable Remote Memory over RDMA
 * 
 * A simple/naive code to connect 2 endpoints with rdma using RDMA-CM.
 *
 * Copyright (c) 2020-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 * 
 */


#pragma once
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <infiniband/verbs.h>
#include "verbsEP.hpp"



struct ibv_device *ctx_find_dev(const char *ib_devname) {
	int num_of_device;
	struct ibv_device **dev_list;
	struct ibv_device *ib_dev = NULL;

	dev_list = ibv_get_device_list(&num_of_device);

	if (num_of_device <= 0) {
	  fprintf(stderr, " Did not detect devices \n");
	  fprintf(stderr, " If device exists, check if driver is up\n");
	  return NULL;
	}

	if (!ib_devname) {
	  ib_dev = dev_list[0];
	  if (!ib_dev) {
	    fprintf(stderr, "No IB devices found\n");
	    exit(1);
	  }
	} else {
	  for (; (ib_dev = *dev_list); ++dev_list)
	    if (!strcmp(ibv_get_device_name(ib_dev), ib_devname)) break;
	  if (!ib_dev) fprintf(stderr, "IB device %s not found\n", ib_devname);
	}
	return ib_dev;
}


class ServerRDMA{

	struct rdma_event_channel *cm_channel;
	struct rdma_cm_id *listen_id = NULL;
	//struct ibv_context *ctx;

public:
	ServerRDMA(char* ip, int port){
		int ret;
		struct rdma_addrinfo hints;
		struct rdma_addrinfo *addrinfo;
	
	/*	struct ibv_device    *ib_dev = NULL;
		ib_dev = ctx_find_dev(devname);
		ctx = ibv_open_device(ib_dev);*/

		memset(&hints, 0, sizeof hints);
		hints.ai_flags = RAI_PASSIVE;
		hints.ai_port_space = RDMA_PS_TCP;
       
        char strport[80];
        sprintf(strport, "%d", port);

		ret = rdma_getaddrinfo(ip, strport, &hints, &addrinfo);
        if (ret) {
            perror("rdma_getaddrinfo\n");
            exit(1);
        } 
/*
        this->cm_channel = rdma_create_event_channel();

	    if (this->cm_channel == NULL) {
	      perror(" rdma_create_event_channel failed\n");
	      exit(1);
	    }
		 ret = rdma_create_id(this->cm_channel, &listen_id, NULL, RDMA_PS_TCP);
	      if (ret) {
	        perror("Failed to create RDMA CM server control ID.");
	        exit(1);
	      }

	      ret = rdma_bind_addr(listen_id, addrinfo->ai_src_addr);
	      if (ret) {
	        perror("Failed to bind RDMA CM address on the server.");
	        exit(1);
	      }
*/

        ret = rdma_create_ep(&listen_id, addrinfo, NULL, NULL);
        if (ret) {
            perror("rdma_create_ep\n");
            exit(1);
        }

        rdma_freeaddrinfo(addrinfo);

        ret = rdma_listen(listen_id, 2);
        if (ret) {
           perror("rdma_listen");
           exit(1);
        }

	}

    int get_listen_fd() 
    {    
    
        assert(this->listen_id->channel!=NULL);
        int options = fcntl(this->listen_id->channel->fd, F_GETFL, 0);

        if (fcntl(this->listen_id->channel->fd, F_SETFL, options | O_NONBLOCK)) {
              perror("[RDMA_COM] cannot set server_client to non-blocking mode");
              exit(1);
              return 0;
        }
 
        return this->listen_id->channel->fd;
    }

	struct ibv_pd * create_pd(){
		return ibv_alloc_pd(listen_id->verbs);
	}


	static struct ibv_srq* create_srq(struct ibv_pd * pd, uint32_t max_wr, uint32_t max_sge=1){

		struct ibv_srq_init_attr attr;
		memset(&attr, 0, sizeof attr);
		attr.attr.max_wr = max_wr;
		attr.attr.max_sge = max_sge;
		return ibv_create_srq(pd, &attr);
	}

    struct ibv_cq *create_cq(uint32_t max_wr, struct ibv_comp_channel *channel = NULL){
		return  ibv_create_cq(listen_id->verbs, max_wr, NULL,channel, 0);
 	}

 	
 	struct rdma_cm_id * getConnectRequest(){
 		int ret;
		struct rdma_cm_id *id;

		ret = rdma_get_request(this->listen_id, &id);
        if (ret) {
            perror("rdma_get_request");
            exit(1);
        }
        return id;
 	}


	static VerbsEP* acceptEP(struct rdma_cm_id *id, struct ibv_qp_init_attr *attr, struct rdma_conn_param *conn_param, struct ibv_pd* pd = NULL){
		int ret;
		attr->qp_type = IBV_QPT_RC;
        
		ret = rdma_create_qp(id, pd, attr);
		if (ret) {
            perror("rdma_create_qp");
            exit(1);
        }
        
		ret = rdma_accept(id, conn_param);
        if (ret) {
            perror("rdma_accept");
            exit(1);
        }

        return new VerbsEP(id, attr->cap.max_inline_data, attr->cap.max_send_wr, attr->cap.max_recv_wr ); 
	}

};



class ClientRDMA{

	struct rdma_addrinfo *addrinfo;

public:
	ClientRDMA(char* ip, int port){
		int ret;
		struct rdma_addrinfo hints;

		memset(&hints, 0, sizeof hints);
		hints.ai_port_space = RDMA_PS_TCP;
       
        char strport[80];
        sprintf(strport, "%d", port);

		ret = rdma_getaddrinfo(ip, strport, &hints, &addrinfo);
        if (ret) {
            perror("rdma_getaddrinfo\n");
            exit(1);
        } 
 
	}

	~ClientRDMA(){
		rdma_freeaddrinfo(addrinfo);
	}

	struct rdma_cm_id * sendConnectRequest(){
		int ret;
		struct rdma_cm_id *id;

		ret = rdma_create_ep(&id, this->addrinfo, NULL, NULL); 
        if (ret) {
            perror("rdma_create_ep");
            exit(1);
        }
        return id;
	}

	static struct ibv_pd * create_pd(struct rdma_cm_id *id){
		return ibv_alloc_pd(id->verbs);
	}


	static struct ibv_srq* create_srq(struct ibv_pd * pd, uint32_t max_wr, uint32_t max_sge=1){
		struct ibv_srq_init_attr attr;
		memset(&attr, 0, sizeof attr);
		attr.attr.max_wr = max_wr;
		attr.attr.max_sge = max_sge;
		return ibv_create_srq(pd, &attr);
	}

    static struct ibv_cq *create_cq(struct rdma_cm_id * id, uint32_t max_wr, struct ibv_comp_channel *channel = NULL){
     	return  ibv_create_cq(id->verbs, max_wr, NULL, channel, 0);
 	}

	static VerbsEP* connectEP(struct rdma_cm_id * id, struct ibv_qp_init_attr *attr, struct rdma_conn_param *conn_param, struct ibv_pd* pd = NULL){
 		int ret;
		attr->qp_type = IBV_QPT_RC;
		ret = rdma_create_qp(id, pd, attr);
		if (ret) {
            perror("rdma_create_qp");
            exit(1);
        }
        
		ret = rdma_connect(id, conn_param);
        if (ret) {
            perror("rdma_accept");
            exit(1);
        }
 
        return new VerbsEP(id, attr->cap.max_inline_data, attr->cap.max_send_wr, attr->cap.max_recv_wr ); 
	}

};