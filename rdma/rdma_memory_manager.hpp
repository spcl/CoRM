/**                                                                                                      
 * CoRM: Compactable Remote Memory over RDMA
 * 
 * A simple manager of block registration. It also used to support experimental API, but I removed it as it is depricated by MOFED.
 *
 * Copyright (c) 2020-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 * 
 */

#pragma once 

#include <infiniband/verbs.h>

/*
#ifdef HAVE_ODP_MR_PREFETCH 
#warning "Prefecth is loaded"
#include <infiniband/verbs_exp.h> 
#else
#warning "Prefecth is not supported"
#endif
*/

struct ibv_memory_manager{

	struct ibv_pd * const pd;
    const bool _withODP;

	ibv_memory_manager(struct ibv_pd *pd, bool with_odp): pd(pd), _withODP(with_odp) {
		// empty
	}	

    struct ibv_mr * mem_reg_odp(void *addr,uint32_t size){
        return ibv_reg_mr(pd,addr,size,IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE \
            | IBV_ACCESS_REMOTE_READ | (IBV_ACCESS_ON_DEMAND * (int)this->_withODP ));
    }

    void mem_rereg(struct ibv_mr * mr){
        if( !_withODP ){
         int ret = ibv_rereg_mr(mr,IBV_REREG_MR_CHANGE_TRANSLATION, pd, mr->addr,mr->length,0);
         assert(ret==0 && "ibv_rereg_mr failed");
           return;
        }  
        
        struct ibv_sge sge; 
        sge.addr = (uint64_t)mr->addr;
        sge.length = mr->length;
        sge.lkey = mr->lkey;

        int ret = ibv_advise_mr(pd, IBV_ADVISE_MR_ADVICE_PREFETCH,
                        IBV_ADVISE_MR_FLAG_FLUSH,
                        &sge, 1);
 
        assert(ret==0 && "ibv_advise_mr failed");

#if 0
      struct ibv_exp_prefetch_attr prefetch_attr;
      prefetch_attr.flags = IBV_EXP_PREFETCH_WRITE_ACCESS;
      prefetch_attr.addr = (uint64_t)mr->addr;
      prefetch_attr.length = mr->length;
      prefetch_attr.comp_mask = 0;
      int ret =ibv_exp_prefetch_mr(mr, &prefetch_attr);          
#endif


    }


    void mem_dereg(struct ibv_mr * mr){
    	int ret = ibv_dereg_mr(mr);
    	assert(ret==0 && "ibv_dereg_mr failed");
        return;
    }
};