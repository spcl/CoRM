
/**                                                                                                      
 * CoRM: Compactable Remote Memory over RDMA
 * 
 * A simple YCSB implementation
 *
 * Copyright (c) 2020-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 * 
 */
#pragma once
#include <iostream>       // std::cout
#include <thread>
#include <chrono>
#include <vector>
#include <cstdlib>
#include <string>
#include <sstream>
#include <cassert>
#include <stdlib.h>
#include <csignal>
#include <set>


#include <random>
#include "zipf.hpp"

class Trace{
public:
    virtual ~Trace() = default;
	virtual std::pair<uint32_t, char> get_next() = 0;
};

class YCSB: public Trace
{

 	const uint32_t max_value_uni = 0xFFFFFFFF;
	std::mt19937 generator;
	zipf_distribution<> zipf;
	std::uniform_int_distribution<uint32_t> dis;
	const uint32_t read_threshold;

public:

	YCSB(unsigned long seed, double read_prob, uint32_t N, double theta): 
	generator(seed), zipf{N,theta}, dis(0,max_value_uni-1), read_threshold((uint32_t)(read_prob*max_value_uni))
	{
		//empty
	}
 

 	virtual std::pair<uint32_t, char> get_next() override
 	{
 		uint32_t rank  = zipf(generator)-1;
 		uint32_t val  = dis(generator);
 		char type = (val < read_threshold) ? 'r' : 'w';
 		return std::make_pair(rank, type);   
 	}

};


class Uniform: public Trace
{
 	const uint32_t max_value_uni = 0xFFFFFFFF;
	std::mt19937 generator;
	std::uniform_int_distribution<uint32_t> uni;
	std::uniform_int_distribution<uint32_t> dis;
	const uint32_t read_threshold;

public:

	Uniform(unsigned long seed, double read_prob, uint32_t N): 
	generator(seed), uni{0,N-1}, dis(0,max_value_uni-1), read_threshold((uint32_t)(read_prob*max_value_uni))
	{
		//empty
	}
 
 	virtual std::pair<uint32_t, char> get_next() override
 	{
 		uint32_t rank  = uni(generator);
 		uint32_t val  = dis(generator);
 		char type = (val < read_threshold) ? 'r' : 'w';
 		return std::make_pair(rank, type);   
 	}

};