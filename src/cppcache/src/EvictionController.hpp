#pragma once

#ifndef GEODE_EVICTIONCONTROLLER_H_
#define GEODE_EVICTIONCONTROLLER_H_

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <ace/ACE.h>
#include <ace/OS.h>
#include <ace/Singleton.h>
#include <ace/Thread_Mutex.h>
#include <ace/Task.h>
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/Log.hpp>
#include <gfcpp/SharedPtr.hpp>
#include "IntQueue.hpp"
#include "EvictionThread.hpp"
#include <string>
#include <vector>

/**
 * This class ensures that the cache consumes only as much memory as
 * specified by the heap-lru-limit. Every region that is created in the
 * system registers with the EvictionController. Everytime there is any
 * activity that changes the memory usage in the region, it puts a message
 * into a queue that the EvictionController waits on. The message contains
 * the full region name, the total size of the region (inclusive of keys and
 * values) and the total number of entries.The EvictionController thread picks
 * up the message, updates the total memory size as well as the size of the
 * region in question. It determines whether memory usage is within limits.
 * If so, it goes back to waiting on the queue. If memory usage is out of bounds
 * it does the following.
 *  1> Figures out the delta between specified and actual
 *  2> Determines the size percentage that needs to be evicted
 *  3> Determines the nmber of entries per region (based on size per entry) that
       needs to be evicted. This is a slice of the total eviction that is
 needed.
    4> Invokes a method on each region to trigger eviction of entries.The evict
       method on the region will return the total size evicted for the entries.
    5> Goes back and checks queue size and recalculates the heap size usage
 *
 *
 * When a region is destroyed, it deregisters itself with the EvictionController
 * Format of object that is put into the region map (int size, int numEntries)
 */
namespace apache {
namespace geode {
namespace client {

typedef IntQueue<int64_t> HeapSizeInfoQueue;
typedef std::vector<std::string> VectorOfString;

class EvictionController;
class EvictionThread;
class CacheImpl;
typedef SharedPtr<EvictionController> EvictionControllerPtr;

class CPPCACHE_EXPORT EvictionController : public ACE_Task_Base,
                                           public SharedBase {
 public:
  EvictionController(size_t maxHeapSize, int32_t heapSizeDelta,
                     CacheImpl* cache);

  ~EvictionController();

  inline void start() {
    m_run = true;
    evictionThreadPtr->start();
    this->activate();
    LOGFINE("Eviction Controller started");
  }

  inline void stop() {
    m_run = false;
    evictionThreadPtr->stop();
    this->wait();
    m_regions.clear();
    m_queue.clear();

    LOGFINE("Eviction controller stopped");
  }

  int svc(void);

  void updateRegionHeapInfo(int64_t info);
  void registerRegion(std::string& name);
  void deregisterRegion(std::string& name);
  void evict(int32_t percentage);

 private:
  void orderEvictions(int32_t percentage);
  void processHeapInfo(int64_t& readInfo, int64_t& pendingEvictions);

 private:
  bool m_run;
  int64_t m_maxHeapSize;
  int64_t m_heapSizeDelta;
  CacheImpl* m_cacheImpl;
  int64_t m_currentHeapSize;
  HeapSizeInfoQueue m_queue;
  VectorOfString m_regions;
  mutable ACE_RW_Thread_Mutex m_regionLock;
  EvictionThread* evictionThreadPtr;
  static const char* NC_EC_Thread;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_EVICTIONCONTROLLER_H_
