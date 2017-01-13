/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef _EVICTIONCONTROLLER_H__
#define _EVICTIONCONTROLLER_H__

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
namespace gemfire {

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
}
#endif  //_EVICTIONCONTROLLER_H__
