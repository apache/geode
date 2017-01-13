/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef _EVICTIONTHREAD_H__
#define _EVICTIONTHREAD_H__

#include <ace/ACE.h>
#include <ace/OS.h>
#include <ace/Singleton.h>
#include <ace/Thread_Mutex.h>
#include <ace/Task.h>
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/Log.hpp>
#include "IntQueue.hpp"
/**
 * This class does the actual evictions
 */
namespace gemfire {
class EvictionController;
typedef IntQueue<int64_t> HeapSizeInfoQueue;

class CPPCACHE_EXPORT EvictionThread : public ACE_Task_Base {
 public:
  EvictionThread(EvictionController* parent);

  inline void start() {
    m_run = true;
    this->activate();
    LOGFINE("Eviction Thread started");
  }

  inline void stop() {
    m_run = false;
    this->wait();
    m_queue.clear();
    LOGFINE("Eviction Thread stopped");
  }

  int svc();
  void putEvictionInfo(int32_t info);
  void processEvictions();

 private:
  EvictionController* m_pParent;
  HeapSizeInfoQueue m_queue;
  bool m_run;

  static const char* NC_Evic_Thread;
};
}

#endif  //_EVICTIONTHREAD_H__
