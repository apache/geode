/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __POOLSTATSSAMPLER__
#define __POOLSTATSSAMPLER__
#include <ace/Task.h>
#include <gfcpp/gfcpp_globals.hpp>
namespace gemfire {
class CacheImpl;
class ThinClientBaseDM;
class AdminRegion;
class ThinClientPoolDM;
}
using namespace gemfire;
namespace gemfire_statistics {
class StatisticsManager;
class CPPCACHE_EXPORT PoolStatsSampler : public ACE_Task_Base {
 public:
  PoolStatsSampler(int64 sampleRate, CacheImpl* cache,
                   ThinClientPoolDM* distMan);
  void start();
  void stop();
  int32 svc(void);
  bool isRunning();
  virtual ~PoolStatsSampler();

 private:
  PoolStatsSampler();
  PoolStatsSampler& operator=(const PoolStatsSampler&);
  PoolStatsSampler(const PoolStatsSampler& PoolStatsSampler);
  void putStatsInAdminRegion();
  volatile bool m_running;
  volatile bool m_stopRequested;
  int64 m_sampleRate;
  AdminRegion* m_adminRegion;
  ThinClientPoolDM* m_distMan;
  ACE_Recursive_Thread_Mutex m_lock;
  static const char* NC_PSS_Thread;
};
}
#endif
