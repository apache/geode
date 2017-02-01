#pragma once

#ifndef GEODE_STATISTICS_POOLSTATSSAMPLER_H_
#define GEODE_STATISTICS_POOLSTATSSAMPLER_H_

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
#include <ace/Task.h>
#include <gfcpp/gfcpp_globals.hpp>
namespace apache {
namespace geode {
namespace client {
class CacheImpl;
class ThinClientBaseDM;
class AdminRegion;
class ThinClientPoolDM;
}  // namespace client
}  // namespace geode
}  // namespace apache
using namespace apache::geode::client;
namespace apache {
namespace geode {
namespace statistics {
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
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_STATISTICS_POOLSTATSSAMPLER_H_
