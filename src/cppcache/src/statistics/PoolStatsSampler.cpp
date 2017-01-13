/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "PoolStatsSampler.hpp"
#include <string>
#include <ReadWriteLock.hpp>
#include <CacheImpl.hpp>
#include <ThinClientPoolDM.hpp>
#include "GemfireStatisticsFactory.hpp"
#include <ClientHealthStats.hpp>
#include <NanoTimer.hpp>
#include "HostStatHelper.hpp"
using namespace gemfire_statistics;
using namespace gemfire;
const char* PoolStatsSampler::NC_PSS_Thread = "NC PSS Thread";

PoolStatsSampler::PoolStatsSampler(int64 sampleRate, CacheImpl* cache,
                                   ThinClientPoolDM* distMan)
    : m_sampleRate(sampleRate), m_distMan(distMan) {
  m_running = false;
  m_stopRequested = false;
  m_adminRegion = new AdminRegion(cache, distMan);
}

PoolStatsSampler::~PoolStatsSampler() {
  // GF_SAFE_DELETE(m_adminRegion);
}

int32 PoolStatsSampler::svc() {
  DistributedSystemImpl::setThreadName(NC_PSS_Thread);
  int32 msSpentWorking = 0;
  int32 msRate = static_cast<int32>(m_sampleRate);
  // ACE_Guard < ACE_Recursive_Thread_Mutex > _guard( m_lock );
  while (!m_stopRequested) {
    int64 sampleStartNanos = NanoTimer::now();
    putStatsInAdminRegion();
    int64 sampleEndNanos = NanoTimer::now();
    int64 nanosSpentWorking = sampleEndNanos - sampleStartNanos;
    msSpentWorking = static_cast<int32>(nanosSpentWorking / 1000000);
    int32 msToWait = msRate - msSpentWorking;
    while (msToWait > 0) {
      ACE_Time_Value sleepTime;
      sleepTime.msec(msToWait > 100 ? 100 : msToWait);
      ACE_OS::sleep(sleepTime);
      msToWait -= 100;
      if (m_stopRequested) {
        break;
      }
    }
  }
  return 0;
}

void PoolStatsSampler::start() {
  if (!m_running) {
    m_running = true;
    this->activate();
  }
}

void PoolStatsSampler::stop() {
  // ACE_Guard < ACE_Recursive_Thread_Mutex > _guard( m_lock );
  m_stopRequested = true;
  this->wait();
}
bool PoolStatsSampler::isRunning() { return m_running; }

void PoolStatsSampler::putStatsInAdminRegion() {
  // Get Values of gets, puts,misses,listCalls,numThread
  try {
    static std::string clientId = "";
    if (!m_adminRegion->isDestroyed()) {
      int puts = 0, gets = 0, misses = 0, numListeners = 0, numThreads = 0,
          creates = 0;
      int64 cpuTime = 0;
      GemfireStatisticsFactory* gf =
          GemfireStatisticsFactory::getExistingInstance();
      if (gf) {
        StatisticsType* cacheStatType = gf->findType("CachePerfStats");
        if (cacheStatType) {
          Statistics* cachePerfStats =
              gf->findFirstStatisticsByType(cacheStatType);
          if (cachePerfStats) {
            puts = cachePerfStats->getInt((char*)"puts");
            gets = cachePerfStats->getInt((char*)"gets");
            misses = cachePerfStats->getInt((char*)"misses");
            creates = cachePerfStats->getInt((char*)"creates");
            numListeners =
                cachePerfStats->getInt((char*)"cacheListenerCallsCompleted");
            puts += creates;
          }
        }
        numThreads = HostStatHelper::getNumThreads();
        cpuTime = HostStatHelper::getCpuTime();
      }
      static int numCPU = ACE_OS::num_processors();
      ClientHealthStatsPtr obj = ClientHealthStats::create(
          gets, puts, misses, numListeners, numThreads, cpuTime, numCPU);
      ClientProxyMembershipID* memId = m_distMan->getMembershipId();
      clientId = memId->getDSMemberIdForThinClientUse();
      CacheableKeyPtr keyPtr = CacheableString::create(clientId.c_str());
      m_adminRegion->put(keyPtr, obj);
    }
  } catch (const AllConnectionsInUseException&) {
    LOGDEBUG("All connection are in use, trying again.");
  } catch (const NotConnectedException& ex) {
    try {
      ExceptionPtr exCause =
          dynCast<SharedPtr<NoAvailableLocatorsException> >(ex.getCause());
      LOGDEBUG("No locators available, trying again.");
    } catch (ClassCastException&) {
      LOGDEBUG("Not connected to gemfire, trying again.");
    }
  } catch (...) {
    LOGDEBUG("Exception occurred, trying again.");
  }
}
