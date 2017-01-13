#ifndef __GEMFIRE_REMOTEQUERYSERVICE_H__
#define __GEMFIRE_REMOTEQUERYSERVICE_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/SharedPtr.hpp>
#include "CqService.hpp"

#include <gfcpp/QueryService.hpp>
#include "ThinClientCacheDistributionManager.hpp"

#include <ace/Recursive_Thread_Mutex.h>

namespace gemfire {

class CacheImpl;
class ThinClientPoolDM;
typedef std::map<std::string, bool> CqPoolsConnected;
class CPPCACHE_EXPORT RemoteQueryService : public QueryService {
 public:
  RemoteQueryService(CacheImpl* cptr, ThinClientPoolDM* poolDM = NULL);

  void init();

  QueryPtr newQuery(const char* querystring);

  inline ACE_RW_Thread_Mutex& getLock() { return m_rwLock; }
  inline const volatile bool& invalid() { return m_invalid; }

  void close();

  ~RemoteQueryService() {}
  virtual CqQueryPtr newCq(const char* querystr, CqAttributesPtr& cqAttr,
                           bool isDurable = false);
  virtual CqQueryPtr newCq(const char* name, const char* querystr,
                           CqAttributesPtr& cqAttr, bool isDurable = false);
  virtual void closeCqs();
  virtual void getCqs(VectorOfCqQuery& vec);
  virtual CqQueryPtr getCq(const char* name);
  virtual void executeCqs();
  virtual void stopCqs();
  virtual CqServiceStatisticsPtr getCqServiceStatistics();
  void executeAllCqs(bool failover);
  virtual CacheableArrayListPtr getAllDurableCqsFromServer();
  /**
   * execute all cqs on the endpoint after failover
   */
  GfErrType executeAllCqs(TcrEndpoint* endpoint);
  void receiveNotification(TcrMessage* msg);
  void invokeCqConnectedListeners(ThinClientPoolDM* pool, bool connected);
  // For Lazy Cq Start-no use, no start
  inline void initCqService() {
    if (m_cqService == NULLPTR) {
      LOGFINE("RemoteQueryService: starting cq service");
      m_cqService = new CqService(m_tccdm);
      LOGFINE("RemoteQueryService: started cq service");
    }
  }

 private:
  volatile bool m_invalid;
  mutable ACE_RW_Thread_Mutex m_rwLock;

  ThinClientBaseDM* m_tccdm;
  CqServicePtr m_cqService;
  CqPoolsConnected m_CqPoolsConnected;
};

typedef SharedPtr<RemoteQueryService> RemoteQueryServicePtr;

};      // namespace gemfire
#endif  // ifndef __GEMFIRE_REMOTEQUERYSERVICE_H__
