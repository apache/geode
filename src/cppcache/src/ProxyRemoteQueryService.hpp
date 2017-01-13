#ifndef __GEMFIRE_PROXYREMOTEQUERYSERVICE_H__
#define __GEMFIRE_PROXYREMOTEQUERYSERVICE_H__
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
#include "UserAttributes.hpp"
#include <gfcpp/QueryService.hpp>
#include "ProxyCache.hpp"
#include "ThinClientCacheDistributionManager.hpp"

#include <ace/Recursive_Thread_Mutex.h>

namespace gemfire {

class CacheImpl;
class ThinClientPoolDM;

class CPPCACHE_EXPORT ProxyRemoteQueryService : public QueryService {
 public:
  ProxyRemoteQueryService(ProxyCache* cptr);

  QueryPtr newQuery(const char* querystring);

  ~ProxyRemoteQueryService() {}
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
  virtual CacheableArrayListPtr getAllDurableCqsFromServer();

 private:
  void unSupportedException(const char* operationName);
  void addCqQuery(const CqQueryPtr& cqQuery);
  void closeCqs(bool keepAlive);

  QueryServicePtr m_realQueryService;
  ProxyCachePtr m_proxyCache;
  VectorOfCqQuery m_cqQueries;
  // lock for cqQuery list;
  ACE_Recursive_Thread_Mutex m_cqQueryListLock;
  friend class ProxyCache;
};

typedef SharedPtr<ProxyRemoteQueryService> ProxyRemoteQueryServicePtr;

};      // namespace gemfire
#endif  // ifndef __GEMFIRE_PROXYREMOTEQUERYSERVICE_H__
