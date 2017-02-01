#pragma once

#ifndef GEODE_PROXYREMOTEQUERYSERVICE_H_
#define GEODE_PROXYREMOTEQUERYSERVICE_H_

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
#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/SharedPtr.hpp>
#include "CqService.hpp"
#include "UserAttributes.hpp"
#include <gfcpp/QueryService.hpp>
#include "ProxyCache.hpp"
#include "ThinClientCacheDistributionManager.hpp"

#include <ace/Recursive_Thread_Mutex.h>

namespace apache {
namespace geode {
namespace client {

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
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_PROXYREMOTEQUERYSERVICE_H_
