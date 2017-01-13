#ifndef __GEMFIRE_EXECUTION_IMPL_H__
#define __GEMFIRE_EXECUTION_IMPL_H__

/*=========================================================================
  * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
  *
  * The specification of function behaviors is found in the corresponding .cpp
 fil
  e.
  *
  *========================================================================
  */

#include <gfcpp/gf_types.hpp>
#include <gfcpp/Execution.hpp>
#include <gfcpp/CacheableBuiltins.hpp>
#include <gfcpp/ResultCollector.hpp>
#include <gfcpp/Region.hpp>
#include "ProxyCache.hpp"
#include <ace/Condition_Recursive_Thread_Mutex.h>
#include <ace/Guard_T.h>
#include <map>

namespace gemfire {
typedef std::map<std::string, std::vector<int8>*> FunctionToFunctionAttributes;

class ExecutionImpl : public Execution {
 public:
  ExecutionImpl(RegionPtr rptr = NULLPTR, ProxyCachePtr proxyCache = NULLPTR,
                PoolPtr pp = NULLPTR)
      : m_routingObj(NULLPTR),
        m_args(NULLPTR),
        m_rc(NULLPTR),
        m_region(rptr),
        m_allServer(false),
        m_pool(pp),
        m_proxyCache(proxyCache) {}
  ExecutionImpl(PoolPtr pool, bool allServer = false,
                ProxyCachePtr proxyCache = NULLPTR)
      : m_routingObj(NULLPTR),
        m_args(NULLPTR),
        m_rc(NULLPTR),
        m_region(NULLPTR),
        m_allServer(allServer),
        m_pool(pool),
        m_proxyCache(proxyCache) {}
  virtual ExecutionPtr withFilter(CacheableVectorPtr routingObj);
  virtual ExecutionPtr withArgs(CacheablePtr args);
  virtual ExecutionPtr withCollector(ResultCollectorPtr rs);
  // java function has hasResult property. we put the hasResult argument
  // here as a kluge.
  virtual ResultCollectorPtr execute(CacheableVectorPtr& routingObj,
                                     CacheablePtr& args, ResultCollectorPtr& rs,
                                     const char* func, uint32_t timeout);
  virtual ResultCollectorPtr execute(
      const char* func, uint32_t timeout = DEFAULT_QUERY_RESPONSE_TIMEOUT);
  static void addResults(ResultCollectorPtr& collector,
                         const CacheableVectorPtr& results);

 private:
  ResultCollectorPtr execute(const char* func, uint32_t timeout,
                             bool verifyFuncArgs);
  ExecutionImpl(const ExecutionImpl& rhs)
      : m_routingObj(rhs.m_routingObj),
        m_args(rhs.m_args),
        m_rc(rhs.m_rc),
        m_region(rhs.m_region),
        m_allServer(rhs.m_allServer),
        m_pool(rhs.m_pool),
        m_proxyCache(rhs.m_proxyCache) {}
  ExecutionImpl(const CacheableVectorPtr& routingObj, const CacheablePtr& args,
                const ResultCollectorPtr& rc, const RegionPtr& region,
                const bool allServer, const PoolPtr& pool,
                ProxyCachePtr proxyCache = NULLPTR)
      : m_routingObj(routingObj),
        m_args(args),
        m_rc(rc),
        m_region(region),
        m_allServer(allServer),
        m_pool(pool),
        m_proxyCache(proxyCache) {}
  // ACE_Recursive_Thread_Mutex m_lock;
  CacheableVectorPtr m_routingObj;
  CacheablePtr m_args;
  ResultCollectorPtr m_rc;
  RegionPtr m_region;
  bool m_allServer;
  PoolPtr m_pool;
  ProxyCachePtr m_proxyCache;
  static ACE_Recursive_Thread_Mutex m_func_attrs_lock;
  static FunctionToFunctionAttributes m_func_attrs;
  //  std::vector<int8> m_attributes;
  CacheableVectorPtr executeOnPool(
      std::string& func, uint8_t getResult, int32_t retryAttempts,
      uint32_t timeout = DEFAULT_QUERY_RESPONSE_TIMEOUT);
  void executeOnAllServers(std::string& func, uint8_t getResult,
                           uint32_t timeout = DEFAULT_QUERY_RESPONSE_TIMEOUT);
  std::vector<int8>* getFunctionAttributes(const char* func);
  GfErrType getFuncAttributes(const char* func, std::vector<int8>** attr);
};
}  // namespace gemfire
#endif  //__GEMFIRE_EXECUTION_IMPL_H__
