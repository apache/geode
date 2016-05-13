#ifndef __GEMFIRE_REMOTEQUERY_H__
#define __GEMFIRE_REMOTEQUERY_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "../gfcpp_globals.hpp"
#include "../gf_types.hpp"
#include "../ExceptionTypes.hpp"
#include "../SharedPtr.hpp"

#include "../Query.hpp"
#include "../SelectResults.hpp"
#include "../ResultSet.hpp"
#include "../StructSet.hpp"
#include "CacheImpl.hpp"
#include "ThinClientBaseDM.hpp"
#include "ProxyCache.hpp"
#include <string>

/**
 * @file
 */

namespace gemfire
{

class CPPCACHE_EXPORT RemoteQuery : public Query
{

  std::string m_queryString;

  RemoteQueryServicePtr m_queryService;
  ThinClientBaseDM* m_tccdm;
  ProxyCachePtr m_proxyCache;

public:

  RemoteQuery(const char * querystr, const RemoteQueryServicePtr& queryService,
    ThinClientBaseDM* tccdmptr, ProxyCachePtr proxyCache = NULLPTR);

  //@TODO check the return type, is it ok. second option could be to pass SelectResults by reference as a parameter.
  SelectResultsPtr execute(uint32_t timeout = DEFAULT_QUERY_RESPONSE_TIMEOUT);

  //@TODO check the return type, is it ok. second option could be to pass SelectResults by reference as a parameter.
  SelectResultsPtr execute(CacheableVectorPtr paramList=NULLPTR, uint32_t timeout = DEFAULT_QUERY_RESPONSE_TIMEOUT);
  
  // executes a query using a given distribution manager
  // used by Region.query() and Region.getAll()
  SelectResultsPtr execute(uint32_t timeout,
      const char* func, ThinClientBaseDM* tcdm, CacheableVectorPtr paramList);

  // nothrow version of execute()
  GfErrType executeNoThrow(uint32_t timeout, TcrMessage& reply,
      const char* func, ThinClientBaseDM* tcdm, CacheableVectorPtr paramList);

  const char * getQueryString() const;

  void compile();

  bool isCompiled();

};

typedef SharedPtr< RemoteQuery > RemoteQueryPtr;

} //namespace gemfire

#endif //ifndef __GEMFIRE_REMOTEQUERY_H__
