#pragma once

#ifndef GEODE_REMOTEQUERY_H_
#define GEODE_REMOTEQUERY_H_

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
#include <gfcpp/gf_types.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include <gfcpp/SharedPtr.hpp>

#include <gfcpp/Query.hpp>
#include <gfcpp/SelectResults.hpp>
#include <gfcpp/ResultSet.hpp>
#include <gfcpp/StructSet.hpp>
#include "CacheImpl.hpp"
#include "ThinClientBaseDM.hpp"
#include "ProxyCache.hpp"
#include <string>

/**
 * @file
 */

namespace apache {
namespace geode {
namespace client {

class CPPCACHE_EXPORT RemoteQuery : public Query {
  std::string m_queryString;

  RemoteQueryServicePtr m_queryService;
  ThinClientBaseDM* m_tccdm;
  ProxyCachePtr m_proxyCache;

 public:
  RemoteQuery(const char* querystr, const RemoteQueryServicePtr& queryService,
              ThinClientBaseDM* tccdmptr, ProxyCachePtr proxyCache = NULLPTR);

  //@TODO check the return type, is it ok. second option could be to pass
  // SelectResults by reference as a parameter.
  SelectResultsPtr execute(uint32_t timeout = DEFAULT_QUERY_RESPONSE_TIMEOUT);

  //@TODO check the return type, is it ok. second option could be to pass
  // SelectResults by reference as a parameter.
  SelectResultsPtr execute(CacheableVectorPtr paramList = NULLPTR,
                           uint32_t timeout = DEFAULT_QUERY_RESPONSE_TIMEOUT);

  // executes a query using a given distribution manager
  // used by Region.query() and Region.getAll()
  SelectResultsPtr execute(uint32_t timeout, const char* func,
                           ThinClientBaseDM* tcdm,
                           CacheableVectorPtr paramList);

  // nothrow version of execute()
  GfErrType executeNoThrow(uint32_t timeout, TcrMessageReply& reply,
                           const char* func, ThinClientBaseDM* tcdm,
                           CacheableVectorPtr paramList);

  const char* getQueryString() const;

  void compile();

  bool isCompiled();
};

typedef SharedPtr<RemoteQuery> RemoteQueryPtr;
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_REMOTEQUERY_H_
