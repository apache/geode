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
#include "RemoteQuery.hpp"
#include "TcrMessage.hpp"
#include "ResultSetImpl.hpp"
#include "StructSetImpl.hpp"
#include <gfcpp/GemfireTypeIds.hpp>
#include "ReadWriteLock.hpp"
#include "ThinClientRegion.hpp"
#include "UserAttributes.hpp"
#include "EventId.hpp"
#include "ThinClientPoolDM.hpp"

using namespace apache::geode::client;

RemoteQuery::RemoteQuery(const char* querystr,
                         const RemoteQueryServicePtr& queryService,
                         ThinClientBaseDM* tccdmptr, ProxyCachePtr proxyCache) {
  m_queryString = querystr;
  m_queryService = queryService;
  m_tccdm = tccdmptr;
  m_proxyCache = proxyCache;
  LOGFINEST("RemoteQuery: created a new query: %s", querystr);
}

SelectResultsPtr RemoteQuery::execute(uint32_t timeout) {
  GuardUserAttribures gua;
  if (m_proxyCache != NULLPTR) {
    gua.setProxyCache(m_proxyCache);
  }
  return execute(timeout, "Query::execute", m_tccdm, NULLPTR);
}

SelectResultsPtr RemoteQuery::execute(CacheableVectorPtr paramList,
                                      uint32_t timeout) {
  GuardUserAttribures gua;
  if (m_proxyCache != NULLPTR) {
    gua.setProxyCache(m_proxyCache);
  }
  return execute(timeout, "Query::execute", m_tccdm, paramList);
}

SelectResultsPtr RemoteQuery::execute(uint32_t timeout, const char* func,
                                      ThinClientBaseDM* tcdm,
                                      CacheableVectorPtr paramList) {
  if ((timeout * 1000) >= 0x7fffffff) {
    char exMsg[1024];
    ACE_OS::snprintf(exMsg, 1023,
                     "%s: timeout parameter "
                     "greater than maximum allowed (2^31/1000 i.e 2147483)",
                     func);
    throw IllegalArgumentException(exMsg);
  }
  ThinClientPoolDM* pool = dynamic_cast<ThinClientPoolDM*>(tcdm);
  if (pool != NULL) {
    pool->getStats().incQueryExecutionId();
  }
  /*get the start time for QueryExecutionTime stat*/
  int64 sampleStartNanos = Utils::startStatOpTime();
  TcrMessageReply reply(true, tcdm);
  ChunkedQueryResponse* resultCollector = (new ChunkedQueryResponse(reply));
  reply.setChunkedResultHandler(
      static_cast<TcrChunkedResult*>(resultCollector));
  GfErrType err = executeNoThrow(timeout, reply, func, tcdm, paramList);
  GfErrTypeToException(func, err);

  SelectResultsPtr sr;

  LOGFINEST("%s: reading reply for query: %s", func, m_queryString.c_str());
  CacheableVectorPtr values = resultCollector->getQueryResults();
  const std::vector<CacheableStringPtr>& fieldNameVec =
      resultCollector->getStructFieldNames();
  size_t sizeOfFieldNamesVec = fieldNameVec.size();
  if (sizeOfFieldNamesVec == 0) {
    LOGFINEST("%s: creating ResultSet for query: %s", func,
              m_queryString.c_str());
    sr = new ResultSetImpl(values);
  } else {
    if (values->size() % fieldNameVec.size() != 0) {
      char exMsg[1024];
      ACE_OS::snprintf(exMsg, 1023,
                       "%s: Number of values coming from "
                       "server has to be exactly divisible by field count",
                       func);
      throw MessageException(exMsg);
    } else {
      LOGFINEST("%s: creating StructSet for query: %s", func,
                m_queryString.c_str());
      sr = new StructSetImpl(values, fieldNameVec);
    }
  }

  /*update QueryExecutionTime stat */
  if (pool != NULL) {
    Utils::updateStatOpTime(
        pool->getStats().getStats(),
        PoolStatType::getInstance()->getQueryExecutionTimeId(),
        sampleStartNanos);
  }
  delete resultCollector;
  return sr;
}

GfErrType RemoteQuery::executeNoThrow(uint32_t timeout, TcrMessageReply& reply,
                                      const char* func, ThinClientBaseDM* tcdm,
                                      CacheableVectorPtr paramList) {
  LOGFINEST("%s: executing query: %s", func, m_queryString.c_str());

  TryReadGuard guard(m_queryService->getLock(), m_queryService->invalid());

  if (m_queryService->invalid()) {
    return GF_CACHE_CLOSED_EXCEPTION;
  }
  LOGDEBUG("%s: creating QUERY TcrMessage for query: %s", func,
           m_queryString.c_str());
  if (paramList != NULLPTR) {
    // QUERY_WITH_PARAMETERS
    TcrMessageQueryWithParameters msg(
        m_queryString, NULLPTR, paramList,
        static_cast<int>(timeout * 1000) /* in milli second */, tcdm);
    msg.setTimeout(timeout);
    reply.setTimeout(timeout);

    GfErrType err = GF_NOERR;
    LOGFINEST("%s: sending request for query: %s", func, m_queryString.c_str());
    if (tcdm == NULL) {
      tcdm = m_tccdm;
    }
    err = tcdm->sendSyncRequest(msg, reply);
    if (err != GF_NOERR) {
      return err;
    }
    if (reply.getMessageType() == TcrMessage::EXCEPTION) {
      err = ThinClientRegion::handleServerException(func, reply.getException());
      if (err == GF_CACHESERVER_EXCEPTION) {
        err = GF_REMOTE_QUERY_EXCEPTION;
      }
    }
    return err;
  } else {
    TcrMessageQuery msg(m_queryString,
                        static_cast<int>(timeout * 1000) /* in milli second */,
                        tcdm);
    msg.setTimeout(timeout);
    reply.setTimeout(timeout);

    GfErrType err = GF_NOERR;
    LOGFINEST("%s: sending request for query: %s", func, m_queryString.c_str());
    if (tcdm == NULL) {
      tcdm = m_tccdm;
    }
    err = tcdm->sendSyncRequest(msg, reply);
    if (err != GF_NOERR) {
      return err;
    }
    if (reply.getMessageType() == TcrMessage::EXCEPTION) {
      err = ThinClientRegion::handleServerException(func, reply.getException());
      if (err == GF_CACHESERVER_EXCEPTION) {
        err = GF_REMOTE_QUERY_EXCEPTION;
      }
    }
    return err;
  }
}

const char* RemoteQuery::getQueryString() const {
  return m_queryString.c_str();
}

void RemoteQuery::compile() {
  throw UnsupportedOperationException("Not supported in native clients");
}

bool RemoteQuery::isCompiled() {
  throw UnsupportedOperationException("Not supported in native clients");
}
