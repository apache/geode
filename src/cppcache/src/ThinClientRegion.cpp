/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 * *
 */

#include "Utils.hpp"
#include "ThinClientRegion.hpp"
#include "TcrDistributionManager.hpp"
#include "ThinClientPoolDM.hpp"
#include "ThinClientBaseDM.hpp"
#include "TcrEndpoint.hpp"
#include <gfcpp/SystemProperties.hpp>
#include "CacheImpl.hpp"
#include "RegionGlobalLocks.hpp"
#include "ReadWriteLock.hpp"
#include "RemoteQuery.hpp"
#include <gfcpp/SelectResultsIterator.hpp>
#include <gfcpp/Struct.hpp>
#include "GemfireTypeIdsImpl.hpp"
#include "AutoDelete.hpp"
#include <gfcpp/PoolManager.hpp>
#include "UserAttributes.hpp"
#include <gfcpp/UserFunctionExecutionException.hpp>
#include "PutAllPartialResultServerException.hpp"
#include "VersionedCacheableObjectPartList.hpp"
//#include "PutAllPartialResult.hpp"

using namespace gemfire;

namespace gemfire {
void setTSSExceptionMessage(const char* exMsg);
}

class PutAllWork : public PooledWork<GfErrType>,
                   private NonCopyable,
                   private NonAssignable {
  ThinClientPoolDM* m_poolDM;
  BucketServerLocationPtr m_serverLocation;
  TcrMessage* m_request;
  TcrMessageReply* m_reply;
  MapOfUpdateCounters m_mapOfUpdateCounters;
  bool m_attemptFailover;
  bool m_isBGThread;
  UserAttributesPtr m_userAttribute;
  const RegionPtr m_region;
  VectorOfCacheableKeyPtr m_keys;
  HashMapOfCacheablePtr m_map;
  VersionedCacheableObjectPartListPtr m_verObjPartListPtr;
  uint32_t m_timeout;
  PutAllPartialResultServerExceptionPtr m_papException;
  bool m_isPapeReceived;
  ChunkedPutAllResponse* m_resultCollector;
  // UNUSED const UserDataPtr& m_aCallbackArgument;

 public:
  PutAllWork(ThinClientPoolDM* poolDM,
             const BucketServerLocationPtr& serverLocation,
             const RegionPtr& region, bool attemptFailover, bool isBGThread,
             const HashMapOfCacheablePtr map,
             const VectorOfCacheableKeyPtr keys, uint32_t timeout,
             const UserDataPtr& aCallbackArgument)
      : m_poolDM(poolDM),
        m_serverLocation(serverLocation),
        m_attemptFailover(attemptFailover),
        m_isBGThread(isBGThread),
        m_userAttribute(NULLPTR),
        m_region(region),
        m_keys(keys),
        m_map(map),
        m_timeout(timeout),
        m_papException(NULLPTR),
        m_isPapeReceived(false)
  // UNUSED , m_aCallbackArgument(aCallbackArgument)
  {
    m_request = new TcrMessagePutAll(m_region.ptr(), *m_map.ptr(),
                                     static_cast<int>(m_timeout * 1000),
                                     m_poolDM, aCallbackArgument);
    m_reply = new TcrMessageReply(true, m_poolDM);

    // create new instanceof VCOPL
    ACE_Recursive_Thread_Mutex responseLock;
    m_verObjPartListPtr =
        new VersionedCacheableObjectPartList(keys.ptr(), responseLock);

    if (m_poolDM->isMultiUserMode()) {
      m_userAttribute = TSSUserAttributesWrapper::s_gemfireTSSUserAttributes
                            ->getUserAttributes();
    }

    m_request->setTimeout(m_timeout);
    m_reply->setTimeout(m_timeout);
    m_resultCollector = new ChunkedPutAllResponse(
        m_region, *m_reply, responseLock, m_verObjPartListPtr);
    m_reply->setChunkedResultHandler(m_resultCollector);
  }

  ~PutAllWork() {
    delete m_request;
    delete m_reply;
    delete m_resultCollector;
  }

  TcrMessage* getReply() { return m_reply; }

  HashMapOfCacheablePtr getPutAllMap() { return m_map; }

  VersionedCacheableObjectPartListPtr getVerObjPartList() {
    return m_verObjPartListPtr;
  }

  ChunkedPutAllResponse* getResultCollector() { return m_resultCollector; }

  BucketServerLocationPtr getServerLocation() { return m_serverLocation; }

  PutAllPartialResultServerExceptionPtr getPaPResultException() {
    return m_papException;
  }

  void init() {}
  GfErrType execute(void) {
    GuardUserAttribures gua;

    if (m_userAttribute != NULLPTR) {
      gua.setProxyCache(m_userAttribute->getProxyCache());
    }

    GfErrType err = GF_NOERR;
    err = m_poolDM->sendSyncRequest(*m_request, *m_reply, m_attemptFailover,
                                    m_isBGThread, m_serverLocation);

    // Set Version Tags
    LOGDEBUG(" m_verObjPartListPtr size = %d err = %d ",
             m_resultCollector->getList()->size(), err);
    m_verObjPartListPtr->setVersionedTagptr(
        m_resultCollector->getList()->getVersionedTagptr());

    if (err != GF_NOERR) {
      return err;
    } /*This can be GF_NOTCON, counterpart to java
                          ServerConnectivityException*/

    switch (m_reply->getMessageType()) {
      case TcrMessage::REPLY:
        break;
      case TcrMessage::RESPONSE:
        LOGDEBUG("PutAllwork execute err = %d ", err);
        break;
      case TcrMessage::EXCEPTION:
        // TODO::Check for the PAPException and READ
        // PutAllPartialResultServerException and set its member for later use.
        // set m_papException and m_isPapeReceived
        m_isPapeReceived = true;
        if (m_poolDM->isNotAuthorizedException(m_reply->getException())) {
          LOGDEBUG("received NotAuthorizedException");
          err = GF_AUTHENTICATION_FAILED_EXCEPTION;
        } else if (m_poolDM->isPutAllPartialResultException(
                       m_reply->getException())) {
          LOGDEBUG("received PutAllPartialResultException");
          err = GF_PUTALL_PARTIAL_RESULT_EXCEPTION;
        } else {
          LOGDEBUG("received unknown exception:%s", m_reply->getException());
          err = GF_PUTALL_PARTIAL_RESULT_EXCEPTION;
          // TODO should assign a new err code
        }

        break;
      case TcrMessage::PUT_DATA_ERROR:
        err = GF_CACHESERVER_EXCEPTION;
        break;
      default:
        LOGERROR("Unknown message type %d during region put-all",
                 m_reply->getMessageType());
        err = GF_NOTOBJ;
        break;
    }
    return err;
  }
};

class RemoveAllWork : public PooledWork<GfErrType>,
                      private NonCopyable,
                      private NonAssignable {
  ThinClientPoolDM* m_poolDM;
  BucketServerLocationPtr m_serverLocation;
  TcrMessage* m_request;
  TcrMessageReply* m_reply;
  MapOfUpdateCounters m_mapOfUpdateCounters;
  bool m_attemptFailover;
  bool m_isBGThread;
  UserAttributesPtr m_userAttribute;
  const RegionPtr m_region;
  const UserDataPtr& m_aCallbackArgument;
  VectorOfCacheableKeyPtr m_keys;
  VersionedCacheableObjectPartListPtr m_verObjPartListPtr;
  PutAllPartialResultServerExceptionPtr m_papException;
  bool m_isPapeReceived;
  ChunkedRemoveAllResponse* m_resultCollector;

 public:
  RemoveAllWork(ThinClientPoolDM* poolDM,
                const BucketServerLocationPtr& serverLocation,
                const RegionPtr& region, bool attemptFailover, bool isBGThread,
                const VectorOfCacheableKeyPtr keys,
                const UserDataPtr& aCallbackArgument)
      : m_poolDM(poolDM),
        m_serverLocation(serverLocation),
        m_attemptFailover(attemptFailover),
        m_isBGThread(isBGThread),
        m_userAttribute(NULLPTR),
        m_region(region),
        m_aCallbackArgument(aCallbackArgument),
        m_keys(keys),
        m_papException(NULLPTR),
        m_isPapeReceived(false) {
    m_request = new TcrMessageRemoveAll(m_region.ptr(), *keys,
                                        m_aCallbackArgument, m_poolDM);
    m_reply = new TcrMessageReply(true, m_poolDM);
    // create new instanceof VCOPL
    ACE_Recursive_Thread_Mutex responseLock;
    m_verObjPartListPtr =
        new VersionedCacheableObjectPartList(keys.ptr(), responseLock);

    if (m_poolDM->isMultiUserMode()) {
      m_userAttribute = TSSUserAttributesWrapper::s_gemfireTSSUserAttributes
                            ->getUserAttributes();
    }

    m_resultCollector = new ChunkedRemoveAllResponse(
        m_region, *m_reply, responseLock, m_verObjPartListPtr);
    m_reply->setChunkedResultHandler(m_resultCollector);
  }

  ~RemoveAllWork() {
    delete m_request;
    delete m_reply;
    delete m_resultCollector;
  }

  TcrMessage* getReply() { return m_reply; }

  VersionedCacheableObjectPartListPtr getVerObjPartList() {
    return m_verObjPartListPtr;
  }

  ChunkedRemoveAllResponse* getResultCollector() { return m_resultCollector; }

  BucketServerLocationPtr getServerLocation() { return m_serverLocation; }

  PutAllPartialResultServerExceptionPtr getPaPResultException() {
    return m_papException;
  }

  void init() {}
  GfErrType execute(void) {
    GuardUserAttribures gua;

    if (m_userAttribute != NULLPTR) {
      gua.setProxyCache(m_userAttribute->getProxyCache());
    }

    GfErrType err = GF_NOERR;
    err = m_poolDM->sendSyncRequest(*m_request, *m_reply, m_attemptFailover,
                                    m_isBGThread, m_serverLocation);

    // Set Version Tags
    LOGDEBUG(" m_verObjPartListPtr size = %d err = %d ",
             m_resultCollector->getList()->size(), err);
    m_verObjPartListPtr->setVersionedTagptr(
        m_resultCollector->getList()->getVersionedTagptr());

    if (err != GF_NOERR) {
      return err;
    } /*This can be GF_NOTCON, counterpart to java
                          ServerConnectivityException*/

    switch (m_reply->getMessageType()) {
      case TcrMessage::REPLY:
        break;
      case TcrMessage::RESPONSE:
        LOGDEBUG("RemoveAllWork execute err = %d ", err);
        break;
      case TcrMessage::EXCEPTION:
        // TODO::Check for the PAPException and READ
        // PutAllPartialResultServerException and set its member for later use.
        // set m_papException and m_isPapeReceived
        m_isPapeReceived = true;
        if (m_poolDM->isNotAuthorizedException(m_reply->getException())) {
          LOGDEBUG("received NotAuthorizedException");
          err = GF_AUTHENTICATION_FAILED_EXCEPTION;
        } else if (m_poolDM->isPutAllPartialResultException(
                       m_reply->getException())) {
          LOGDEBUG("received PutAllPartialResultException");
          err = GF_PUTALL_PARTIAL_RESULT_EXCEPTION;
        } else {
          LOGDEBUG("received unknown exception:%s", m_reply->getException());
          err = GF_PUTALL_PARTIAL_RESULT_EXCEPTION;
          // TODO should assign a new err code
        }

        break;
      case TcrMessage::PUT_DATA_ERROR:
        err = GF_CACHESERVER_EXCEPTION;
        break;
      default:
        LOGERROR("Unknown message type %d during region remove-all",
                 m_reply->getMessageType());
        err = GF_NOTOBJ;
        break;
    }
    return err;
  }
};

ThinClientRegion::ThinClientRegion(const std::string& name, CacheImpl* cache,
                                   RegionInternal* rPtr,
                                   const RegionAttributesPtr& attributes,
                                   const CacheStatisticsPtr& stats, bool shared)
    : LocalRegion(name, cache, rPtr, attributes, stats, shared),
      m_tcrdm((ThinClientBaseDM*)0),
      m_notifyRelease(false),
      m_notificationSema(1),
      m_isMetaDataRefreshed(false) {
  m_transactionEnabled = true;
  m_isDurableClnt =
      strlen(DistributedSystem::getSystemProperties()->durableClientId()) > 0;
}

void ThinClientRegion::initTCR() {
  bool subscription = false;
  PoolPtr pool = PoolManager::find(getAttributes()->getPoolName());
  if (pool != NULLPTR) {
    subscription = pool->getSubscriptionEnabled();
  }
  bool notificationEnabled =
      getAttributes()->getClientNotificationEnabled() || subscription;
  if (notificationEnabled) {
    if (DistributedSystem::getSystemProperties()->isGridClient()) {
      LOGWARN(
          "Region %s: client subscription channel enabled for a grid "
          "client; starting required internal subscription, cleanup and "
          "failover threads",
          m_fullPath.c_str());
      m_cacheImpl->tcrConnectionManager().startFailoverAndCleanupThreads();
    }
  }
  try {
    m_tcrdm =
        new TcrDistributionManager(this, m_cacheImpl->tcrConnectionManager());
    m_tcrdm->init();
  } catch (const Exception& ex) {
    GF_SAFE_DELETE(m_tcrdm);
    LOGERROR("Exception while initializing region: %s: %s", ex.getName(),
             ex.getMessage());
    throw;
  }
}

void ThinClientRegion::registerKeys(const VectorOfCacheableKey& keys,
                                    bool isDurable, bool getInitialValues,
                                    bool receiveValues) {
  PoolPtr pool = PoolManager::find(getAttributes()->getPoolName());
  if (pool != NULLPTR) {
    if (!pool->getSubscriptionEnabled()) {
      LOGERROR(
          "Registering keys is supported "
          "only if subscription-enabled attribute is true for pool %s",
          pool->getName());
      throw UnsupportedOperationException(
          "Registering keys is supported "
          "only if pool subscription-enabled attribute is true.");
    }
  }
  if (keys.empty()) {
    LOGERROR("Register keys list is empty");
    throw IllegalArgumentException(
        "Register keys "
        "keys vector is empty");
  }
  if (isDurable && !isDurableClient()) {
    LOGERROR(
        "Register keys durable flag is only applicable for durable clients");
    throw IllegalStateException(
        "Durable flag only applicable for "
        "durable clients");
  }

  InterestResultPolicy interestPolicy = InterestResultPolicy::NONE;
  if (getInitialValues) interestPolicy = InterestResultPolicy::KEYS_VALUES;

  LOGDEBUG("ThinClientRegion::registerKeys : interestpolicy is %d",
           interestPolicy.ordinal);

  GfErrType err = registerKeysNoThrow(keys, true, NULL, isDurable,
                                      interestPolicy, receiveValues);

  if (m_tcrdm->isFatalError(err)) {
    GfErrTypeToException("Region::registerKeys", err);
  }

  GfErrTypeToException("Region::registerKeys", err);
}

void ThinClientRegion::unregisterKeys(const VectorOfCacheableKey& keys) {
  PoolPtr pool = PoolManager::find(getAttributes()->getPoolName());
  if (pool != NULLPTR) {
    if (!pool->getSubscriptionEnabled()) {
      LOGERROR(
          "Unregister keys is supported "
          "only if subscription-enabled attribute is true for pool %s",
          pool->getName());
      throw UnsupportedOperationException(
          "Unregister keys is supported "
          "only if pool subscription-enabled attribute is true.");
    }
  } else {
    if (!getAttributes()->getClientNotificationEnabled()) {
      LOGERROR(
          "Unregister keys is supported "
          "only if region client-notification attribute is true.");
      throw UnsupportedOperationException(
          "Unregister keys is supported "
          "only if region client-notification attribute is true.");
    }
  }
  if (keys.empty()) {
    LOGERROR("Unregister keys list is empty");
    throw IllegalArgumentException(
        "Unregister keys "
        "keys vector is empty");
  }
  GfErrType err = unregisterKeysNoThrow(keys);
  GfErrTypeToException("Region::unregisterKeys", err);
}

void ThinClientRegion::registerAllKeys(bool isDurable,
                                       VectorOfCacheableKeyPtr resultKeys,
                                       bool getInitialValues,
                                       bool receiveValues) {
  PoolPtr pool = PoolManager::find(getAttributes()->getPoolName());
  if (pool != NULLPTR) {
    if (!pool->getSubscriptionEnabled()) {
      LOGERROR(
          "Register all keys is supported only "
          "if subscription-enabled attribute is true for pool ",
          pool->getName());
      throw UnsupportedOperationException(
          "Register all keys is supported only "
          "if pool subscription-enabled attribute is true.");
    }
  }
  if (isDurable && !isDurableClient()) {
    LOGERROR(
        "Register all keys durable flag is only applicable for durable "
        "clients");
    throw IllegalStateException(
        "Durable flag only applicable for durable clients");
  }

  bool isresultKeys = true;
  if (resultKeys == NULLPTR) {
    resultKeys = VectorOfCacheableKeyPtr(new VectorOfCacheableKey());
    isresultKeys = false;
  }

  InterestResultPolicy interestPolicy = InterestResultPolicy::NONE;
  if (getInitialValues) {
    interestPolicy = InterestResultPolicy::KEYS_VALUES;
  } else {
    interestPolicy = InterestResultPolicy::KEYS;
  }

  LOGDEBUG("ThinClientRegion::registerAllKeys : interestpolicy is %d",
           interestPolicy.ordinal);

  //  if we need to fetch initial data, then we get the keys in
  // that call itself using the special GET_ALL message and do not need
  // to get the keys in the initial  register interest  call
  GfErrType err = registerRegexNoThrow(".*", true, NULL, isDurable, resultKeys,
                                       interestPolicy, receiveValues);

  if (m_tcrdm->isFatalError(err)) {
    GfErrTypeToException("Region::registerAllKeys", err);
  }

  // Get the entries from the server using a special GET_ALL message
  if (isresultKeys == false) {
    resultKeys = NULLPTR;
  }
  GfErrTypeToException("Region::registerAllKeys", err);
}

void ThinClientRegion::registerRegex(const char* regex, bool isDurable,
                                     VectorOfCacheableKeyPtr resultKeys,
                                     bool getInitialValues,
                                     bool receiveValues) {
  PoolPtr pool = PoolManager::find(getAttributes()->getPoolName());
  if (pool != NULLPTR) {
    if (!pool->getSubscriptionEnabled()) {
      LOGERROR(
          "Register regex is supported only if "
          "subscription-enabled attribute is true for pool ",
          pool->getName());
      throw UnsupportedOperationException(
          "Register regex is supported only if "
          "pool subscription-enabled attribute is true.");
    }
  }
  if (isDurable && !isDurableClient()) {
    LOGERROR("Register regex durable flag only applicable for durable clients");
    throw IllegalStateException(
        "Durable flag only applicable for durable clients");
  }

  if (regex == NULL || regex[0] == '\0') {
    throw IllegalArgumentException(
        "Region::registerRegex: Regex string is empty");
  }

  std::string sregex = regex;
  // bool allKeys = (sregex == ".*");
  bool isresultKeys = true;

  // if we need initial values then use resultKeys to get the keys from server
  if (resultKeys == NULLPTR) {
    resultKeys = new VectorOfCacheableKey();
    isresultKeys = false;
  }

  InterestResultPolicy interestPolicy = InterestResultPolicy::NONE;
  if (getInitialValues) {
    interestPolicy = InterestResultPolicy::KEYS_VALUES;
  } else {
    interestPolicy = InterestResultPolicy::KEYS;
  }

  LOGDEBUG("ThinClientRegion::registerRegex : interestpolicy is %d",
           interestPolicy.ordinal);

  //  if we need to fetch initial data for "allKeys" case, then we
  // get the keys in that call itself using the special GET_ALL message and
  // do not need to get the keys in the initial  register interest  call
  GfErrType err = registerRegexNoThrow(
      sregex, true, NULL, isDurable, resultKeys, interestPolicy, receiveValues);

  if (m_tcrdm->isFatalError(err)) {
    GfErrTypeToException("Region::registerRegex", err);
  }

  if (isresultKeys == false) {
    resultKeys = NULLPTR;
  }
  GfErrTypeToException("Region::registerRegex", err);
}

void ThinClientRegion::unregisterRegex(const char* regex) {
  PoolPtr pool = PoolManager::find(getAttributes()->getPoolName());
  if (pool != NULLPTR) {
    if (!pool->getSubscriptionEnabled()) {
      LOGERROR(
          "Unregister regex is supported only if "
          "subscription-enabled attribute is true for pool ",
          pool->getName());
      throw UnsupportedOperationException(
          "Unregister regex is supported only if "
          "pool subscription-enabled attribute is true.");
    }
  }

  if (regex == NULL || regex[0] == '\0') {
    LOGERROR("Unregister regex string is empty");
    throw IllegalArgumentException("Unregister regex string is empty");
  }

  std::string sregex = regex;

  GfErrType err = unregisterRegexNoThrow(sregex);
  GfErrTypeToException("Region::unregisterRegex", err);
}

void ThinClientRegion::unregisterAllKeys() {
  PoolPtr pool = PoolManager::find(getAttributes()->getPoolName());
  if (pool != NULLPTR) {
    if (!pool->getSubscriptionEnabled()) {
      LOGERROR(
          "Unregister all keys is supported only if "
          "subscription-enabled attribute is true for pool ",
          pool->getName());
      throw UnsupportedOperationException(
          "Unregister all keys is supported only if "
          "pool subscription-enabled attribute is true.");
    }
  }
  GfErrType err = unregisterRegexNoThrow(".*");
  GfErrTypeToException("Region::unregisterAllKeys", err);
}

SelectResultsPtr ThinClientRegion::query(const char* predicate,
                                         uint32_t timeout) {
  CHECK_DESTROY_PENDING(TryReadGuard, Region::query);

  if (predicate == NULL || predicate[0] == '\0') {
    LOGERROR("Region query predicate string is empty");
    throw IllegalArgumentException("Region query predicate string is empty");
  }

  bool isFullQuery = false;

  size_t predlen = ACE_OS::strlen(predicate);

  if (predlen > 6)  // perhaps it has 'select' or 'import' if its > 6
  {
    int skipspace = 0;

    while (ACE_OS::ace_isspace(predicate[skipspace])) {
      skipspace++;
    }

    if (predlen - skipspace > 6)  // check remaining length again to avoid
                                  // reading past predicate char array
    {
      char firstWord[7] = {0};

      for (int charpos = 0; charpos < 6; charpos++) {
        firstWord[charpos] =
            ACE_OS::ace_tolower(predicate[charpos + skipspace]);
      }

      if (!ACE_OS::strcmp(firstWord, "select") ||
          !ACE_OS::strcmp(firstWord, "import")) {
        isFullQuery = true;
      }
    }
  }

  std::string squery;

  if (isFullQuery) {
    squery = predicate;
  } else {
    squery = "select distinct * from ";
    squery += getFullPath();
    squery += " this where ";
    squery += predicate;
  }

  RemoteQueryPtr queryPtr;

  // TODO:
  ThinClientPoolDM* poolDM = dynamic_cast<ThinClientPoolDM*>(m_tcrdm);

  if (poolDM) {
    queryPtr = dynCast<RemoteQueryPtr>(
        poolDM->getQueryServiceWithoutCheck()->newQuery(squery.c_str()));
  } else {
    queryPtr = dynCast<RemoteQueryPtr>(
        m_cacheImpl->getQueryService()->newQuery(squery.c_str()));
  }

  return queryPtr->execute(timeout, "Region::query", m_tcrdm, NULLPTR);
}

bool ThinClientRegion::existsValue(const char* predicate, uint32_t timeout) {
  SelectResultsPtr results = query(predicate, timeout);

  if (results == NULLPTR) {
    return false;
  }

  return results->size() > 0;
}

GfErrType ThinClientRegion::unregisterKeysBeforeDestroyRegion() {
  PoolPtr pool = PoolManager::find(getAttributes()->getPoolName());
  if (pool != NULLPTR) {
    if (!pool->getSubscriptionEnabled()) {
      LOGDEBUG(
          "pool subscription-enabled attribute is false, No need to Unregister "
          "keys");
      return GF_NOERR;
    }
  }
  GfErrType err = GF_NOERR;
  GfErrType opErr = GF_NOERR;

  opErr = unregisterStoredRegexLocalDestroy(m_interestListRegex);
  err = opErr != GF_NOERR ? opErr : err;
  opErr = unregisterStoredRegexLocalDestroy(m_durableInterestListRegex);
  err = opErr != GF_NOERR ? opErr : err;
  opErr = unregisterStoredRegexLocalDestroy(
      m_interestListRegexForUpdatesAsInvalidates);
  err = opErr != GF_NOERR ? opErr : err;
  opErr = unregisterStoredRegexLocalDestroy(
      m_durableInterestListRegexForUpdatesAsInvalidates);
  err = opErr != GF_NOERR ? opErr : err;

  VectorOfCacheableKey keysVec;
  copyInterestList(keysVec, m_interestList);
  opErr = unregisterKeysNoThrowLocalDestroy(keysVec, false);
  err = opErr != GF_NOERR ? opErr : err;

  VectorOfCacheableKey keysVecDurable;
  copyInterestList(keysVecDurable, m_durableInterestList);
  opErr = unregisterKeysNoThrowLocalDestroy(keysVecDurable, false);
  err = opErr != GF_NOERR ? opErr : err;

  VectorOfCacheableKey keysVecForUpdatesAsInvalidates;
  copyInterestList(keysVecForUpdatesAsInvalidates,
                   m_interestListForUpdatesAsInvalidates);
  opErr =
      unregisterKeysNoThrowLocalDestroy(keysVecForUpdatesAsInvalidates, false);
  err = opErr != GF_NOERR ? opErr : err;

  VectorOfCacheableKey keysVecDurableForUpdatesAsInvalidates;
  copyInterestList(keysVecDurableForUpdatesAsInvalidates,
                   m_durableInterestListForUpdatesAsInvalidates);
  opErr = unregisterKeysNoThrowLocalDestroy(
      keysVecDurableForUpdatesAsInvalidates, false);
  err = opErr != GF_NOERR ? opErr : err;
  return err;
}

SerializablePtr ThinClientRegion::selectValue(const char* predicate,
                                              uint32_t timeout) {
  SelectResultsPtr results = query(predicate, timeout);

  if (results == NULLPTR || results->size() == 0) {
    return NULLPTR;
  }

  if (results->size() > 1) {
    throw QueryException("selectValue has more than one result");
  }

  return results->operator[](0);
}

void ThinClientRegion::serverKeys(VectorOfCacheableKey& v) {
  CHECK_DESTROY_PENDING(TryReadGuard, Region::serverKeys);

  TcrMessageReply reply(true, m_tcrdm);
  TcrMessageKeySet request(m_fullPath, m_tcrdm);
  reply.setMessageTypeRequest(TcrMessage::KEY_SET);
  // need to check
  ChunkedKeySetResponse* resultCollector(
      new ChunkedKeySetResponse(request, v, reply));
  reply.setChunkedResultHandler(resultCollector);

  GfErrType err = GF_NOERR;

  err = m_tcrdm->sendSyncRequest(request, reply);

  GfErrTypeToException("Region::serverKeys", err);

  switch (reply.getMessageType()) {
    case TcrMessage::RESPONSE: {
      // keyset result is handled by ChunkedKeySetResponse
      break;
    }
    case TcrMessage::EXCEPTION: {
      err = handleServerException("Region:serverKeys", reply.getException());
      break;
    }
    case TcrMessage::KEY_SET_DATA_ERROR: {
      LOGERROR("Region serverKeys: an error occurred on the server");
      err = GF_CACHESERVER_EXCEPTION;
      break;
    }
    default: {
      LOGERROR("Unknown message type %d during region serverKeys",
               reply.getMessageType());
      err = GF_MSG;
      break;
    }
  }
  delete resultCollector;
  GfErrTypeToException("Region::serverKeys", err);
}

bool ThinClientRegion::containsKeyOnServer(
    const CacheableKeyPtr& keyPtr) const {
  GfErrType err = GF_NOERR;
  bool ret = false;
  TXState* txState = getTXState();

  if (txState != NULL) {
    //		if (!txState->isReplay()) {
    //			VectorOfCacheablePtr args(new VectorOfCacheable());
    //			txState->recordTXOperation(GF_CONTAINS_KEY,
    // getFullPath(),
    // keyPtr,
    //					args);
    //		}
  }

  /** @brief Create message and send to bridge server */

  TcrMessageContainsKey request(this, keyPtr, static_cast<UserDataPtr>(NULLPTR),
                                true, m_tcrdm);
  TcrMessageReply reply(true, m_tcrdm);
  reply.setMessageTypeRequest(TcrMessage::CONTAINS_KEY);
  err = m_tcrdm->sendSyncRequest(request, reply);
  // if ( err != GF_NOERR ) return ret;

  switch (reply.getMessageType()) {
    case TcrMessage::RESPONSE:
      ret = reply.getBoolValue();
      break;

    case TcrMessage::EXCEPTION:
      err = handleServerException("Region::containsKeyOnServer:",
                                  reply.getException());
      break;

    case TcrMessage::REQUEST_DATA_ERROR:
      LOGERROR(
          "Region::containsKeyOnServer: read error occurred on the endpoint %s",
          m_tcrdm->getActiveEndpoint()->name().c_str());
      err = GF_CACHESERVER_EXCEPTION;
      break;

    default:
      LOGERROR("Unknown message type in Region::containsKeyOnServer %d",
               reply.getMessageType());
      err = GF_MSG;
      break;
  }

  CacheableBooleanPtr rptr = CacheableBoolean::create(ret);

  rptr = handleReplay(err, rptr);
  GfErrTypeToException("Region::containsKeyOnServer ", err);
  return rptr->value();
}

bool ThinClientRegion::containsValueForKey_remote(
    const CacheableKeyPtr& keyPtr) const {
  GfErrType err = GF_NOERR;
  bool ret = false;
  TXState* txState = getTXState();

  if (txState != NULL) {
    //		if (!txState->isReplay()) {
    //			VectorOfCacheablePtr args(new VectorOfCacheable());
    //			txState->recordTXOperation(GF_CONTAINS_VALUE_FOR_KEY,
    //					getFullPath(), keyPtr, args);
    //		}
  }

  /** @brief Create message and send to bridge server */

  TcrMessageContainsKey request(this, keyPtr, static_cast<UserDataPtr>(NULLPTR),
                                false, m_tcrdm);
  TcrMessageReply reply(true, m_tcrdm);
  reply.setMessageTypeRequest(TcrMessage::CONTAINS_KEY);
  err = m_tcrdm->sendSyncRequest(request, reply);
  // if ( err != GF_NOERR ) return ret;

  switch (reply.getMessageType()) {
    case TcrMessage::RESPONSE:
      ret = reply.getBoolValue();
      break;

    case TcrMessage::EXCEPTION:
      err = handleServerException("Region::containsValueForKey:",
                                  reply.getException());
      break;

    case TcrMessage::REQUEST_DATA_ERROR:
      LOGERROR(
          "Region::containsValueForKey: read error occurred on the endpoint %s",
          m_tcrdm->getActiveEndpoint()->name().c_str());
      err = GF_CACHESERVER_EXCEPTION;
      break;

    default:
      LOGERROR("Unknown message type in Region::containsValueForKey %d",
               reply.getMessageType());
      err = GF_MSG;
      break;
  }

  CacheableBooleanPtr rptr = CacheableBoolean::create(ret);

  rptr = handleReplay(err, rptr);

  GfErrTypeToException("Region::containsValueForKey ", err);
  return rptr->value();
}

void ThinClientRegion::clear(const UserDataPtr& aCallbackArgument) {
  GfErrType err = GF_NOERR;
  err = localClearNoThrow(aCallbackArgument, CacheEventFlags::NORMAL);
  if (err != GF_NOERR) GfErrTypeToException("Region::clear", err);

  /** @brief Create message and send to bridge server */

  TcrMessageClearRegion request(this, aCallbackArgument, -1, m_tcrdm);
  TcrMessageReply reply(true, m_tcrdm);
  err = m_tcrdm->sendSyncRequest(request, reply);
  if (err != GF_NOERR) GfErrTypeToException("Region::clear", err);

  switch (reply.getMessageType()) {
    case TcrMessage::REPLY:
      LOGFINE("Region %s clear message sent to server successfully",
              m_fullPath.c_str());
      break;
    case TcrMessage::EXCEPTION:
      err = handleServerException("Region::clear:", reply.getException());
      break;

    case TcrMessage::CLEAR_REGION_DATA_ERROR:
      LOGERROR("Region clear read error occurred on the endpoint %s",
               m_tcrdm->getActiveEndpoint()->name().c_str());
      err = GF_CACHESERVER_EXCEPTION;
      break;

    default:
      LOGERROR("Unknown message type %d during region clear",
               reply.getMessageType());
      err = GF_MSG;
      break;
  }
  if (err == GF_NOERR) {
    err = invokeCacheListenerForRegionEvent(
        aCallbackArgument, CacheEventFlags::NORMAL, AFTER_REGION_CLEAR);
  }
  GfErrTypeToException("Region::clear", err);
}

GfErrType ThinClientRegion::getNoThrow_remote(
    const CacheableKeyPtr& keyPtr, CacheablePtr& valPtr,
    const UserDataPtr& aCallbackArgument, VersionTagPtr& versionTag) {
  GfErrType err = GF_NOERR;

  /** @brief Create message and send to bridge server */

  TcrMessageRequest request(this, keyPtr, aCallbackArgument, m_tcrdm);
  TcrMessageReply reply(true, m_tcrdm);
  err = m_tcrdm->sendSyncRequest(request, reply);
  if (err != GF_NOERR) return err;

  // put the object into local region
  switch (reply.getMessageType()) {
    case TcrMessage::RESPONSE: {
      valPtr = reply.getValue();
      versionTag = reply.getVersionTag();
      break;
    }
    case TcrMessage::EXCEPTION: {
      err = handleServerException("Region::get", reply.getException());
      break;
    }
    case TcrMessage::REQUEST_DATA_ERROR: {
      // LOGERROR("A read error occurred on the endpoint %s",
      //    m_tcrdm->getActiveEndpoint()->name().c_str());
      err = GF_CACHESERVER_EXCEPTION;
      break;
    }
    default: {
      LOGERROR("Unknown message type %d while getting entry from region",
               reply.getMessageType());
      err = GF_MSG;
      break;
    }
  }
  return err;
}

GfErrType ThinClientRegion::invalidateNoThrow_remote(
    const CacheableKeyPtr& keyPtr, const UserDataPtr& aCallbackArgument,
    VersionTagPtr& versionTag) {
  GfErrType err = GF_NOERR;

  /** @brief Create message and send to bridge server */

  TcrMessageInvalidate request(this, keyPtr, aCallbackArgument, m_tcrdm);
  TcrMessageReply reply(true, m_tcrdm);
  err = m_tcrdm->sendSyncRequest(request, reply);
  if (err != GF_NOERR) return err;

  // put the object into local region
  switch (reply.getMessageType()) {
    case TcrMessage::REPLY: {
      versionTag = reply.getVersionTag();
      break;
    }
    case TcrMessage::EXCEPTION: {
      err = handleServerException("Region::get", reply.getException());
      break;
    }
    case TcrMessage::INVALIDATE_ERROR: {
      // LOGERROR("A read error occurred on the endpoint %s",
      //    m_tcrdm->getActiveEndpoint()->name().c_str());
      err = GF_CACHESERVER_EXCEPTION;
      break;
    }
    default: {
      LOGERROR("Unknown message type %d while getting entry from region",
               reply.getMessageType());
      err = GF_MSG;
      break;
    }
  }
  return err;
}

GfErrType ThinClientRegion::putNoThrow_remote(
    const CacheableKeyPtr& keyPtr, const CacheablePtr& valuePtr,
    const UserDataPtr& aCallbackArgument, VersionTagPtr& versionTag,
    bool checkDelta) {
  GfErrType err = GF_NOERR;
  // do TCR put
  // bool delta = valuePtr->hasDelta();
  bool delta = false;
  const char* conFlationValue =
      DistributedSystem::getSystemProperties()->conflateEvents();
  if (checkDelta && valuePtr != NULLPTR && conFlationValue != NULL &&
      strcmp(conFlationValue, "true") != 0 &&
      ThinClientBaseDM::isDeltaEnabledOnServer()) {
    Delta* temp = dynamic_cast<Delta*>(valuePtr.ptr());
    delta = (temp && temp->hasDelta());
  }
  TcrMessagePut request(this, keyPtr, valuePtr, aCallbackArgument, delta,
                        m_tcrdm);
  TcrMessageReply* reply = new TcrMessageReply(true, m_tcrdm);
  err = m_tcrdm->sendSyncRequest(request, *reply);
  if (delta) {
    m_cacheImpl->m_cacheStats
        ->incDeltaPut();  // Does not chcek whether success of failure..
    if (reply->getMessageType() ==
        TcrMessage::PUT_DELTA_ERROR) {  // Try without delta
      TcrMessagePut request(this, keyPtr, valuePtr, aCallbackArgument, false,
                            m_tcrdm, false, true);
      delete reply;
      reply = new TcrMessageReply(true, m_tcrdm);
      err = m_tcrdm->sendSyncRequest(request, *reply);
    }
  }
  if (err != GF_NOERR) return err;

  // put the object into local region
  switch (reply->getMessageType()) {
    case TcrMessage::REPLY: {
      versionTag = reply->getVersionTag();
      break;
    }
    case TcrMessage::EXCEPTION: {
      err = handleServerException("Region::put", reply->getException());
      break;
    }
    case TcrMessage::PUT_DATA_ERROR: {
      // LOGERROR("A write error occurred on the endpoint %s",
      //    m_tcrdm->getActiveEndpoint()->name().c_str());
      err = GF_CACHESERVER_EXCEPTION;
      break;
    }
    default: {
      LOGERROR("Unknown message type %d during region put reply",
               reply->getMessageType());
      err = GF_MSG;
    }
  }
  delete reply;
  reply = NULL;
  return err;
}

GfErrType ThinClientRegion::createNoThrow_remote(
    const CacheableKeyPtr& keyPtr, const CacheablePtr& valuePtr,
    const UserDataPtr& aCallbackArgument, VersionTagPtr& versionTag) {
  return putNoThrow_remote(keyPtr, valuePtr, aCallbackArgument, versionTag,
                           false);
}

GfErrType ThinClientRegion::destroyNoThrow_remote(
    const CacheableKeyPtr& keyPtr, const UserDataPtr& aCallbackArgument,
    VersionTagPtr& versionTag) {
  GfErrType err = GF_NOERR;

  // do TCR destroy
  TcrMessageDestroy request(this, keyPtr, NULLPTR, aCallbackArgument, m_tcrdm);
  TcrMessageReply reply(true, m_tcrdm);
  err = m_tcrdm->sendSyncRequest(request, reply);
  if (err != GF_NOERR) return err;

  switch (reply.getMessageType()) {
    case TcrMessage::REPLY: {
      if (reply.getEntryNotFound() == 1) {
        err = GF_CACHE_ENTRY_NOT_FOUND;
      } else {
        LOGDEBUG("Remote key [%s] is destroyed successfully in region %s",
                 Utils::getCacheableKeyString(keyPtr)->asChar(),
                 m_fullPath.c_str());
      }
      versionTag = reply.getVersionTag();
      break;
    }
    case TcrMessage::EXCEPTION: {
      err = handleServerException("Region::destroy", reply.getException());
      break;
    }
    case TcrMessage::DESTROY_DATA_ERROR: {
      err = GF_CACHE_ENTRY_DESTROYED_EXCEPTION;
      break;
    }
    default: {
      LOGERROR("Unknown message type %d while destroying region entry",
               reply.getMessageType());
      err = GF_MSG;
    }
  }

  return err;
}  // destroyNoThrow_remote()

GfErrType ThinClientRegion::removeNoThrow_remote(
    const CacheableKeyPtr& keyPtr, const CacheablePtr& cvalue,
    const UserDataPtr& aCallbackArgument, VersionTagPtr& versionTag) {
  GfErrType err = GF_NOERR;

  // do TCR remove
  TcrMessageDestroy request(this, keyPtr, cvalue, aCallbackArgument, m_tcrdm);
  TcrMessageReply reply(true, m_tcrdm);
  err = m_tcrdm->sendSyncRequest(request, reply);
  if (err != GF_NOERR) {
    return err;
  }
  switch (reply.getMessageType()) {
    case TcrMessage::REPLY: {
      if (reply.getEntryNotFound() == 1) {
        err = GF_ENOENT;
      } else {
        LOGDEBUG("Remote key [%s] is removed successfully in region %s",
                 Utils::getCacheableKeyString(keyPtr)->asChar(),
                 m_fullPath.c_str());
      }
      versionTag = reply.getVersionTag();
      break;
    }
    case TcrMessage::EXCEPTION: {
      err = handleServerException("Region::remove", reply.getException());
      break;
    }
    case TcrMessage::DESTROY_DATA_ERROR: {
      err = GF_CACHE_ENTRY_DESTROYED_EXCEPTION;
      break;
    }
    default: {
      LOGERROR("Unknown message type %d while removing region entry",
               reply.getMessageType());
      err = GF_MSG;
    }
  }
  return err;
}

GfErrType ThinClientRegion::removeNoThrowEX_remote(
    const CacheableKeyPtr& keyPtr, const UserDataPtr& aCallbackArgument,
    VersionTagPtr& versionTag) {
  GfErrType err = GF_NOERR;

  // do TCR remove
  TcrMessageDestroy request(this, keyPtr, NULLPTR, aCallbackArgument, m_tcrdm);
  TcrMessageReply reply(true, m_tcrdm);
  err = m_tcrdm->sendSyncRequest(request, reply);
  if (err != GF_NOERR) {
    return err;
  }
  switch (reply.getMessageType()) {
    case TcrMessage::REPLY: {
      versionTag = reply.getVersionTag();
      if (reply.getEntryNotFound() == 1) {
        err = GF_ENOENT;
      } else {
        LOGDEBUG("Remote key [%s] is removed successfully in region %s",
                 Utils::getCacheableKeyString(keyPtr)->asChar(),
                 m_fullPath.c_str());
      }
      break;
    }
    case TcrMessage::EXCEPTION: {
      err = handleServerException("Region::removeEx", reply.getException());
      break;
    }
    case TcrMessage::DESTROY_DATA_ERROR: {
      err = GF_CACHE_ENTRY_DESTROYED_EXCEPTION;
      break;
    }
    default: {
      LOGERROR("Unknown message type %d while removing region entry",
               reply.getMessageType());
      err = GF_MSG;
    }
  }
  return err;
}

GfErrType ThinClientRegion::getAllNoThrow_remote(
    const VectorOfCacheableKey* keys, const HashMapOfCacheablePtr& values,
    const HashMapOfExceptionPtr& exceptions,
    const VectorOfCacheableKeyPtr& resultKeys, bool addToLocalCache,
    const UserDataPtr& aCallbackArgument) {
  GfErrType err = GF_NOERR;
  MapOfUpdateCounters updateCountMap;
  int32_t destroyTracker = 0;
  addToLocalCache = addToLocalCache && m_regionAttributes->getCachingEnabled();
  if (addToLocalCache && !m_regionAttributes->getConcurrencyChecksEnabled()) {
    // start tracking the entries
    if (keys == NULL) {
      // track all entries with destroy tracking for non-existent entries
      destroyTracker = m_entries->addTrackerForAllEntries(updateCountMap, true);
    } else {
      for (int32_t index = 0; index < keys->size(); ++index) {
        CacheablePtr oldValue;
        const CacheableKeyPtr& key = keys->operator[](index);
        int updateCount =
            m_entries->addTrackerForEntry(key, oldValue, true, false, false);
        updateCountMap.insert(std::make_pair(key, updateCount));
      }
    }
  }
  // create the GET_ALL request
  TcrMessageGetAll request(
      this, keys, m_tcrdm,
      aCallbackArgument);  // now we need to initialize later

  TcrMessageReply reply(true, m_tcrdm);
  ACE_Recursive_Thread_Mutex responseLock;
  // need to check
  TcrChunkedResult* resultCollector(new ChunkedGetAllResponse(
      reply, this, keys, values, exceptions, resultKeys, updateCountMap,
      destroyTracker, addToLocalCache, responseLock));

  reply.setChunkedResultHandler(resultCollector);
  err = m_tcrdm->sendSyncRequest(request, reply);

  if (addToLocalCache && !m_regionAttributes->getConcurrencyChecksEnabled()) {
    // remove the tracking for remaining keys in case some keys do not have
    // values from server in GII
    for (MapOfUpdateCounters::const_iterator iter = updateCountMap.begin();
         iter != updateCountMap.end(); ++iter) {
      if (iter->second >= 0) {
        m_entries->removeTrackerForEntry(iter->first);
      }
    }
    // remove tracking for destroys
    if (destroyTracker > 0) {
      m_entries->removeDestroyTracking();
    }
  }
  delete resultCollector;
  if (err != GF_NOERR) {
    return err;
  }

  switch (reply.getMessageType()) {
    case TcrMessage::RESPONSE: {
      // nothing to be done; put in local region, if required,
      // is handled by ChunkedGetAllResponse
      break;
    }
    case TcrMessage::EXCEPTION: {
      err = handleServerException("Region:getAll", reply.getException());
      break;
    }
    case TcrMessage::GET_ALL_DATA_ERROR: {
      LOGERROR("Region get-all: a read error occurred on the endpoint %s",
               m_tcrdm->getActiveEndpoint()->name().c_str());
      err = GF_CACHESERVER_EXCEPTION;
      break;
    }
    default: {
      LOGERROR("Unknown message type %d during region get-all",
               reply.getMessageType());
      err = GF_MSG;
      break;
    }
  }
  return err;
}

GfErrType ThinClientRegion::singleHopPutAllNoThrow_remote(
    ThinClientPoolDM* tcrdm, const HashMapOfCacheable& map,
    VersionedCacheableObjectPartListPtr& versionedObjPartList, uint32_t timeout,
    const UserDataPtr& aCallbackArgument) {
  LOGDEBUG(" ThinClientRegion::singleHopPutAllNoThrow_remote map size = %d",
           map.size());
  RegionPtr region(this);

  GfErrType error = GF_NOERR;
  /*Step-1::
   * populate the keys vector from the user Map and pass it to the
   * getServerToFilterMap to generate locationMap
   * If locationMap is NULL try the old, existing putAll impl that may take
   * multiple n/w hops
   */
  VectorOfCacheableKey* userKeys = new VectorOfCacheableKey();
  for (HashMapOfCacheable::Iterator iter = map.begin(); iter != map.end();
       ++iter) {
    userKeys->push_back(iter.first());
  }
  // last param in getServerToFilterMap() is false for putAll

  // LOGDEBUG("ThinClientRegion::singleHopPutAllNoThrow_remote keys.size() = %d
  // ", userKeys->size());
  HashMapT<BucketServerLocationPtr, VectorOfCacheableKeyPtr>* locationMap =
      tcrdm->getClientMetaDataService()->getServerToFilterMap(userKeys, region,
                                                              true);
  if (locationMap == NULL) {
    // putAll with multiple hop implementation
    LOGDEBUG("locationMap is Null or Empty");
    /*
    ==24194== 7,825,919 (24 direct, 7,825,895 indirect) bytes in 2 blocks are
    definitely lost in loss record 598 of 598
    ==24194==    at 0x4007D75: operator new(unsigned int)
    (vg_replace_malloc.c:313)
    ==24194==    by 0x43B5C5F:
    gemfire::ThinClientRegion::singleHopPutAllNoThrow_remote(gemfire::ThinClientPoolDM*,
    gemfire::HashMapOfCacheable const&,
    gemfire::SharedPtr<gemfire::VersionedCacheableObjectPartList>&, unsigned
    int) (ThinClientRegion.cpp:1180)
    ==24194==    by 0x43B8B65:
    gemfire::ThinClientRegion::putAllNoThrow_remote(gemfire::HashMapOfCacheable
    const&, gemfire::SharedPtr<gemfire::VersionedCacheableObjectPartList>&,
    unsigned int) (ThinClientRegion.cpp:1500)
    ==24194==    by 0x42E55F5:
    gemfire::LocalRegion::putAllNoThrow(gemfire::HashMapOfCacheable const&,
    unsigned int) (LocalRegion.cpp:1956)
    ==24194==    by 0x42DC797:
    gemfire::LocalRegion::putAll(gemfire::HashMapOfCacheable const&, unsigned
    int) (LocalRegion.cpp:366)
    ==24194==    by 0x806FF8D: Task_StepEight::doTask() (in
    /export/pnq-gst-dev01a/users/adongre/cedar_dev_Nov12/build-artifacts/linux/tests/cppcache/testThinClientPutAll)
    ==24194==    by 0x807CE2A: dunit::TestSlave::begin() (in
    /export/pnq-gst-dev01a/users/adongre/cedar_dev_Nov12/build-artifacts/linux/tests/cppcache/testThinClientPutAll)
    ==24194==    by 0x8078F57: dunit::dmain(int, char**) (in
    /export/pnq-gst-dev01a/users/adongre/cedar_dev_Nov12/build-artifacts/linux/tests/cppcache/testThinClientPutAll)
    ==24194==    by 0x805D7EA: main (in
    /export/pnq-gst-dev01a/users/adongre/cedar_dev_Nov12/build-artifacts/linux/tests/cppcache/testThinClientPutAll)
    ==24194==
    */
    delete userKeys;
    userKeys = NULL;

    return multiHopPutAllNoThrow_remote(map, versionedObjPartList, timeout,
                                        aCallbackArgument);
  }
  /*
  ==24194== 7,825,919 (24 direct, 7,825,895 indirect) bytes in 2 blocks are
  definitely lost in loss record 598 of 598
  ==24194==    at 0x4007D75: operator new(unsigned int)
  (vg_replace_malloc.c:313)
  ==24194==    by 0x43B5C5F:
  gemfire::ThinClientRegion::singleHopPutAllNoThrow_remote(gemfire::ThinClientPoolDM*,
  gemfire::HashMapOfCacheable const&,
  gemfire::SharedPtr<gemfire::VersionedCacheableObjectPartList>&, unsigned int)
  (ThinClientRegion.cpp:1180)
  ==24194==    by 0x43B8B65:
  gemfire::ThinClientRegion::putAllNoThrow_remote(gemfire::HashMapOfCacheable
  const&, gemfire::SharedPtr<gemfire::VersionedCacheableObjectPartList>&,
  unsigned int) (ThinClientRegion.cpp:1500)
  ==24194==    by 0x42E55F5:
  gemfire::LocalRegion::putAllNoThrow(gemfire::HashMapOfCacheable const&,
  unsigned int) (LocalRegion.cpp:1956)
  ==24194==    by 0x42DC797:
  gemfire::LocalRegion::putAll(gemfire::HashMapOfCacheable const&, unsigned int)
  (LocalRegion.cpp:366)
  ==24194==    by 0x806FF8D: Task_StepEight::doTask() (in
  /export/pnq-gst-dev01a/users/adongre/cedar_dev_Nov12/build-artifacts/linux/tests/cppcache/testThinClientPutAll)
  ==24194==    by 0x807CE2A: dunit::TestSlave::begin() (in
  /export/pnq-gst-dev01a/users/adongre/cedar_dev_Nov12/build-artifacts/linux/tests/cppcache/testThinClientPutAll)
  ==24194==    by 0x8078F57: dunit::dmain(int, char**) (in
  /export/pnq-gst-dev01a/users/adongre/cedar_dev_Nov12/build-artifacts/linux/tests/cppcache/testThinClientPutAll)
  ==24194==    by 0x805D7EA: main (in
  /export/pnq-gst-dev01a/users/adongre/cedar_dev_Nov12/build-artifacts/linux/tests/cppcache/testThinClientPutAll)
  ==24194==
  */
  delete userKeys;
  userKeys = NULL;

  // set this flag that indicates putAll on PR is invoked with singlehop
  // enabled.
  m_isPRSingleHopEnabled = true;
  // LOGDEBUG("locationMap.size() = %d ", locationMap->size());

  /*Step-2
   *  a. create vector of PutAllWork
   *  b. locationMap<BucketServerLocationPtr, VectorOfCacheableKeyPtr>.
   *     Create server specific filteredMap/subMap by populating all keys
   * (locationIter.second()) and its corr. values from the user Map.
   *  c. create new instance of PutAllWork, i.e worker with required params.
   *     //TODO:: Add details of each parameter later
   *  d. enqueue the worker for thread from threadPool to perform/run execute
   * method.
   *  e. insert the worker into the vector.
   */
  std::vector<PutAllWork*> putAllWorkers;
  ThreadPool* threadPool = TPSingleton::instance();
  int locationMapIndex = 0;
  for (HashMapT<BucketServerLocationPtr, VectorOfCacheableKeyPtr>::Iterator
           locationIter = locationMap->begin();
       locationIter != locationMap->end(); locationIter++) {
    BucketServerLocationPtr serverLocation = locationIter.first();
    if (serverLocation == NULLPTR) {
      LOGDEBUG("serverLocation is NULLPTR");
    }
    VectorOfCacheableKeyPtr keys = locationIter.second();

    // Create server specific Sub-Map by iterating over keys.
    HashMapOfCacheablePtr filteredMap(new HashMapOfCacheable());
    if (keys != NULLPTR && keys->size() > 0) {
      for (int32_t index = 0; index < keys->size(); index++) {
        HashMapOfCacheable::Iterator iter = map.find(keys->at(index));
        if (iter != map.end()) {
          filteredMap->insert(iter.first(), iter.second());
        } else {
          LOGDEBUG("DEBUG:: KEY not found in user MAP");
        }
      }
    }

    // TEST-CODE :: PRINT each sub-Map entries
    /*
    LOGDEBUG("Printing map at %d locationMapindex ", locationMapIndex);
    for (HashMapOfCacheable::Iterator filteredMapIter = filteredMap->begin();
    filteredMapIter != filteredMap->end(); ++filteredMapIter) {
      CacheableInt32Ptr kPtr =
    dynCast<CacheableInt32Ptr>(filteredMapIter.first()) ;
      CacheableInt32Ptr vPtr =
    dynCast<CacheableInt32Ptr>(filteredMapIter.second());
      LOGDEBUG("Key = %d  Value = %d ", kPtr->value(), vPtr->value() );
    }
    */

    PutAllWork* worker = new PutAllWork(
        tcrdm, serverLocation, region, true /*attemptFailover*/,
        false /*isBGThread*/, filteredMap, keys, timeout, aCallbackArgument);
    threadPool->perform(worker);
    putAllWorkers.push_back(worker);
    locationMapIndex++;
  }

  // TODO::CHECK, do we need to set following ..??
  // reply.setMessageType(TcrMessage::RESPONSE);

  int cnt = 1;

  /**
   * Step::3
   * a. Iterate over all vector of putAllWorkers and populate worker specific
   * information into the HashMap
   *    resultMap<BucketServerLocationPtr, SerializablePtr>, 2nd part, Value can
   * be a VersionedCacheableObjectPartListPtr or
   * PutAllPartialResultServerExceptionPtr.
   *    failedServers<BucketServerLocationPtr, CacheableInt32Ptr>, 2nd part,
   * Value is a ErrorCode.
   * b. delete the worker
   */
  HashMapT<BucketServerLocationPtr, SerializablePtr>* resultMap =
      new HashMapT<BucketServerLocationPtr, SerializablePtr>();
  HashMapT<BucketServerLocationPtr, CacheableInt32Ptr>* failedServers =
      new HashMapT<BucketServerLocationPtr, CacheableInt32Ptr>();

  for (std::vector<PutAllWork*>::iterator iter = putAllWorkers.begin();
       iter != putAllWorkers.end(); iter++) {
    PutAllWork* worker = *iter;

    GfErrType err =
        worker->getResult();  // wait() or blocking call for worker thread.
    LOGDEBUG("Error code :: %s:%d err = %d ", __FILE__, __LINE__, err);

    if (err != GF_NOERR) {
      error = err;

      if (error == GF_PUTALL_PARTIAL_RESULT_EXCEPTION) {
        resultMap->insert(worker->getServerLocation(),
                          worker->getPaPResultException());
      } else if (error == GF_NOTCON) {
        // Refresh the metadata in case of GF_NOTCON.
        tcrdm->getClientMetaDataService()->enqueueForMetadataRefresh(
            region->getFullPath(), 0);
      }
      failedServers->insert(worker->getServerLocation(),
                            CacheableInt32::create(error));
    } else {
      // No Exception from server
      resultMap->insert(worker->getServerLocation(),
                        worker->getResultCollector()->getList());
    }

    LOGDEBUG("worker->getPutAllMap()->size() = %d ",
             worker->getPutAllMap()->size());
    LOGDEBUG(
        "worker->getResultCollector()->getList()->getVersionedTagsize() = %d ",
        worker->getResultCollector()->getList()->getVersionedTagsize());

    // TODO::CHECK, why do we need following code... ??
    // TcrMessage* currentReply = worker->getReply();
    /*
    if(currentReply->getMessageType() != TcrMessage::REPLY)
    {
      reply.setMessageType(currentReply->getMessageType());
    }
    */

    delete worker;
    cnt++;
  }
  /**
   * Step:4
   * a. create instance of PutAllPartialResultPtr with total size= map.size()
   * b. Iterate over the resultMap and value for the particular serverlocation
   * is of type VersionedCacheableObjectPartList add keys and versions.
   * C. ToDO:: what if the value in the resultMap is of type
   * PutAllPartialResultServerException
   */
  ACE_Recursive_Thread_Mutex responseLock;
  PutAllPartialResultPtr result(
      new PutAllPartialResult(map.size(), responseLock));
  LOGDEBUG(
      " TCRegion:: %s:%d  "
      "result->getSucceededKeysAndVersions()->getVersionedTagsize() = %d ",
      __FILE__, __LINE__,
      result->getSucceededKeysAndVersions()->getVersionedTagsize());
  LOGDEBUG(" TCRegion:: %s:%d resultMap->size() ", __FILE__, __LINE__,
           resultMap->size());
  for (HashMapT<BucketServerLocationPtr, SerializablePtr>::Iterator
           resultMapIter = resultMap->begin();
       resultMapIter != resultMap->end(); resultMapIter++) {
    SerializablePtr value = resultMapIter.second();
    PutAllPartialResultServerException* papException = NULL;
    VersionedCacheableObjectPartListPtr list = NULLPTR;

    papException =
        dynamic_cast<PutAllPartialResultServerException*>(value.ptr());
    // LOGDEBUG(" TCRegion:: %s:%d
    // result->getSucceededKeysAndVersions()->getVersionedTagsize() = %d ",
    // __FILE__, __LINE__,
    // result->getSucceededKeysAndVersions()->getVersionedTagsize());
    if (papException != NULL) {
      // PutAllPartialResultServerException CASE:: value in resultMap is of type
      // PutAllPartialResultServerException.
      // TODO:: Add failedservers.keySet= all fialed servers, i.e list out all
      // keys in map failedServers,
      //       that is set view of the keys contained in failedservers map.
      // LOGDEBUG("TCRegion:: PutAll SingleHop encountered
      // PutAllPartialResultServerException exception: %s , failedServers are:
      // ", papException->getMessage()->asChar());
      // TODO:: need to read  papException and populate PutAllPartialResult.
      result->consolidate(papException->getResult());
    } else if (value != NULLPTR &&
               (list = dynCast<VersionedCacheableObjectPartListPtr>(value)) !=
                   NULLPTR) {
      // value in resultMap is of type VCOPL.
      // LOGDEBUG("TCRegion::  %s:%d :: list->getSucceededKeys()->size()=%d
      // list->getVersionedTagsize() = %d", __FILE__, __LINE__,
      // list->getSucceededKeys()->size(), list->getVersionedTagsize());
      result->addKeysAndVersions(list);
    } else {
      // ERROR CASE
      if (value != NULLPTR) {
        LOGERROR(
            "ERROR:: ThinClientRegion::singleHopPutAllNoThrow_remote value "
            "could not Cast to either VCOPL or "
            "PutAllPartialResultServerException:%s",
            value->toString()->asChar());
      } else {
        LOGERROR(
            "ERROR:: ThinClientRegion::singleHopPutAllNoThrow_remote value is "
            "NULL");
      }
    }
  }

  /**
   * a. if PutAllPartialResult result does not contains any entry,  Iterate over
   * locationMap.
   * b. Create VectorOfCacheableKey succeedKeySet, and keep adding set of keys
   * (locationIter.second()) in locationMap for which
   * failedServers->contains(locationIter.first()is false.
   */

  LOGDEBUG("ThinClientRegion:: %s:%d failedServers->size() = %d", __FILE__,
           __LINE__, failedServers->size());

  // if the partial result set doesn't already have keys (for tracking version
  // tags)
  // then we need to gather up the keys that we know have succeeded so far and
  // add them to the partial result set (See bug Id #955)
  if (failedServers->size() > 0) {
    VectorOfCacheableKeyPtr succeedKeySet(new VectorOfCacheableKey());
    if (result->getSucceededKeysAndVersions()->size() == 0) {
      for (HashMapT<BucketServerLocationPtr, VectorOfCacheableKeyPtr>::Iterator
               locationIter = locationMap->begin();
           locationIter != locationMap->end(); locationIter++) {
        if (!failedServers->contains(locationIter.first())) {
          for (int32_t i = 0; i < locationIter.second()->size(); i++) {
            succeedKeySet->push_back(locationIter.second()->at(i));
          }
        }
      }
      result->addKeys(succeedKeySet);
    }
  }

  /**
   * a. Iterate over the failedServers map
   * c. if failedServer map contains "GF_PUTALL_PARTIAL_RESULT_EXCEPTION" then
   * continue, Do not retry putAll for corr. keys.
   * b. Retry for all the failed server.
   *    Generate a newSubMap by finding Keys specific to failedServers from
   * locationMap and finding their respective values from the usermap.
   */
  error = GF_NOERR;
  bool oneSubMapRetryFailed = false;
  for (HashMapT<BucketServerLocationPtr, CacheableInt32Ptr>::Iterator
           failedServerIter = failedServers->begin();
       failedServerIter != failedServers->end(); failedServerIter++) {
    if (failedServerIter.second()->value() ==
        GF_PUTALL_PARTIAL_RESULT_EXCEPTION) {  // serverLocation
      // will not retry for PutAllPartialResultException
      // but it means at least one sub map ever failed
      oneSubMapRetryFailed = true;
      error = GF_PUTALL_PARTIAL_RESULT_EXCEPTION;
      continue;
    }
    HashMapT<BucketServerLocationPtr, VectorOfCacheableKeyPtr>::Iterator
        failedSerInLocMapIter = locationMap->find(failedServerIter.first());
    VectorOfCacheableKeyPtr failedKeys = NULLPTR;
    if (failedSerInLocMapIter != locationMap->end()) {
      failedKeys = failedSerInLocMapIter.second();
    }

    if (failedKeys == NULLPTR) {
      LOGERROR(
          "TCRegion::singleHopPutAllNoThrow_remote :: failedKeys are NULL that "
          "is not valid");
    }

    HashMapOfCacheablePtr newSubMap(new HashMapOfCacheable());
    if (failedKeys != NULLPTR && failedKeys->size() > 0) {
      for (int32_t index = 0; index < failedKeys->size(); index++) {
        HashMapOfCacheable::Iterator iter = map.find(failedKeys->at(index));
        if (iter != map.end()) {
          newSubMap->insert(iter.first(), iter.second());
        } else {
          LOGERROR(
              "DEBUG:: TCRegion.cpp singleHopPutAllNoThrow_remote KEY not "
              "found in user failedSubMap");
        }
      }
    }

    VersionedCacheableObjectPartListPtr vcopListPtr;
    PutAllPartialResultServerExceptionPtr papResultServerExc = NULLPTR;
    GfErrType errCode = multiHopPutAllNoThrow_remote(
        *newSubMap.ptr(), vcopListPtr, timeout, aCallbackArgument);
    if (errCode == GF_NOERR) {
      result->addKeysAndVersions(vcopListPtr);
    } else if (errCode == GF_PUTALL_PARTIAL_RESULT_EXCEPTION) {
      oneSubMapRetryFailed = true;
      // TODO:: Commented it as papResultServerExc is NULLPTR this time
      //       UnComment it once you read papResultServerExc.
      // result->consolidate(papResultServerExc->getResult());
      error = errCode;
    } else /*if(errCode != GF_NOERR)*/ {
      oneSubMapRetryFailed = true;
      CacheableKeyPtr firstKey = newSubMap->begin().first();
      ExceptionPtr excptPtr = NULLPTR;
      // TODO:: formulat excptPtr from the errCode
      result->saveFailedKey(firstKey, excptPtr);
      error = errCode;
    }
  }

  if (!oneSubMapRetryFailed) {
    error = GF_NOERR;
  }
  versionedObjPartList = result->getSucceededKeysAndVersions();
  LOGDEBUG("singlehop versionedObjPartList = %d error=%d",
           versionedObjPartList->size(), error);
  delete locationMap;
  delete failedServers;
  delete resultMap;

  return error;
}

GfErrType ThinClientRegion::multiHopPutAllNoThrow_remote(
    const HashMapOfCacheable& map,
    VersionedCacheableObjectPartListPtr& versionedObjPartList, uint32_t timeout,
    const UserDataPtr& aCallbackArgument) {
  // Multiple hop implementation
  LOGDEBUG("ThinClientRegion::multiHopPutAllNoThrow_remote ");
  GfErrType err = GF_NOERR;

  // Construct request/reply for putAll
  TcrMessagePutAll request(this, map, static_cast<int>(timeout * 1000), m_tcrdm,
                           aCallbackArgument);
  TcrMessageReply reply(true, m_tcrdm);
  request.setTimeout(timeout);
  reply.setTimeout(timeout);

  ACE_Recursive_Thread_Mutex responseLock;
  versionedObjPartList = new VersionedCacheableObjectPartList(responseLock);
  // need to check
  ChunkedPutAllResponse* resultCollector(new ChunkedPutAllResponse(
      RegionPtr(this), reply, responseLock, versionedObjPartList));
  reply.setChunkedResultHandler(resultCollector);

  err = m_tcrdm->sendSyncRequest(request, reply);

  versionedObjPartList = resultCollector->getList();
  LOGDEBUG("multiple hop versionedObjPartList size = %d , err = %d  ",
           versionedObjPartList->size(), err);
  delete resultCollector;
  if (err != GF_NOERR) return err;
  LOGDEBUG(
      "ThinClientRegion::multiHopPutAllNoThrow_remote reply.getMessageType() = "
      "%d ",
      reply.getMessageType());
  switch (reply.getMessageType()) {
    case TcrMessage::REPLY:
      // LOGDEBUG("Map is written into remote server at region %s",
      // m_fullPath.c_str());
      break;
    case TcrMessage::RESPONSE:
      LOGDEBUG(
          "multiHopPutAllNoThrow_remote TcrMessage::RESPONSE %s, err = %d ",
          m_fullPath.c_str(), err);
      break;
    case TcrMessage::EXCEPTION:
      err = handleServerException("ThinClientRegion::putAllNoThrow",
                                  reply.getException());
      // TODO:: Do we need to read PutAllPartialServerException for multiple
      // hop.
      break;
    case TcrMessage::PUT_DATA_ERROR:
      // LOGERROR( "A write error occurred on the endpoint %s",
      // m_tcrdm->getActiveEndpoint( )->name( ).c_str( ) );
      err = GF_CACHESERVER_EXCEPTION;
      break;
    default:
      LOGERROR("Unknown message type %d during region put-all",
               reply.getMessageType());
      err = GF_NOTOBJ;
      break;
  }
  return err;
}

GfErrType ThinClientRegion::putAllNoThrow_remote(
    const HashMapOfCacheable& map,
    VersionedCacheableObjectPartListPtr& versionedObjPartList, uint32_t timeout,
    const UserDataPtr& aCallbackArgument) {
  LOGDEBUG("ThinClientRegion::putAllNoThrow_remote");

  ThinClientPoolDM* poolDM = dynamic_cast<ThinClientPoolDM*>(m_tcrdm);
  TXState* txState = TSSTXStateWrapper::s_gemfireTSSTXState->getTXState();

  if (poolDM != NULL) {
    if (poolDM->getPRSingleHopEnabled() &&
        poolDM->getClientMetaDataService() != NULL &&
        txState == NULL /*For Tx use multi-hop*/) {
      return singleHopPutAllNoThrow_remote(poolDM, map, versionedObjPartList,
                                           timeout, aCallbackArgument);
    } else {
      return multiHopPutAllNoThrow_remote(map, versionedObjPartList, timeout,
                                          aCallbackArgument);
    }
  } else {
    LOGERROR("ThinClientRegion::putAllNoThrow_remote :: Pool Not Specified ");
    return GF_NOTSUP;
  }
}

GfErrType ThinClientRegion::singleHopRemoveAllNoThrow_remote(
    ThinClientPoolDM* tcrdm, const VectorOfCacheableKey& keys,
    VersionedCacheableObjectPartListPtr& versionedObjPartList,
    const UserDataPtr& aCallbackArgument) {
  LOGDEBUG(" ThinClientRegion::singleHopRemoveAllNoThrow_remote keys size = %d",
           keys.size());
  RegionPtr region(this);
  GfErrType error = GF_NOERR;

  HashMapT<BucketServerLocationPtr, VectorOfCacheableKeyPtr>* locationMap =
      tcrdm->getClientMetaDataService()->getServerToFilterMap(&keys, region,
                                                              true);
  if (locationMap == NULL) {
    // removeAll with multiple hop implementation
    LOGDEBUG("locationMap is Null or Empty");
    return multiHopRemoveAllNoThrow_remote(keys, versionedObjPartList,
                                           aCallbackArgument);
  }

  // set this flag that indicates putAll on PR is invoked with singlehop
  // enabled.
  m_isPRSingleHopEnabled = true;
  LOGDEBUG("locationMap.size() = %d ", locationMap->size());

  /*Step-2
   *  a. create vector of RemoveAllWork
   *  b. locationMap<BucketServerLocationPtr, VectorOfCacheableKeyPtr>.
   *     Create server specific filteredMap/subMap by populating all keys
   * (locationIter.second()) and its corr. values from the user Map.
   *  c. create new instance of RemoveAllWork, i.e worker with required params.
   *     //TODO:: Add details of each parameter later
   *  d. enqueue the worker for thread from threadPool to perform/run execute
   * method.
   *  e. insert the worker into the vector.
   */
  std::vector<RemoveAllWork*> removeAllWorkers;
  ThreadPool* threadPool = TPSingleton::instance();
  int locationMapIndex = 0;
  for (HashMapT<BucketServerLocationPtr, VectorOfCacheableKeyPtr>::Iterator
           locationIter = locationMap->begin();
       locationIter != locationMap->end(); locationIter++) {
    BucketServerLocationPtr serverLocation = locationIter.first();
    if (serverLocation == NULLPTR) {
      LOGDEBUG("serverLocation is NULLPTR");
    }
    VectorOfCacheableKeyPtr mappedkeys = locationIter.second();
    RemoveAllWork* worker = new RemoveAllWork(
        tcrdm, serverLocation, region, true /*attemptFailover*/,
        false /*isBGThread*/, mappedkeys, aCallbackArgument);
    threadPool->perform(worker);
    removeAllWorkers.push_back(worker);
    locationMapIndex++;
  }
  // TODO::CHECK, do we need to set following ..??
  // reply.setMessageType(TcrMessage::RESPONSE);

  int cnt = 1;

  /**
   * Step::3
   * a. Iterate over all vector of putAllWorkers and populate worker specific
   * information into the HashMap
   *    resultMap<BucketServerLocationPtr, SerializablePtr>, 2nd part, Value can
   * be a VersionedCacheableObjectPartListPtr or
   * PutAllPartialResultServerExceptionPtr.
   *    failedServers<BucketServerLocationPtr, CacheableInt32Ptr>, 2nd part,
   * Value is a ErrorCode.
   * b. delete the worker
   */
  HashMapT<BucketServerLocationPtr, SerializablePtr>* resultMap =
      new HashMapT<BucketServerLocationPtr, SerializablePtr>();
  HashMapT<BucketServerLocationPtr, CacheableInt32Ptr>* failedServers =
      new HashMapT<BucketServerLocationPtr, CacheableInt32Ptr>();
  for (std::vector<RemoveAllWork*>::iterator iter = removeAllWorkers.begin();
       iter != removeAllWorkers.end(); iter++) {
    RemoveAllWork* worker = *iter;

    GfErrType err =
        worker->getResult();  // wait() or blocking call for worker thread.
    LOGDEBUG("Error code :: %s:%d err = %d ", __FILE__, __LINE__, err);

    if (err != GF_NOERR) {
      error = err;

      if (error == GF_PUTALL_PARTIAL_RESULT_EXCEPTION) {
        resultMap->insert(worker->getServerLocation(),
                          worker->getPaPResultException());
      } else if (error == GF_NOTCON) {
        // Refresh the metadata in case of GF_NOTCON.
        tcrdm->getClientMetaDataService()->enqueueForMetadataRefresh(
            region->getFullPath(), 0);
      }
      failedServers->insert(worker->getServerLocation(),
                            CacheableInt32::create(error));
    } else {
      // No Exception from server
      resultMap->insert(worker->getServerLocation(),
                        worker->getResultCollector()->getList());
    }

    LOGDEBUG(
        "worker->getResultCollector()->getList()->getVersionedTagsize() = %d ",
        worker->getResultCollector()->getList()->getVersionedTagsize());

    delete worker;
    cnt++;
  }
  /**
   * Step:4
   * a. create instance of PutAllPartialResultPtr with total size= map.size()
   * b. Iterate over the resultMap and value for the particular serverlocation
   * is of type VersionedCacheableObjectPartList add keys and versions.
   * C. ToDO:: what if the value in the resultMap is of type
   * PutAllPartialResultServerException
   */
  ACE_Recursive_Thread_Mutex responseLock;
  PutAllPartialResultPtr result(
      new PutAllPartialResult(keys.size(), responseLock));
  LOGDEBUG(
      " TCRegion:: %s:%d  "
      "result->getSucceededKeysAndVersions()->getVersionedTagsize() = %d ",
      __FILE__, __LINE__,
      result->getSucceededKeysAndVersions()->getVersionedTagsize());
  LOGDEBUG(" TCRegion:: %s:%d resultMap->size() ", __FILE__, __LINE__,
           resultMap->size());
  for (HashMapT<BucketServerLocationPtr, SerializablePtr>::Iterator
           resultMapIter = resultMap->begin();
       resultMapIter != resultMap->end(); resultMapIter++) {
    SerializablePtr value = resultMapIter.second();
    PutAllPartialResultServerException* papException = NULL;
    VersionedCacheableObjectPartListPtr list = NULLPTR;

    papException =
        dynamic_cast<PutAllPartialResultServerException*>(value.ptr());
    if (papException != NULL) {
      // PutAllPartialResultServerException CASE:: value in resultMap is of type
      // PutAllPartialResultServerException.
      // TODO:: Add failedservers.keySet= all fialed servers, i.e list out all
      // keys in map failedServers,
      //       that is set view of the keys contained in failedservers map.
      // LOGDEBUG("TCRegion:: PutAll SingleHop encountered
      // PutAllPartialResultServerException exception: %s , failedServers are:
      // ", papException->getMessage()->asChar());
      // TODO:: need to read  papException and populate PutAllPartialResult.
      result->consolidate(papException->getResult());
    } else if (value != NULLPTR &&
               (list = dynCast<VersionedCacheableObjectPartListPtr>(value)) !=
                   NULLPTR) {
      // value in resultMap is of type VCOPL.
      // LOGDEBUG("TCRegion::  %s:%d :: list->getSucceededKeys()->size()=%d
      // list->getVersionedTagsize() = %d", __FILE__, __LINE__,
      // list->getSucceededKeys()->size(), list->getVersionedTagsize());
      result->addKeysAndVersions(list);
    } else {
      // ERROR CASE
      if (value != NULLPTR) {
        LOGERROR(
            "ERROR:: ThinClientRegion::singleHopRemoveAllNoThrow_remote value "
            "could not Cast to either VCOPL or "
            "PutAllPartialResultServerException:%s",
            value->toString()->asChar());
      } else {
        LOGERROR(
            "ERROR:: ThinClientRegion::singleHopRemoveAllNoThrow_remote value "
            "is NULL");
      }
    }
  }

  /**
   * a. if PutAllPartialResult result does not contains any entry,  Iterate over
   * locationMap.
   * b. Create VectorOfCacheableKey succeedKeySet, and keep adding set of keys
   * (locationIter.second()) in locationMap for which
   * failedServers->contains(locationIter.first()is false.
   */

  LOGDEBUG("ThinClientRegion:: %s:%d failedServers->size() = %d", __FILE__,
           __LINE__, failedServers->size());

  // if the partial result set doesn't already have keys (for tracking version
  // tags)
  // then we need to gather up the keys that we know have succeeded so far and
  // add them to the partial result set (See bug Id #955)
  if (failedServers->size() > 0) {
    VectorOfCacheableKeyPtr succeedKeySet(new VectorOfCacheableKey());
    if (result->getSucceededKeysAndVersions()->size() == 0) {
      for (HashMapT<BucketServerLocationPtr, VectorOfCacheableKeyPtr>::Iterator
               locationIter = locationMap->begin();
           locationIter != locationMap->end(); locationIter++) {
        if (!failedServers->contains(locationIter.first())) {
          for (int32_t i = 0; i < locationIter.second()->size(); i++) {
            succeedKeySet->push_back(locationIter.second()->at(i));
          }
        }
      }
      result->addKeys(succeedKeySet);
    }
  }

  /**
   * a. Iterate over the failedServers map
   * c. if failedServer map contains "GF_PUTALL_PARTIAL_RESULT_EXCEPTION" then
   * continue, Do not retry putAll for corr. keys.
   * b. Retry for all the failed server.
   *    Generate a newSubMap by finding Keys specific to failedServers from
   * locationMap and finding their respective values from the usermap.
   */
  error = GF_NOERR;
  bool oneSubMapRetryFailed = false;
  for (HashMapT<BucketServerLocationPtr, CacheableInt32Ptr>::Iterator
           failedServerIter = failedServers->begin();
       failedServerIter != failedServers->end(); failedServerIter++) {
    if (failedServerIter.second()->value() ==
        GF_PUTALL_PARTIAL_RESULT_EXCEPTION) {  // serverLocation
      // will not retry for PutAllPartialResultException
      // but it means at least one sub map ever failed
      oneSubMapRetryFailed = true;
      error = GF_PUTALL_PARTIAL_RESULT_EXCEPTION;
      continue;
    }
    HashMapT<BucketServerLocationPtr, VectorOfCacheableKeyPtr>::Iterator
        failedSerInLocMapIter = locationMap->find(failedServerIter.first());
    VectorOfCacheableKeyPtr failedKeys = NULLPTR;
    if (failedSerInLocMapIter != locationMap->end()) {
      failedKeys = failedSerInLocMapIter.second();
    }

    if (failedKeys == NULLPTR) {
      LOGERROR(
          "TCRegion::singleHopRemoveAllNoThrow_remote :: failedKeys are NULL "
          "that is not valid");
    }

    VersionedCacheableObjectPartListPtr vcopListPtr;
    PutAllPartialResultServerExceptionPtr papResultServerExc = NULLPTR;
    GfErrType errCode = multiHopRemoveAllNoThrow_remote(
        *failedKeys, vcopListPtr, aCallbackArgument);
    if (errCode == GF_NOERR) {
      result->addKeysAndVersions(vcopListPtr);
    } else if (errCode == GF_PUTALL_PARTIAL_RESULT_EXCEPTION) {
      oneSubMapRetryFailed = true;
      error = errCode;
    } else /*if(errCode != GF_NOERR)*/ {
      oneSubMapRetryFailed = true;
      ExceptionPtr excptPtr = NULLPTR;
      result->saveFailedKey(failedKeys->at(0), excptPtr);
      error = errCode;
    }
  }

  if (!oneSubMapRetryFailed) {
    error = GF_NOERR;
  }
  versionedObjPartList = result->getSucceededKeysAndVersions();
  LOGDEBUG("singlehop versionedObjPartList = %d error=%d",
           versionedObjPartList->size(), error);
  delete locationMap;
  delete failedServers;
  delete resultMap;
  return error;
}

GfErrType ThinClientRegion::multiHopRemoveAllNoThrow_remote(
    const VectorOfCacheableKey& keys,
    VersionedCacheableObjectPartListPtr& versionedObjPartList,
    const UserDataPtr& aCallbackArgument) {
  // Multiple hop implementation
  LOGDEBUG("ThinClientRegion::multiHopRemoveAllNoThrow_remote ");
  GfErrType err = GF_NOERR;

  // Construct request/reply for putAll
  TcrMessageRemoveAll request(this, keys, aCallbackArgument, m_tcrdm);
  TcrMessageReply reply(true, m_tcrdm);

  ACE_Recursive_Thread_Mutex responseLock;
  versionedObjPartList = new VersionedCacheableObjectPartList(responseLock);
  // need to check
  ChunkedRemoveAllResponse* resultCollector(new ChunkedRemoveAllResponse(
      RegionPtr(this), reply, responseLock, versionedObjPartList));
  reply.setChunkedResultHandler(resultCollector);

  err = m_tcrdm->sendSyncRequest(request, reply);

  versionedObjPartList = resultCollector->getList();
  LOGDEBUG("multiple hop versionedObjPartList size = %d , err = %d  ",
           versionedObjPartList->size(), err);
  delete resultCollector;
  if (err != GF_NOERR) return err;
  LOGDEBUG(
      "ThinClientRegion::multiHopRemoveAllNoThrow_remote "
      "reply.getMessageType() = %d ",
      reply.getMessageType());
  switch (reply.getMessageType()) {
    case TcrMessage::REPLY:
      // LOGDEBUG("Map is written into remote server at region %s",
      // m_fullPath.c_str());
      break;
    case TcrMessage::RESPONSE:
      LOGDEBUG(
          "multiHopRemoveAllNoThrow_remote TcrMessage::RESPONSE %s, err = %d ",
          m_fullPath.c_str(), err);
      break;
    case TcrMessage::EXCEPTION:
      err = handleServerException("ThinClientRegion::putAllNoThrow",
                                  reply.getException());
      break;
    default:
      LOGERROR("Unknown message type %d during region remove-all",
               reply.getMessageType());
      err = GF_NOTOBJ;
      break;
  }
  return err;
}

GfErrType ThinClientRegion::removeAllNoThrow_remote(
    const VectorOfCacheableKey& keys,
    VersionedCacheableObjectPartListPtr& versionedObjPartList,
    const UserDataPtr& aCallbackArgument) {
  LOGDEBUG("ThinClientRegion::removeAllNoThrow_remote");

  ThinClientPoolDM* poolDM = dynamic_cast<ThinClientPoolDM*>(m_tcrdm);
  TXState* txState = TSSTXStateWrapper::s_gemfireTSSTXState->getTXState();

  if (poolDM != NULL) {
    if (poolDM->getPRSingleHopEnabled() &&
        poolDM->getClientMetaDataService() != NULL &&
        txState == NULL /*For Tx use multi-hop*/) {
      return singleHopRemoveAllNoThrow_remote(
          poolDM, keys, versionedObjPartList, aCallbackArgument);
    } else {
      return multiHopRemoveAllNoThrow_remote(keys, versionedObjPartList,
                                             aCallbackArgument);
    }
  } else {
    LOGERROR(
        "ThinClientRegion::removeAllNoThrow_remote :: Pool Not Specified ");
    return GF_NOTSUP;
  }
}

uint32_t ThinClientRegion::size_remote() {
  LOGDEBUG("ThinClientRegion::size_remote");
  GfErrType err = GF_NOERR;

  // do TCR size
  TcrMessageSize request(m_fullPath.c_str());
  TcrMessageReply reply(true, m_tcrdm);
  err = m_tcrdm->sendSyncRequest(request, reply);

  if (err != GF_NOERR) {
    GfErrTypeToException("Region::size", err);
  }

  switch (reply.getMessageType()) {
    case TcrMessage::RESPONSE: {
      CacheableInt32Ptr size = staticCast<CacheableInt32Ptr>(reply.getValue());
      return size->value();
      // LOGINFO("Map is written into remote server at region %s",
      // m_fullPath.c_str());
    } break;
    case TcrMessage::EXCEPTION:
      err =
          handleServerException("ThinClientRegion::size", reply.getException());
      break;
    case TcrMessage::SIZE_ERROR:
      // LOGERROR( "A write error occurred on the endpoint %s",
      //     m_tcrdm->getActiveEndpoint( )->name( ).c_str( ) );
      err = GF_CACHESERVER_EXCEPTION;
      break;
    default:
      LOGERROR("Unknown message type %d during region size",
               reply.getMessageType());
      err = GF_NOTOBJ;
  }

  GfErrTypeToException("Region::size", err);
  return 0;
}

GfErrType ThinClientRegion::registerStoredRegex(
    TcrEndpoint* endpoint,
    std::unordered_map<std::string, InterestResultPolicy>& interestListRegex,
    bool isDurable, bool receiveValues) {
  GfErrType opErr = GF_NOERR;
  GfErrType retVal = GF_NOERR;

  for (std::unordered_map<std::string, InterestResultPolicy>::iterator it =
           interestListRegex.begin();
       it != interestListRegex.end(); ++it) {
    opErr = registerRegexNoThrow(it->first, false, endpoint, isDurable, NULLPTR,
                                 it->second, receiveValues);
    if (opErr != GF_NOERR) {
      retVal = opErr;
    }
  }

  return retVal;
}

GfErrType ThinClientRegion::registerKeys(TcrEndpoint* endpoint,
                                         const TcrMessage* request,
                                         TcrMessageReply* reply) {
  GfErrType err = GF_NOERR;
  GfErrType opErr = GF_NOERR;

  // called when failover to a different server
  opErr = registerStoredRegex(endpoint, m_interestListRegex);
  err = opErr != GF_NOERR ? opErr : err;
  opErr = registerStoredRegex(
      endpoint, m_interestListRegexForUpdatesAsInvalidates, false, false);
  err = opErr != GF_NOERR ? opErr : err;
  opErr = registerStoredRegex(endpoint, m_durableInterestListRegex, true);
  err = opErr != GF_NOERR ? opErr : err;
  opErr = registerStoredRegex(
      endpoint, m_durableInterestListRegexForUpdatesAsInvalidates, true, false);
  err = opErr != GF_NOERR ? opErr : err;

  VectorOfCacheableKey keysVec;
  InterestResultPolicy interestPolicy =
      copyInterestList(keysVec, m_interestList);
  opErr = registerKeysNoThrow(keysVec, false, endpoint, false, interestPolicy);
  err = opErr != GF_NOERR ? opErr : err;

  VectorOfCacheableKey keysVecForUpdatesAsInvalidates;
  interestPolicy = copyInterestList(keysVecForUpdatesAsInvalidates,
                                    m_interestListForUpdatesAsInvalidates);
  opErr = registerKeysNoThrow(keysVecForUpdatesAsInvalidates, false, endpoint,
                              false, interestPolicy, false);
  err = opErr != GF_NOERR ? opErr : err;

  VectorOfCacheableKey keysVecDurable;
  interestPolicy = copyInterestList(keysVecDurable, m_durableInterestList);
  opErr = registerKeysNoThrow(keysVecDurable, false, endpoint, true,
                              interestPolicy);
  err = opErr != GF_NOERR ? opErr : err;

  VectorOfCacheableKey keysVecDurableForUpdatesAsInvalidates;
  interestPolicy =
      copyInterestList(keysVecDurableForUpdatesAsInvalidates,
                       m_durableInterestListForUpdatesAsInvalidates);
  opErr = registerKeysNoThrow(keysVecDurableForUpdatesAsInvalidates, false,
                              endpoint, true, interestPolicy, false);
  err = opErr != GF_NOERR ? opErr : err;

  if (request != NULL && request->getRegionName() == m_fullPath &&
      (request->getMessageType() == TcrMessage::REGISTER_INTEREST ||
       request->getMessageType() == TcrMessage::REGISTER_INTEREST_LIST)) {
    const VectorOfCacheableKey* newKeysVec = request->getKeys();
    bool isDurable = request->isDurable();
    bool receiveValues = request->receiveValues();
    if (newKeysVec == NULL || newKeysVec->empty()) {
      const std::string& newRegex = request->getRegex();
      if (!newRegex.empty()) {
        if (request->getRegionName() != m_fullPath) {
          reply = NULL;
        }
        opErr = registerRegexNoThrow(
            newRegex, false, endpoint, isDurable, NULLPTR,
            request->getInterestResultPolicy(), receiveValues, reply);
        err = opErr != GF_NOERR ? opErr : err;
      }
    } else {
      opErr = registerKeysNoThrow(*newKeysVec, false, endpoint, isDurable,
                                  request->getInterestResultPolicy(),
                                  receiveValues, reply);
      err = opErr != GF_NOERR ? opErr : err;
    }
  }
  return err;
}

GfErrType ThinClientRegion::unregisterStoredRegex(
    std::unordered_map<std::string, InterestResultPolicy>& interestListRegex) {
  GfErrType opErr = GF_NOERR;
  GfErrType retVal = GF_NOERR;

  for (std::unordered_map<std::string, InterestResultPolicy>::iterator it =
           interestListRegex.begin();
       it != interestListRegex.end(); ++it) {
    opErr = unregisterRegexNoThrow(it->first, false);
    if (opErr != GF_NOERR) {
      retVal = opErr;
    }
  }

  return retVal;
}

GfErrType ThinClientRegion::unregisterStoredRegexLocalDestroy(
    std::unordered_map<std::string, InterestResultPolicy>& interestListRegex) {
  GfErrType opErr = GF_NOERR;
  GfErrType retVal = GF_NOERR;

  for (std::unordered_map<std::string, InterestResultPolicy>::iterator it =
           interestListRegex.begin();
       it != interestListRegex.end(); ++it) {
    opErr = unregisterRegexNoThrowLocalDestroy(it->first, false);
    if (opErr != GF_NOERR) {
      retVal = opErr;
    }
  }
  return retVal;
}

GfErrType ThinClientRegion::unregisterKeys() {
  GfErrType err = GF_NOERR;
  GfErrType opErr = GF_NOERR;

  // called when disconnect from a server
  opErr = unregisterStoredRegex(m_interestListRegex);
  err = opErr != GF_NOERR ? opErr : err;
  opErr = unregisterStoredRegex(m_durableInterestListRegex);
  err = opErr != GF_NOERR ? opErr : err;
  opErr = unregisterStoredRegex(m_interestListRegexForUpdatesAsInvalidates);
  err = opErr != GF_NOERR ? opErr : err;
  opErr =
      unregisterStoredRegex(m_durableInterestListRegexForUpdatesAsInvalidates);
  err = opErr != GF_NOERR ? opErr : err;

  VectorOfCacheableKey keysVec;
  copyInterestList(keysVec, m_interestList);
  opErr = unregisterKeysNoThrow(keysVec, false);
  err = opErr != GF_NOERR ? opErr : err;

  VectorOfCacheableKey keysVecDurable;
  copyInterestList(keysVecDurable, m_durableInterestList);
  opErr = unregisterKeysNoThrow(keysVecDurable, false);
  err = opErr != GF_NOERR ? opErr : err;

  VectorOfCacheableKey keysVecForUpdatesAsInvalidates;
  copyInterestList(keysVecForUpdatesAsInvalidates,
                   m_interestListForUpdatesAsInvalidates);
  opErr = unregisterKeysNoThrow(keysVecForUpdatesAsInvalidates, false);
  err = opErr != GF_NOERR ? opErr : err;

  VectorOfCacheableKey keysVecDurableForUpdatesAsInvalidates;
  copyInterestList(keysVecDurableForUpdatesAsInvalidates,
                   m_durableInterestListForUpdatesAsInvalidates);
  opErr = unregisterKeysNoThrow(keysVecDurableForUpdatesAsInvalidates, false);
  err = opErr != GF_NOERR ? opErr : err;

  return err;
}

GfErrType ThinClientRegion::destroyRegionNoThrow_remote(
    const UserDataPtr& aCallbackArgument) {
  GfErrType err = GF_NOERR;

  // do TCR destroyRegion
  TcrMessageDestroyRegion request(this, aCallbackArgument, -1, m_tcrdm);
  TcrMessageReply reply(true, m_tcrdm);
  err = m_tcrdm->sendSyncRequest(request, reply);
  if (err != GF_NOERR) return err;

  switch (reply.getMessageType()) {
    case TcrMessage::REPLY: {
      // LOGINFO("Region %s at remote is destroyed successfully",
      // m_fullPath.c_str());
      break;
    }
    case TcrMessage::EXCEPTION: {
      err =
          handleServerException("Region::destroyRegion", reply.getException());
      break;
    }
    case TcrMessage::DESTROY_REGION_DATA_ERROR: {
      err = GF_CACHE_REGION_DESTROYED_EXCEPTION;
      break;
    }
    default: {
      LOGERROR("Unknown message type %d during destroy region",
               reply.getMessageType());
      err = GF_MSG;
    }
  }
  return err;
}

GfErrType ThinClientRegion::registerKeysNoThrow(
    const VectorOfCacheableKey& keys, bool attemptFailover,
    TcrEndpoint* endpoint, bool isDurable, InterestResultPolicy interestPolicy,
    bool receiveValues, TcrMessageReply* reply) {
  RegionGlobalLocks acquireLocksRedundancy(this, false);
  RegionGlobalLocks acquireLocksFailover(this);
  CHECK_DESTROY_PENDING_NOTHROW(TryReadGuard);
  GfErrType err = GF_NOERR;

  ACE_Guard<ACE_Recursive_Thread_Mutex> keysGuard(m_keysLock);
  if (keys.empty()) {
    return err;
  }

  TcrMessageReply replyLocal(true, m_tcrdm);
  bool needToCreateRC = true;
  if (reply == NULL) {
    reply = &replyLocal;
  } else {
    needToCreateRC = false;
  }

  LOGDEBUG("ThinClientRegion::registerKeysNoThrow : interestpolicy is %d",
           interestPolicy.ordinal);

  TcrMessageRegisterInterestList request(
      this, keys, isDurable, getAttributes()->getCachingEnabled(),
      receiveValues, interestPolicy, m_tcrdm);
  ACE_Recursive_Thread_Mutex responseLock;
  TcrChunkedResult* resultCollector = NULL;
  if (interestPolicy.ordinal == InterestResultPolicy::KEYS_VALUES.ordinal) {
    HashMapOfCacheablePtr values(new HashMapOfCacheable());
    HashMapOfExceptionPtr exceptions(new HashMapOfException());
    MapOfUpdateCounters trackers;
    int32_t destroyTracker = 1;
    if (needToCreateRC) {
      resultCollector = (new ChunkedGetAllResponse(
          request, this, &keys, values, exceptions, NULLPTR, trackers,
          destroyTracker, true, responseLock));
      reply->setChunkedResultHandler(resultCollector);
    }
  } else {
    if (needToCreateRC) {
      resultCollector = (new ChunkedInterestResponse(request, NULLPTR, *reply));
      reply->setChunkedResultHandler(resultCollector);
    }
  }

  err = m_tcrdm->sendSyncRequestRegisterInterest(
      request, *reply, attemptFailover, this, endpoint);

  if (err == GF_NOERR /*|| err == GF_CACHE_REDUNDANCY_FAILURE*/) {
    if (reply->getMessageType() == TcrMessage::RESPONSE_FROM_SECONDARY) {
      LOGFINER(
          "registerKeysNoThrow - got response from secondary for "
          "endpoint %s, ignoring.",
          endpoint->name().c_str());
    } else if (attemptFailover) {
      addKeys(keys, isDurable, receiveValues, interestPolicy);
      if (!(interestPolicy.ordinal ==
            InterestResultPolicy::KEYS_VALUES.ordinal)) {
        localInvalidateForRegisterInterest(keys);
      }
    }
  }
  if (needToCreateRC) {
    delete resultCollector;
  }
  return err;
}

GfErrType ThinClientRegion::unregisterKeysNoThrow(
    const VectorOfCacheableKey& keys, bool attemptFailover) {
  RegionGlobalLocks acquireLocksRedundancy(this, false);
  RegionGlobalLocks acquireLocksFailover(this);
  CHECK_DESTROY_PENDING_NOTHROW(TryReadGuard);
  GfErrType err = GF_NOERR;
  ACE_Guard<ACE_Recursive_Thread_Mutex> keysGuard(m_keysLock);
  TcrMessageReply reply(true, m_tcrdm);
  if (keys.empty()) {
    return err;
  }

  if (m_interestList.empty() && m_durableInterestList.empty() &&
      m_interestListForUpdatesAsInvalidates.empty() &&
      m_durableInterestListForUpdatesAsInvalidates.empty()) {
    // did not register any keys before.
    return GF_CACHE_ILLEGAL_STATE_EXCEPTION;
  }

  TcrMessageUnregisterInterestList request(this, keys, false, false, true,
                                           InterestResultPolicy::NONE, m_tcrdm);
  err = m_tcrdm->sendSyncRequestRegisterInterest(request, reply);
  if (err == GF_NOERR /*|| err == GF_CACHE_REDUNDANCY_FAILURE*/) {
    if (attemptFailover) {
      for (VectorOfCacheableKey::Iterator iter = keys.begin();
           iter != keys.end(); ++iter) {
        m_interestList.erase(*iter);
        m_durableInterestList.erase(*iter);
        m_interestListForUpdatesAsInvalidates.erase(*iter);
        m_durableInterestListForUpdatesAsInvalidates.erase(*iter);
      }
    }
  }
  return err;
}

GfErrType ThinClientRegion::unregisterKeysNoThrowLocalDestroy(
    const VectorOfCacheableKey& keys, bool attemptFailover) {
  RegionGlobalLocks acquireLocksRedundancy(this, false);
  RegionGlobalLocks acquireLocksFailover(this);
  GfErrType err = GF_NOERR;
  ACE_Guard<ACE_Recursive_Thread_Mutex> keysGuard(m_keysLock);
  TcrMessageReply reply(true, m_tcrdm);
  if (keys.empty()) {
    return err;
  }

  if (m_interestList.empty() && m_durableInterestList.empty() &&
      m_interestListForUpdatesAsInvalidates.empty() &&
      m_durableInterestListForUpdatesAsInvalidates.empty()) {
    // did not register any keys before.
    return GF_CACHE_ILLEGAL_STATE_EXCEPTION;
  }

  TcrMessageUnregisterInterestList request(this, keys, false, false, true,
                                           InterestResultPolicy::NONE, m_tcrdm);
  err = m_tcrdm->sendSyncRequestRegisterInterest(request, reply);
  if (err == GF_NOERR) {
    if (attemptFailover) {
      for (VectorOfCacheableKey::Iterator iter = keys.begin();
           iter != keys.end(); ++iter) {
        m_interestList.erase(*iter);
        m_durableInterestList.erase(*iter);
        m_interestListForUpdatesAsInvalidates.erase(*iter);
        m_durableInterestListForUpdatesAsInvalidates.erase(*iter);
      }
    }
  }
  return err;
}

bool ThinClientRegion::isRegexRegistered(
    std::unordered_map<std::string, InterestResultPolicy>& interestListRegex,
    const std::string& regex, bool allKeys) {
  if (interestListRegex.find(".*") != interestListRegex.end() ||
      (!allKeys && interestListRegex.find(regex) != interestListRegex.end())) {
    return true;
  }
  return false;
}

GfErrType ThinClientRegion::registerRegexNoThrow(
    const std::string& regex, bool attemptFailover, TcrEndpoint* endpoint,
    bool isDurable, VectorOfCacheableKeyPtr resultKeys,
    InterestResultPolicy interestPolicy, bool receiveValues,
    TcrMessageReply* reply) {
  RegionGlobalLocks acquireLocksRedundancy(this, false);
  RegionGlobalLocks acquireLocksFailover(this);
  CHECK_DESTROY_PENDING_NOTHROW(TryReadGuard);
  GfErrType err = GF_NOERR;

  bool allKeys = (regex == ".*");
  ACE_Guard<ACE_Recursive_Thread_Mutex> keysGuard(m_keysLock);

  if (attemptFailover) {
    if ((isDurable &&
         (isRegexRegistered(m_durableInterestListRegex, regex, allKeys) ||
          isRegexRegistered(m_durableInterestListRegexForUpdatesAsInvalidates,
                            regex, allKeys))) ||
        (!isDurable &&
         (isRegexRegistered(m_interestListRegex, regex, allKeys) ||
          isRegexRegistered(m_interestListRegexForUpdatesAsInvalidates, regex,
                            allKeys)))) {
      return err;
    }
  }

  TcrMessageReply replyLocal(true, m_tcrdm);
  ChunkedInterestResponse* resultCollector = NULL;
  ChunkedGetAllResponse* getAllResultCollector = NULL;
  if (reply != NULL) {
    // need to check
    resultCollector = dynamic_cast<ChunkedInterestResponse*>(
        reply->getChunkedResultHandler());
    if (resultCollector != NULL) {
      resultKeys = resultCollector->getResultKeys();
    } else {
      getAllResultCollector = dynamic_cast<ChunkedGetAllResponse*>(
          reply->getChunkedResultHandler());
      resultKeys = getAllResultCollector->getResultKeys();
    }
  }

  bool isRCCreatedLocally = false;
  LOGDEBUG("ThinClientRegion::registerRegexNoThrow : interestpolicy is %d",
           interestPolicy.ordinal);

  // TODO:
  TcrMessageRegisterInterest request(
      m_fullPath, regex.c_str(), interestPolicy, isDurable,
      getAttributes()->getCachingEnabled(), receiveValues, m_tcrdm);
  ACE_Recursive_Thread_Mutex responseLock;
  if (reply == NULL) {
    reply = &replyLocal;
    if (interestPolicy.ordinal == InterestResultPolicy::KEYS_VALUES.ordinal) {
      HashMapOfCacheablePtr values(new HashMapOfCacheable());
      HashMapOfExceptionPtr exceptions(new HashMapOfException());
      MapOfUpdateCounters trackers;
      int32_t destroyTracker = 1;
      if (resultKeys == NULLPTR) {
        resultKeys = VectorOfCacheableKeyPtr(new VectorOfCacheableKey());
      }
      // need to check
      getAllResultCollector = (new ChunkedGetAllResponse(
          request, this, NULL, values, exceptions, resultKeys, trackers,
          destroyTracker, true, responseLock));
      reply->setChunkedResultHandler(getAllResultCollector);
      isRCCreatedLocally = true;
    } else {
      isRCCreatedLocally = true;
      // need to check
      resultCollector =
          new ChunkedInterestResponse(request, resultKeys, replyLocal);
      reply->setChunkedResultHandler(resultCollector);
    }
  }
  err = m_tcrdm->sendSyncRequestRegisterInterest(
      request, *reply, attemptFailover, this, endpoint);
  if (err == GF_NOERR /*|| err == GF_CACHE_REDUNDANCY_FAILURE*/) {
    if (reply->getMessageType() == TcrMessage::RESPONSE_FROM_SECONDARY) {
      LOGFINER(
          "registerRegexNoThrow - got response from secondary for "
          "endpoint %s, ignoring.",
          endpoint->name().c_str());
    } else if (attemptFailover) {
      addRegex(regex, isDurable, receiveValues, interestPolicy);
      if (interestPolicy.ordinal != InterestResultPolicy::KEYS_VALUES.ordinal) {
        if (allKeys) {
          localInvalidateRegion_internal();
        } else {
          const VectorOfCacheableKeyPtr& keys =
              resultCollector != NULL ? resultCollector->getResultKeys()
                                      : getAllResultCollector->getResultKeys();
          if (keys != NULLPTR) {
            localInvalidateForRegisterInterest(*keys);
          }
        }
      }
    }
  }

  if (isRCCreatedLocally == true) {
    if (resultCollector != NULL) delete resultCollector;
    if (getAllResultCollector != NULL) delete getAllResultCollector;
  }
  return err;
}

GfErrType ThinClientRegion::unregisterRegexNoThrow(const std::string& regex,
                                                   bool attemptFailover) {
  RegionGlobalLocks acquireLocksRedundancy(this, false);
  RegionGlobalLocks acquireLocksFailover(this);
  CHECK_DESTROY_PENDING_NOTHROW(TryReadGuard);
  GfErrType err = GF_NOERR;

  err = findRegex(regex);

  if (err == GF_NOERR) {
    TcrMessageReply reply(false, m_tcrdm);
    TcrMessageUnregisterInterest request(m_fullPath, regex,
                                         InterestResultPolicy::NONE, false,
                                         false, true, m_tcrdm);
    err = m_tcrdm->sendSyncRequestRegisterInterest(request, reply);
    if (err == GF_NOERR /*|| err == GF_CACHE_REDUNDANCY_FAILURE*/) {
      if (attemptFailover) {
        clearRegex(regex);
      }
    }
  }
  return err;
}

GfErrType ThinClientRegion::findRegex(const std::string& regex) {
  GfErrType err = GF_NOERR;
  ACE_Guard<ACE_Recursive_Thread_Mutex> keysGuard(m_keysLock);

  if (m_interestListRegex.find(regex) == m_interestListRegex.end() &&
      m_durableInterestListRegex.find(regex) ==
          m_durableInterestListRegex.end() &&
      m_interestListRegexForUpdatesAsInvalidates.find(regex) ==
          m_interestListRegexForUpdatesAsInvalidates.end() &&
      m_durableInterestListRegexForUpdatesAsInvalidates.find(regex) ==
          m_durableInterestListRegexForUpdatesAsInvalidates.end()) {
    return GF_CACHE_ILLEGAL_STATE_EXCEPTION;
  } else {
    return err;
  }
}

void ThinClientRegion::clearRegex(const std::string& regex) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> keysGuard(m_keysLock);
  m_interestListRegex.erase(regex);
  m_durableInterestListRegex.erase(regex);
  m_interestListRegexForUpdatesAsInvalidates.erase(regex);
  m_durableInterestListRegexForUpdatesAsInvalidates.erase(regex);
}

GfErrType ThinClientRegion::unregisterRegexNoThrowLocalDestroy(
    const std::string& regex, bool attemptFailover) {
  GfErrType err = GF_NOERR;

  err = findRegex(regex);

  if (err == GF_NOERR) {
    TcrMessageReply reply(false, m_tcrdm);
    TcrMessageUnregisterInterest request(m_fullPath, regex,
                                         InterestResultPolicy::NONE, false,
                                         false, true, m_tcrdm);
    err = m_tcrdm->sendSyncRequestRegisterInterest(request, reply);
    if (err == GF_NOERR) {
      if (attemptFailover) {
        clearRegex(regex);
      }
    }
  }
  return err;
}

void ThinClientRegion::addKeys(const VectorOfCacheableKey& keys, bool isDurable,
                               bool receiveValues,
                               InterestResultPolicy interestpolicy) {
  std::unordered_map<CacheableKeyPtr, InterestResultPolicy>& interestList =
      isDurable ? (receiveValues ? m_durableInterestList
                                 : m_durableInterestListForUpdatesAsInvalidates)
                : (receiveValues ? m_interestList
                                 : m_interestListForUpdatesAsInvalidates);

  for (VectorOfCacheableKey::Iterator iter = keys.begin(); iter != keys.end();
       ++iter) {
    interestList.insert(std::pair<CacheableKeyPtr, InterestResultPolicy>(
        *iter, interestpolicy));
  }
}

void ThinClientRegion::addRegex(const std::string& regex, bool isDurable,
                                bool receiveValues,
                                InterestResultPolicy interestpolicy) {
  std::unordered_map<CacheableKeyPtr, InterestResultPolicy>& interestList =
      isDurable ? (receiveValues ? m_durableInterestList
                                 : m_durableInterestListForUpdatesAsInvalidates)
                : (receiveValues ? m_interestList
                                 : m_interestListForUpdatesAsInvalidates);

  std::unordered_map<std::string, InterestResultPolicy>& interestListRegex =
      isDurable
          ? (receiveValues ? m_durableInterestListRegex
                           : m_durableInterestListRegexForUpdatesAsInvalidates)
          : (receiveValues ? m_interestListRegex
                           : m_interestListRegexForUpdatesAsInvalidates);

  if (regex == ".*") {
    interestListRegex.clear();
    interestList.clear();
  }

  interestListRegex.insert(
      std::pair<std::string, InterestResultPolicy>(regex, interestpolicy));
}

void ThinClientRegion::getInterestList(VectorOfCacheableKey& vlist) const {
  ThinClientRegion* nthis = const_cast<ThinClientRegion*>(this);
  RegionGlobalLocks acquireLocksRedundancy(nthis, false);
  RegionGlobalLocks acquireLocksFailover(nthis);
  CHECK_DESTROY_PENDING(TryReadGuard, getInterestList);
  ACE_Guard<ACE_Recursive_Thread_Mutex> keysGuard(nthis->m_keysLock);
  for (std::unordered_map<CacheableKeyPtr, InterestResultPolicy>::iterator itr =
           nthis->m_durableInterestList.begin();
       itr != nthis->m_durableInterestList.end(); ++itr) {
    vlist.push_back(itr->first);
  }
  for (std::unordered_map<CacheableKeyPtr, InterestResultPolicy>::iterator itr =
           nthis->m_interestList.begin();
       itr != nthis->m_interestList.end(); ++itr) {
    vlist.push_back(itr->first);
  }
}
void ThinClientRegion::getInterestListRegex(
    VectorOfCacheableString& vregex) const {
  ThinClientRegion* nthis = const_cast<ThinClientRegion*>(this);
  RegionGlobalLocks acquireLocksRedundancy(nthis, false);
  RegionGlobalLocks acquireLocksFailover(nthis);
  CHECK_DESTROY_PENDING(TryReadGuard, getInterestListRegex);
  ACE_Guard<ACE_Recursive_Thread_Mutex> keysGuard(nthis->m_keysLock);
  for (std::unordered_map<std::string, InterestResultPolicy>::iterator itr =
           nthis->m_durableInterestListRegex.begin();
       itr != nthis->m_durableInterestListRegex.end(); ++itr) {
    vregex.push_back(CacheableString::create((*itr).first.c_str()));
  }
  for (std::unordered_map<std::string, InterestResultPolicy>::iterator itr =
           nthis->m_interestListRegex.begin();
       itr != nthis->m_interestListRegex.end(); ++itr) {
    vregex.push_back(CacheableString::create((*itr).first.c_str()));
  }
}

GfErrType ThinClientRegion::clientNotificationHandler(TcrMessage& msg) {
  GfErrType err = GF_NOERR;
  CacheablePtr oldValue;
  switch (msg.getMessageType()) {
    case TcrMessage::LOCAL_INVALIDATE: {
      LocalRegion::invalidateNoThrow(
          msg.getKey(), msg.getCallbackArgument(), -1,
          CacheEventFlags::NOTIFICATION | CacheEventFlags::LOCAL,
          msg.getVersionTag());
      break;
    }
    case TcrMessage::LOCAL_DESTROY: {
      err = LocalRegion::destroyNoThrow(
          msg.getKey(), msg.getCallbackArgument(), -1,
          CacheEventFlags::NOTIFICATION | CacheEventFlags::LOCAL,
          msg.getVersionTag());
      break;
    }
    case TcrMessage::CLEAR_REGION: {
      LOGDEBUG("remote clear region event for reigon[%s]",
               msg.getRegionName().c_str());
      err = localClearNoThrow(
          NULLPTR, CacheEventFlags::NOTIFICATION | CacheEventFlags::LOCAL);
      break;
    }
    case TcrMessage::LOCAL_DESTROY_REGION: {
      m_notifyRelease = true;
      preserveSB();
      err = LocalRegion::destroyRegionNoThrow(
          msg.getCallbackArgument(), true,
          CacheEventFlags::NOTIFICATION | CacheEventFlags::LOCAL);
      break;
    }
    case TcrMessage::LOCAL_CREATE:
      err = LocalRegion::putNoThrow(
          msg.getKey(), msg.getValue(), msg.getCallbackArgument(), oldValue, -1,
          CacheEventFlags::NOTIFICATION | CacheEventFlags::LOCAL,
          msg.getVersionTag());
      break;
    case TcrMessage::LOCAL_UPDATE: {
      //  for update set the NOTIFICATION_UPDATE to trigger the
      // afterUpdate event even if the key is not present in local cache
      err = LocalRegion::putNoThrow(
          msg.getKey(), msg.getValue(), msg.getCallbackArgument(), oldValue, -1,
          CacheEventFlags::NOTIFICATION | CacheEventFlags::NOTIFICATION_UPDATE |
              CacheEventFlags::LOCAL,
          msg.getVersionTag(), msg.getDelta(), msg.getEventId());
      break;
    }
    case TcrMessage::TOMBSTONE_OPERATION:
      LocalRegion::tombstoneOperationNoThrow(msg.getTombstoneVersions(),
                                             msg.getTombstoneKeys());
      break;
    default: {
      if (TcrMessage::getAllEPDisMess() == &msg) {
        setProcessedMarker(false);
        LocalRegion::invokeAfterAllEndPointDisconnected();
      } else {
        LOGERROR(
            "Unknown message type %d in subscription event handler; possible "
            "serialization mismatch",
            msg.getMessageType());
        err = GF_MSG;
      }
      break;
    }
  }

  // Update EventIdMap to mark event processed, Only for durable client.
  // In case of closing, don't send it as listener might not be invoked.
  if (!m_destroyPending && (m_isDurableClnt || msg.hasDelta()) &&
      TcrMessage::getAllEPDisMess() != &msg) {
    m_tcrdm->checkDupAndAdd(msg.getEventId());
  }

  return err;
}

GfErrType ThinClientRegion::handleServerException(const char* func,
                                                  const char* exceptionMsg) {
  // LOGERROR("%s: An exception (%s) happened at remote server.", func,
  //			exceptionMsg);
  GfErrType error = GF_NOERR;
  setTSSExceptionMessage(exceptionMsg);
  if (strstr(exceptionMsg,
             "org.apache.geode.security.NotAuthorizedException") != NULL) {
    error = GF_NOT_AUTHORIZED_EXCEPTION;
  } else if (strstr(exceptionMsg,
                    "org.apache.geode.cache.CacheWriterException") != NULL) {
    error = GF_CACHE_WRITER_EXCEPTION;
  } else if (strstr(
                 exceptionMsg,
                 "org.apache.geode.security.AuthenticationFailedException") !=
             NULL) {
    error = GF_AUTHENTICATION_FAILED_EXCEPTION;
  } else if (strstr(exceptionMsg,
                    "org.apache.geode.internal.cache.execute."
                    "InternalFunctionInvocationTargetException") != NULL) {
    error = GF_FUNCTION_EXCEPTION;
  } else if (strstr(exceptionMsg,
                    "org.apache.geode.cache.CommitConflictException") != NULL) {
    error = GF_COMMIT_CONFLICT_EXCEPTION;
  } else if (strstr(exceptionMsg,
                    "org.apache.geode.cache."
                    "TransactionDataNodeHasDepartedException") != NULL) {
    error = GF_TRANSACTION_DATA_NODE_HAS_DEPARTED_EXCEPTION;
  } else if (strstr(
                 exceptionMsg,
                 "org.apache.geode.cache.TransactionDataRebalancedException") !=
             NULL) {
    error = GF_TRANSACTION_DATA_REBALANCED_EXCEPTION;
  } else if (strstr(
                 exceptionMsg,
                 "org.apache.geode.security.AuthenticationRequiredException") !=
             NULL) {
    error = GF_AUTHENTICATION_REQUIRED_EXCEPTION;
  } else {
    error = GF_CACHESERVER_EXCEPTION;
  }

  if (error != GF_AUTHENTICATION_REQUIRED_EXCEPTION) {
    LOGERROR("%s: An exception (%s) happened at remote server.", func,
             exceptionMsg);
  } else {
    LOGFINER("%s: An exception (%s) happened at remote server.", func,
             exceptionMsg);
  }
  return error;
}

void ThinClientRegion::receiveNotification(TcrMessage* msg) {
  {
    TryReadGuard guard(m_rwLock, m_destroyPending);
    if (m_destroyPending) {
      if (msg != TcrMessage::getAllEPDisMess()) {
        GF_SAFE_DELETE(msg);
      }
      return;
    }
    m_notificationSema.acquire();
  }

  if (msg->getMessageType() == TcrMessage::CLIENT_MARKER) {
    handleMarker();
  } else {
    clientNotificationHandler(*msg);
  }

  m_notificationSema.release();
  if (TcrMessage::getAllEPDisMess() != msg) GF_SAFE_DELETE(msg);
}

void ThinClientRegion::localInvalidateRegion_internal() {
  MapEntryImplPtr me;
  CacheablePtr oldValue;

  VectorOfCacheableKey keysVec;
  keys_internal(keysVec);
  for (VectorOfCacheableKey::Iterator iter = keysVec.begin();
       iter != keysVec.end(); ++iter) {
    VersionTagPtr versionTag;
    m_entries->invalidate(*iter, me, oldValue, versionTag);
  }
}

void ThinClientRegion::invalidateInterestList(
    std::unordered_map<CacheableKeyPtr, InterestResultPolicy>& interestList) {
  MapEntryImplPtr me;
  CacheablePtr oldValue;

  if (!m_regionAttributes->getCachingEnabled()) {
    return;
  }
  for (std::unordered_map<CacheableKeyPtr, InterestResultPolicy>::iterator
           iter = interestList.begin();
       iter != interestList.end(); ++iter) {
    VersionTagPtr versionTag;
    m_entries->invalidate(iter->first, me, oldValue, versionTag);
  }
}

void ThinClientRegion::localInvalidateFailover() {
  CHECK_DESTROY_PENDING(TryReadGuard,
                        ThinClientRegion::localInvalidateFailover);

  //  No need to invalidate from the "m_xxxForUpdatesAsInvalidates" lists?
  if (m_interestListRegex.empty() && m_durableInterestListRegex.empty()) {
    invalidateInterestList(m_interestList);
    invalidateInterestList(m_durableInterestList);
  } else {
    localInvalidateRegion_internal();
  }
}

void ThinClientRegion::localInvalidateForRegisterInterest(
    const VectorOfCacheableKey& keys) {
  CHECK_DESTROY_PENDING(TryReadGuard,
                        ThinClientRegion::localInvalidateForRegisterInterest);

  if (!m_regionAttributes->getCachingEnabled()) {
    return;
  }

  CacheablePtr oldValue;
  MapEntryImplPtr me;

  for (VectorOfCacheableKey::Iterator iter = keys.begin(); iter != keys.end();
       ++iter) {
    VersionTagPtr versionTag;
    m_entries->invalidate(*iter, me, oldValue, versionTag);
    // KN: New
    updateAccessAndModifiedTimeForEntry(me, true);
  }
}

InterestResultPolicy ThinClientRegion::copyInterestList(
    VectorOfCacheableKey& keysVector,
    std::unordered_map<CacheableKeyPtr, InterestResultPolicy>& interestList)
    const {
  InterestResultPolicy interestPolicy = InterestResultPolicy::NONE;
  for (std::unordered_map<CacheableKeyPtr, InterestResultPolicy>::const_iterator
           iter = interestList.begin();
       iter != interestList.end(); ++iter) {
    keysVector.push_back(iter->first);
    interestPolicy = iter->second;
  }
  return interestPolicy;
}

void ThinClientRegion::registerInterestGetValues(
    const char* method, const VectorOfCacheableKey* keys,
    const VectorOfCacheableKeyPtr& resultKeys) {
  try {
    HashMapOfExceptionPtr exceptions(new HashMapOfException());
    GfErrType err = getAllNoThrow_remote(keys, NULLPTR, exceptions, resultKeys,
                                         true, NULLPTR);
    GfErrTypeToException(method, err);
    // log any exceptions here
    for (HashMapOfException::Iterator iter = exceptions->begin();
         iter != exceptions->end(); ++iter) {
      LOGWARN("%s Exception for key %s:: %s: %s", method,
              Utils::getCacheableKeyString(iter.first())->asChar(),
              iter.second()->getName(), iter.second()->getMessage());
    }
  } catch (const Exception& ex) {
    LOGWARN("%s Exception while getting values: %s: %s", method, ex.getName(),
            ex.getMessage());
    std::string msg(method);
    msg += " failed in getting values";
    throw EntryNotFoundException(msg.c_str(), NULL, false,
                                 ExceptionPtr(ex.clone()));
  }
}

void ThinClientRegion::destroyDM(bool keepEndpoints) {
  if (m_tcrdm != NULL) {
    m_tcrdm->destroy(keepEndpoints);
  }
}

void ThinClientRegion::release(bool invokeCallbacks) {
  if (m_released) {
    return;
  }
  if (!m_notifyRelease) {
    m_notificationSema.acquire();
  }

  destroyDM(invokeCallbacks);

  m_interestList.clear();
  m_interestListRegex.clear();
  m_durableInterestList.clear();
  m_durableInterestListRegex.clear();

  m_interestListForUpdatesAsInvalidates.clear();
  m_interestListRegexForUpdatesAsInvalidates.clear();
  m_durableInterestListForUpdatesAsInvalidates.clear();
  m_durableInterestListRegexForUpdatesAsInvalidates.clear();

  LocalRegion::release(invokeCallbacks);
}

ThinClientRegion::~ThinClientRegion() {
  TryWriteGuard guard(m_rwLock, m_destroyPending);
  if (!m_destroyPending) {
    release(false);
  }
  GF_SAFE_DELETE(m_tcrdm);
}

void ThinClientRegion::acquireGlobals(bool isFailover) {
  if (isFailover) {
    m_tcrdm->acquireFailoverLock();
  }
}

void ThinClientRegion::releaseGlobals(bool isFailover) {
  if (isFailover) {
    m_tcrdm->releaseFailoverLock();
  }
}

// CacheableVectorPtr ThinClientRegion::executeFunction(const char* func,
//		const CacheablePtr& args, CacheableVectorPtr routingObj,
//		uint8_t getResult, ResultCollectorPtr rc, int32_t retryAttempts,
//		uint32_t timeout) {
//	int32_t attempt = 0;
//	CacheableHashSetPtr failedNodes = NULLPTR;
//
//	//CacheableStringArrayPtr csArray = poolDM->getServers();
//
//	//if (csArray != NULLPTR && csArray->length() != 0) {
//	//	for (int i = 0; i < csArray->length(); i++) {
//	//		CacheableStringPtr cs = csArray[i];
//	//		TcrEndpoint *ep = NULL;
// //     /*
//	//		std::string endpointStr =
// Utils::convertHostToCanonicalForm(
//	//				cs->asChar());
// //         */
//	//		ep = poolDM->addEP(cs->asChar());
//	//	}
//	//}
//
//	//if pools retry attempts are not set then retry once on all available
// endpoints
//	if (retryAttempts == -1) {
//		retryAttempts = (int32_t) m_tcrdm->getNumberOfEndPoints();
//	}
//
//	while (attempt <= retryAttempts) {
//		std::string funcName(func);
//		TcrMessage msg(TcrMessage::EXECUTE_REGION_FUNCTION, funcName,
//				m_fullPath, args, routingObj, getResult,
// failedNodes,
// m_tcrdm);
//		TcrMessage reply(true, m_tcrdm);
//		ChunkedFunctionExecutionResponsePtr
//				resultCollector(new
// ChunkedFunctionExecutionResponse(reply,
//						(getResult & 2), rc));
//		reply.setChunkedResultHandler(resultCollector);
//		reply.setTimeout(timeout);
//
//		GfErrType err = GF_NOERR;
//		err = m_tcrdm->sendSyncRequest(msg, reply, !(getResult & 1));
//		if (err == GF_NOERR && (reply.getMessageType() ==
// TcrMessage::EXCEPTION
//				|| reply.getMessageType()
//						==
// TcrMessage::EXECUTE_REGION_FUNCTION_ERROR)) {
//			err = ThinClientRegion::handleServerException("Execute",
//					reply.getException());
//		}
//
//		if (ThinClientBaseDM::isFatalClientError(err)) {
//			GfErrTypeToException("ExecuteOnRegion:", err);
//		} else if (err != GF_NOERR) {
//			if (getResult & 1) {
//				resultCollector->reset();
//				rc->clearResults();
//				failedNodes = reply.getFailedNode();
//				attempt++;
//				continue;
//			} else {
//				GfErrTypeToException("ExecuteOnRegion:", err);
//			}
//		}
//		CacheableVectorPtr values =
//				resultCollector->getFunctionExecutionResults();
//		return values;
//	}
//	return NULLPTR;
//}

void ThinClientRegion::executeFunction(const char* func,
                                       const CacheablePtr& args,
                                       CacheableVectorPtr routingObj,
                                       uint8_t getResult, ResultCollectorPtr rc,
                                       int32_t retryAttempts,
                                       uint32_t timeout) {
  int32_t attempt = 0;
  CacheableHashSetPtr failedNodes = CacheableHashSet::create();
  // if pools retry attempts are not set then retry once on all available
  // endpoints
  if (retryAttempts == -1) {
    retryAttempts = static_cast<int32_t>(m_tcrdm->getNumberOfEndPoints());
  }

  bool reExecute = false;
  bool reExecuteForServ = false;

  do {
    std::string funcName(func);
    TcrMessage* msg;
    if (reExecuteForServ) {
      msg = new TcrMessageExecuteRegionFunction(
          funcName, this, args, routingObj, getResult, failedNodes, timeout,
          m_tcrdm, static_cast<int8_t>(1));
    } else {
      msg = new TcrMessageExecuteRegionFunction(
          funcName, this, args, routingObj, getResult, failedNodes, timeout,
          m_tcrdm, static_cast<int8_t>(0));
    }
    TcrMessageReply reply(true, m_tcrdm);
    // need to check
    ChunkedFunctionExecutionResponse* resultCollector(
        new ChunkedFunctionExecutionResponse(reply, (getResult & 2), rc));
    reply.setChunkedResultHandler(resultCollector);
    reply.setTimeout(timeout);
    GfErrType err = GF_NOERR;
    err = m_tcrdm->sendSyncRequest(*msg, reply, !(getResult & 1));
    resultCollector->reset();
    delete msg;
    delete resultCollector;
    if (err == GF_NOERR &&
        (reply.getMessageType() == TcrMessage::EXCEPTION ||
         reply.getMessageType() == TcrMessage::EXECUTE_REGION_FUNCTION_ERROR)) {
      err = ThinClientRegion::handleServerException("Execute",
                                                    reply.getException());
    }

    if (ThinClientBaseDM::isFatalClientError(err)) {
      GfErrTypeToException("ExecuteOnRegion:", err);
    } else if (err != GF_NOERR) {
      if (err == GF_FUNCTION_EXCEPTION) {
        reExecute = true;
        rc->clearResults();
        CacheableHashSetPtr failedNodesIds(reply.getFailedNode());
        failedNodes->clear();
        if (failedNodesIds != NULLPTR && failedNodesIds->size() > 0) {
          LOGDEBUG(
              "ThinClientRegion::executeFunction with GF_FUNCTION_EXCEPTION "
              "failedNodesIds size = %d ",
              failedNodesIds->size());
          for (CacheableHashSet::Iterator itr = failedNodesIds->begin();
               itr != failedNodesIds->end(); ++itr) {
            failedNodes->insert(*itr);
          }
        }
      } else if (err == GF_NOTCON) {
        attempt++;
        LOGDEBUG(
            "ThinClientRegion::executeFunction with GF_NOTCON retry attempt = "
            "%d ",
            attempt);
        if (attempt > retryAttempts) {
          GfErrTypeToException("ExecuteOnRegion:", err);
        }
        reExecuteForServ = true;
        rc->clearResults();
        failedNodes->clear();
      } else if (err == GF_TIMOUT) {
        LOGINFO(
            "function timeout. Name: %s, timeout: %d, params: %d, "
            "retryAttempts: %d ",
            funcName.c_str(), timeout, getResult, retryAttempts);
        GfErrTypeToException("ExecuteOnRegion", GF_TIMOUT);
      } else if (err == GF_CLIENT_WAIT_TIMEOUT ||
                 err == GF_CLIENT_WAIT_TIMEOUT_REFRESH_PRMETADATA) {
        LOGINFO(
            "function timeout, possibly bucket is not available or bucket "
            "blacklisted. Name: %s, timeout: %d, params: %d, retryAttempts: "
            "%d ",
            funcName.c_str(), timeout, getResult, retryAttempts);
        GfErrTypeToException("ExecuteOnRegion", GF_CLIENT_WAIT_TIMEOUT);
      } else {
        LOGDEBUG("executeFunction err = %d ", err);
        GfErrTypeToException("ExecuteOnRegion:", err);
      }
    } else {
      reExecute = false;
      reExecuteForServ = false;
    }
  } while (reExecuteForServ);

  if (reExecute && (getResult & 1)) {
    reExecuteFunction(func, args, routingObj, getResult, rc, retryAttempts,
                      failedNodes, timeout);
  }
}

CacheableVectorPtr ThinClientRegion::reExecuteFunction(
    const char* func, const CacheablePtr& args, CacheableVectorPtr routingObj,
    uint8_t getResult, ResultCollectorPtr rc, int32_t retryAttempts,
    CacheableHashSetPtr& failedNodes, uint32_t timeout) {
  int32_t attempt = 0;
  bool reExecute = true;
  // if pools retry attempts are not set then retry once on all available
  // endpoints
  if (retryAttempts == -1) {
    retryAttempts = static_cast<int32_t>(m_tcrdm->getNumberOfEndPoints());
  }

  do {
    reExecute = false;
    std::string funcName(func);
    TcrMessageExecuteRegionFunction msg(
        funcName, this, args, routingObj, getResult, failedNodes, timeout,
        m_tcrdm, /*reExecute*/ static_cast<int8_t>(1));
    TcrMessageReply reply(true, m_tcrdm);
    // need to check
    ChunkedFunctionExecutionResponse* resultCollector(
        new ChunkedFunctionExecutionResponse(reply, (getResult & 2), rc));
    reply.setChunkedResultHandler(resultCollector);
    reply.setTimeout(timeout);

    GfErrType err = GF_NOERR;
    err = m_tcrdm->sendSyncRequest(msg, reply, !(getResult & 1));
    delete resultCollector;
    if (err == GF_NOERR &&
        (reply.getMessageType() == TcrMessage::EXCEPTION ||
         reply.getMessageType() == TcrMessage::EXECUTE_REGION_FUNCTION_ERROR)) {
      err = ThinClientRegion::handleServerException("Execute",
                                                    reply.getException());
    }

    if (ThinClientBaseDM::isFatalClientError(err)) {
      GfErrTypeToException("ExecuteOnRegion:", err);
    } else if (err != GF_NOERR) {
      if (err == GF_FUNCTION_EXCEPTION) {
        reExecute = true;
        rc->clearResults();
        CacheableHashSetPtr failedNodesIds(reply.getFailedNode());
        failedNodes->clear();
        if (failedNodesIds != NULLPTR && failedNodesIds->size() > 0) {
          LOGDEBUG(
              "ThinClientRegion::reExecuteFunction with GF_FUNCTION_EXCEPTION "
              "failedNodesIds size = %d ",
              failedNodesIds->size());
          for (CacheableHashSet::Iterator itr = failedNodesIds->begin();
               itr != failedNodesIds->end(); ++itr) {
            failedNodes->insert(*itr);
          }
        }
      } else if (err == GF_NOTCON) {
        attempt++;
        LOGDEBUG(
            "ThinClientRegion::reExecuteFunction with GF_NOTCON retry attempt "
            "= %d ",
            attempt);
        if (attempt > retryAttempts) {
          GfErrTypeToException("ExecuteOnRegion:", err);
        }
        reExecute = true;
        rc->clearResults();
        failedNodes->clear();
      } else if (err == GF_TIMOUT) {
        LOGINFO("function timeout");
        GfErrTypeToException("ExecuteOnRegion", GF_CACHE_TIMEOUT_EXCEPTION);
      } else {
        LOGDEBUG("reExecuteFunction err = %d ", err);
        GfErrTypeToException("ExecuteOnRegion:", err);
      }
    }
  } while (reExecute);
  return NULLPTR;
}

bool ThinClientRegion::executeFunctionSH(
    const char* func, const CacheablePtr& args, uint8_t getResult,
    ResultCollectorPtr rc,
    HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>* locationMap,
    CacheableHashSetPtr& failedNodes, uint32_t timeout, bool allBuckets) {
  bool reExecute = false;
  ACE_Recursive_Thread_Mutex resultCollectorLock;
  UserAttributesPtr userAttr =
      TSSUserAttributesWrapper::s_gemfireTSSUserAttributes->getUserAttributes();
  std::vector<OnRegionFunctionExecution*> feWorkers;
  ThreadPool* threadPool = TPSingleton::instance();

  for (HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>::Iterator
           locationIter = locationMap->begin();
       locationIter != locationMap->end(); ++locationIter) {
    BucketServerLocationPtr serverLocation = locationIter.first();
    CacheableHashSetPtr buckets = locationIter.second();
    OnRegionFunctionExecution* worker = new OnRegionFunctionExecution(
        func, this, args, buckets, getResult, timeout,
        dynamic_cast<ThinClientPoolDM*>(m_tcrdm), &resultCollectorLock, rc,
        userAttr, false, serverLocation, allBuckets);
    threadPool->perform(worker);
    feWorkers.push_back(worker);
  }

  for (std::vector<OnRegionFunctionExecution*>::iterator iter =
           feWorkers.begin();
       iter != feWorkers.end(); ++iter) {
    OnRegionFunctionExecution* worker = *iter;
    GfErrType err = worker->getResult();
    TcrMessage* currentReply = worker->getReply();

    if (err == GF_NOERR &&
        (currentReply->getMessageType() == TcrMessage::EXCEPTION ||
         currentReply->getMessageType() ==
             TcrMessage::EXECUTE_REGION_FUNCTION_ERROR)) {
      err = ThinClientRegion::handleServerException(
          "Execute", currentReply->getException());
    }

    if (ThinClientBaseDM::isFatalClientError(err)) {
      delete worker;
      GfErrTypeToException("ExecuteOnRegion:", err);
    } else if (err != GF_NOERR) {
      if (err == GF_FUNCTION_EXCEPTION) {
        reExecute = true;
        ThinClientPoolDM* poolDM = dynamic_cast<ThinClientPoolDM*>(m_tcrdm);
        if ((poolDM != NULL) && (poolDM->getClientMetaDataService() != NULL)) {
          poolDM->getClientMetaDataService()->enqueueForMetadataRefresh(
              this->getFullPath(), 0);
        }
        worker->getResultCollector()->reset();
        {
          ACE_Guard<ACE_Recursive_Thread_Mutex> guard(resultCollectorLock);
          rc->clearResults();
        }
        CacheableHashSetPtr failedNodeIds(currentReply->getFailedNode());
        if (failedNodeIds != NULLPTR && failedNodeIds->size() > 0) {
          LOGDEBUG(
              "ThinClientRegion::executeFunctionSH with GF_FUNCTION_EXCEPTION "
              "failedNodeIds size = %d ",
              failedNodeIds->size());
          for (CacheableHashSet::Iterator itr = failedNodeIds->begin();
               itr != failedNodeIds->end(); ++itr) {
            failedNodes->insert(*itr);
          }
        }
      } else if (err == GF_NOTCON) {
        reExecute = true;
        LOGDEBUG("ThinClientRegion::executeFunctionSH with GF_NOTCON ");
        ThinClientPoolDM* poolDM = dynamic_cast<ThinClientPoolDM*>(m_tcrdm);
        if ((poolDM != NULL) && (poolDM->getClientMetaDataService() != NULL)) {
          poolDM->getClientMetaDataService()->enqueueForMetadataRefresh(
              this->getFullPath(), 0);
        }
        worker->getResultCollector()->reset();
        {
          ACE_Guard<ACE_Recursive_Thread_Mutex> guard(resultCollectorLock);
          rc->clearResults();
        }
      } else {
        delete worker;
        LOGDEBUG("executeFunctionSH err = %d ", err);
        GfErrTypeToException("ExecuteOnRegion:", err);
      }
    }
    delete worker;
  }
  return reExecute;
}

GfErrType ThinClientRegion::getFuncAttributes(const char* func,
                                              std::vector<int8>** attr) {
  GfErrType err = GF_NOERR;

  // do TCR GET_FUNCTION_ATTRIBUTES
  LOGDEBUG("Tcrmessage request GET_FUNCTION_ATTRIBUTES ");
  std::string funcName(func);
  TcrMessageGetFunctionAttributes request(funcName, m_tcrdm);
  TcrMessageReply reply(true, m_tcrdm);
  err = m_tcrdm->sendSyncRequest(request, reply);
  if (err != GF_NOERR) {
    return err;
  }
  switch (reply.getMessageType()) {
    case TcrMessage::RESPONSE: {
      *attr = reply.getFunctionAttributes();
      break;
    }
    case TcrMessage::EXCEPTION: {
      err = handleServerException("Region::GET_FUNCTION_ATTRIBUTES",
                                  reply.getException());
      break;
    }
    default: {
      LOGERROR("Unknown message type %d while getting function attributes.",
               reply.getMessageType());
      err = GF_MSG;
    }
  }
  return err;
}

GfErrType ThinClientRegion::getNoThrow_FullObject(EventIdPtr eventId,
                                                  CacheablePtr& fullObject,
                                                  VersionTagPtr& versionTag) {
  TcrMessageRequestEventValue fullObjectMsg(eventId);
  TcrMessageReply reply(true, NULL);

  GfErrType err = GF_NOTCON;
  err = m_tcrdm->sendSyncRequest(fullObjectMsg, reply, false, true);
  if (err == GF_NOERR) {
    fullObject = reply.getValue();
  }
  versionTag = reply.getVersionTag();
  return err;
}

void ThinClientRegion::txDestroy(const CacheableKeyPtr& key,
                                 const UserDataPtr& aCallbackArgument,
                                 VersionTagPtr versionTag) {
  GfErrType err = destroyNoThrowTX(key, aCallbackArgument, -1,
                                   CacheEventFlags::NORMAL, versionTag);
  GfErrTypeToException("Region::destroyTX", err);
}

void ThinClientRegion::txInvalidate(const CacheableKeyPtr& key,
                                    const UserDataPtr& aCallbackArgument,
                                    VersionTagPtr versionTag) {
  GfErrType err = invalidateNoThrowTX(key, aCallbackArgument, -1,
                                      CacheEventFlags::NORMAL, versionTag);
  GfErrTypeToException("Region::invalidateTX", err);
}

void ThinClientRegion::txPut(const CacheableKeyPtr& key,
                             const CacheablePtr& value,
                             const UserDataPtr& aCallbackArgument,
                             VersionTagPtr versionTag) {
  CacheablePtr oldValue;
  int64 sampleStartNanos = Utils::startStatOpTime();
  GfErrType err = putNoThrowTX(key, value, aCallbackArgument, oldValue, -1,
                               CacheEventFlags::NORMAL, versionTag);
  Utils::updateStatOpTime(m_regionStats->getStat(),
                          RegionStatType::getInstance()->getPutTimeId(),
                          sampleStartNanos);
  GfErrTypeToException("Region::putTX", err);
}

void ChunkedInterestResponse::reset() {
  if (m_resultKeys != NULLPTR && m_resultKeys->size() > 0) {
    m_resultKeys->clear();
  }
}

void ChunkedInterestResponse::handleChunk(const uint8_t* chunk,
                                          int32_t chunkLen,
                                          uint8_t isLastChunkWithSecurity) {
  DataInput input(chunk, chunkLen);

  input.setPoolName(m_replyMsg.getPoolName());

  uint32_t partLen;
  if (TcrMessageHelper::readChunkPartHeader(
          m_msg, input, 0, GemfireTypeIds::CacheableArrayList,
          "ChunkedInterestResponse", partLen,
          isLastChunkWithSecurity) != TcrMessageHelper::OBJECT) {
    // encountered an exception part, so return without reading more
    m_replyMsg.readSecureObjectPart(input, false, true,
                                    isLastChunkWithSecurity);
    return;
  }

  if (m_resultKeys == NULLPTR) {
    GF_NEW(m_resultKeys, VectorOfCacheableKey);
  }
  serializer::readObject(input, *m_resultKeys);
  m_replyMsg.readSecureObjectPart(input, false, true, isLastChunkWithSecurity);
}

void ChunkedKeySetResponse::reset() {
  if (m_resultKeys.size() > 0) {
    m_resultKeys.clear();
  }
}

void ChunkedKeySetResponse::handleChunk(const uint8_t* chunk, int32_t chunkLen,
                                        uint8_t isLastChunkWithSecurity) {
  DataInput input(chunk, chunkLen);

  input.setPoolName(m_replyMsg.getPoolName());

  uint32_t partLen;
  if (TcrMessageHelper::readChunkPartHeader(
          m_msg, input, 0, GemfireTypeIds::CacheableArrayList,
          "ChunkedKeySetResponse", partLen,
          isLastChunkWithSecurity) != TcrMessageHelper::OBJECT) {
    // encountered an exception part, so return without reading more
    m_replyMsg.readSecureObjectPart(input, false, true,
                                    isLastChunkWithSecurity);
    return;
  }

  serializer::readObject(input, m_resultKeys);
  m_replyMsg.readSecureObjectPart(input, false, true, isLastChunkWithSecurity);
}

void ChunkedQueryResponse::reset() {
  m_queryResults->clear();
  m_structFieldNames.clear();
}

void ChunkedQueryResponse::readObjectPartList(DataInput& input,
                                              bool isResultSet) {
  bool hasKeys;
  input.readBoolean(&hasKeys);

  if (hasKeys) {
    LOGERROR("Query response has keys which is unexpected.");
    throw IllegalStateException("Query response has keys which is unexpected.");
  }

  int32_t len;
  input.readInt(&len);

  for (int32_t index = 0; index < len; ++index) {
    uint8_t byte = 0;
    input.read(&byte);

    if (byte == 2 /* for exception*/) {
      int32_t skipLen;
      input.readArrayLen(&skipLen);
      input.advanceCursor(skipLen);
      CacheableStringPtr exMsgPtr;
      input.readNativeString(exMsgPtr);
      throw IllegalStateException(exMsgPtr->asChar());
    } else {
      if (isResultSet) {
        CacheablePtr value;
        input.readObject(value);
        m_queryResults->push_back(value);
      } else {
        int8_t arrayType;
        input.read(&arrayType);
        if (arrayType == GemfireTypeIdsImpl::FixedIDByte) {
          input.read(&arrayType);
          if (arrayType != GemfireTypeIdsImpl::CacheableObjectPartList) {
            LOGERROR(
                "Query response got unhandled message format %d while "
                "expecting struct set object part list; possible serialization "
                "mismatch",
                arrayType);
            throw MessageException(
                "Query response got unhandled message format while expecting "
                "struct set object part list; possible serialization mismatch");
          }
          readObjectPartList(input, true);
        } else {
          LOGERROR(
              "Query response got unhandled message format %d while expecting "
              "struct set object part list; possible serialization mismatch",
              arrayType);
          throw MessageException(
              "Query response got unhandled message format while expecting "
              "struct set object part list; possible serialization mismatch");
        }
      }
    }
  }
}

void ChunkedQueryResponse::handleChunk(const uint8_t* chunk, int32_t chunkLen,
                                       uint8_t isLastChunkWithSecurity) {
  LOGDEBUG("ChunkedQueryResponse::handleChunk..");
  DataInput input(chunk, chunkLen);
  input.setPoolName(m_msg.getPoolName());
  uint32_t partLen;
  int8_t isObj;
  TcrMessageHelper::ChunkObjectType objType;
  if ((objType = TcrMessageHelper::readChunkPartHeader(
           m_msg, input, GemfireTypeIdsImpl::FixedIDByte,
           static_cast<uint8_t>(GemfireTypeIdsImpl::CollectionTypeImpl),
           "ChunkedQueryResponse", partLen, isLastChunkWithSecurity)) ==
      TcrMessageHelper::EXCEPTION) {
    // encountered an exception part, so return without reading more
    m_msg.readSecureObjectPart(input, false, true, isLastChunkWithSecurity);
    return;
  } else if (objType == TcrMessageHelper::NULL_OBJECT) {
    // special case for scalar result
    input.readInt(&partLen);
    input.read(&isObj);
    CacheableInt32Ptr intVal;
    input.readObject(intVal, true);
    m_queryResults->push_back(intVal);

    // TODO:
    m_msg.readSecureObjectPart(input, false, true, isLastChunkWithSecurity);

    return;
  }

  uint8_t classByte;
  char* isStructTypeImpl = NULL;
  uint16_t stiLen = 0;
  // soubhik: ignoring parent classes for now
  // we will require to look at it once CQ is to be implemented.
  // skipping HashSet/StructSet
  // qhe: It was agreed upon that we'll use set for all kinds of results.
  // to avoid dealing with compare operator for user objets.
  // If the results on server are in a bag, or the user need to manipulate
  // the elements, then we have to revisit this issue.
  // For now, we'll live with duplicate records, hoping they do not cost much.
  skipClass(input);
  // skipping CollectionTypeImpl
  // skipClass(input); // no longer, since GFE 5.7

  int8_t structType;
  input.read(&structType);  // this is Fixed ID byte (1)
  input.read(&structType);  // this is DataSerializable (45)
  input.read(&classByte);
  uint8_t stringType;
  input.read(&stringType);  // ignore string header - assume 64k string
  input.readUTF(&isStructTypeImpl, &stiLen);

  DeleteArray<char> delSTI(isStructTypeImpl);
  if (strcmp(isStructTypeImpl, "org.apache.geode.cache.query.Struct") == 0) {
    int32_t numOfFldNames;
    input.readArrayLen(&numOfFldNames);
    bool skip = false;
    if (m_structFieldNames.size() != 0) {
      skip = true;
    }
    for (int i = 0; i < numOfFldNames; i++) {
      CacheableStringPtr sptr;
      // input.readObject(sptr);
      input.readNativeString(sptr);
      if (!skip) {
        m_structFieldNames.push_back(sptr);
      }
    }
  }

  // skip the remaining part
  input.reset();
  // skip the whole part including partLen and isObj (4+1)
  input.advanceCursor(partLen + 5);

  input.readInt(&partLen);
  input.read(&isObj);
  if (!isObj) {
    LOGERROR(
        "Query response part is not an object; possible serialization "
        "mismatch");
    throw MessageException(
        "Query response part is not an object; possible serialization "
        "mismatch");
  }

  bool isResultSet = (m_structFieldNames.size() == 0);

  int8_t arrayType;
  input.read(&arrayType);

  if (arrayType == GemfireTypeIds::CacheableObjectArray) {
    int32_t arraySize;
    input.readArrayLen(&arraySize);
    skipClass(input);
    for (int32_t arrayItem = 0; arrayItem < arraySize; ++arrayItem) {
      SerializablePtr value;
      if (isResultSet) {
        input.readObject(value);
        m_queryResults->push_back(value);
      } else {
        input.read(&isObj);
        int32_t arraySize2;
        input.readArrayLen(&arraySize2);
        skipClass(input);
        for (int32_t index = 0; index < arraySize2; ++index) {
          input.readObject(value);
          m_queryResults->push_back(value);
        }
      }
    }
  } else if (arrayType == GemfireTypeIdsImpl::FixedIDByte) {
    input.read(&arrayType);
    if (arrayType != GemfireTypeIdsImpl::CacheableObjectPartList) {
      LOGERROR(
          "Query response got unhandled message format %d while expecting "
          "object part list; possible serialization mismatch",
          arrayType);
      throw MessageException(
          "Query response got unhandled message format while expecting object "
          "part list; possible serialization mismatch");
    }
    readObjectPartList(input, isResultSet);
  } else {
    LOGERROR(
        "Query response got unhandled message format %d; possible "
        "serialization mismatch",
        arrayType);
    throw MessageException(
        "Query response got unhandled message format; possible serialization "
        "mismatch");
  }

  m_msg.readSecureObjectPart(input, false, true, isLastChunkWithSecurity);
}

void ChunkedQueryResponse::skipClass(DataInput& input) {
  uint8_t classByte;
  input.read(&classByte);
  if (classByte == GemfireTypeIdsImpl::Class) {
    uint8_t stringType;
    // ignore string type id - assuming its a normal (under 64k) string.
    input.read(&stringType);
    uint16_t classlen;
    input.readInt(&classlen);
    input.advanceCursor(classlen);
  } else {
    throw IllegalStateException(
        "ChunkedQueryResponse::skipClass: "
        "Did not get expected class header byte");
  }
}

void ChunkedFunctionExecutionResponse::reset() {
  // m_functionExecutionResults->clear();
}

void ChunkedFunctionExecutionResponse::handleChunk(
    const uint8_t* chunk, int32_t chunkLen, uint8_t isLastChunkWithSecurity) {
  LOGDEBUG("ChunkedFunctionExecutionResponse::handleChunk");
  DataInput input(chunk, chunkLen);
  input.setPoolName(m_msg.getPoolName());
  uint32_t partLen;

  int8_t arrayType;
  if ((arrayType = static_cast<TcrMessageHelper::ChunkObjectType>(
           TcrMessageHelper::readChunkPartHeader(
               m_msg, input, "ChunkedFunctionExecutionResponse", partLen,
               isLastChunkWithSecurity))) == TcrMessageHelper::EXCEPTION) {
    // encountered an exception part, so return without reading more
    m_msg.readSecureObjectPart(input, false, true, isLastChunkWithSecurity);
    return;
  }

  if (m_getResult == false) {
    return;
  }

  if (static_cast<TcrMessageHelper::ChunkObjectType>(arrayType) ==
      TcrMessageHelper::NULL_OBJECT) {
    LOGDEBUG("ChunkedFunctionExecutionResponse::handleChunk NULL object");
    //	m_functionExecutionResults->push_back(NULLPTR);
    m_msg.readSecureObjectPart(input, false, true, isLastChunkWithSecurity);
    return;
  }

  int32_t len;
  int startLen =
      input.getBytesRead() -
      1;  // from here need to look value part + memberid AND -1 for array type
  input.readArrayLen(&len);

  // read a byte to determine whether to read exception part for sendException
  // or read objects.
  uint8_t partType;
  input.read(&partType);
  bool isExceptionPart = false;
  // See If partType is JavaSerializable
  const int CHUNK_HDR_LEN = 5;
  const int SECURE_PART_LEN = 5 + 8;
  bool readPart = true;
  LOGDEBUG(
      "ChunkedFunctionExecutionResponse::handleChunk chunkLen = %d & partLen = "
      "%d ",
      chunkLen, partLen);
  if (partType == GemfireTypeIdsImpl::JavaSerializable) {
    isExceptionPart = true;
    // reset the input.
    input.reset();

    if (((isLastChunkWithSecurity & 0x02) &&
         (chunkLen - static_cast<int32_t>(partLen) <=
          CHUNK_HDR_LEN + SECURE_PART_LEN)) ||
        (((isLastChunkWithSecurity & 0x02) == 0) &&
         (chunkLen - static_cast<int32_t>(partLen) <= CHUNK_HDR_LEN))) {
      readPart = false;
      input.readInt(&partLen);
      input.advanceCursor(1);  // skip isObject byte
      input.advanceCursor(partLen);
    } else {
      // skip first part i.e JavaSerializable.
      TcrMessageHelper::skipParts(m_msg, input, 1);

      // read the second part which is string in usual manner, first its length.
      input.readInt(&partLen);

      int8_t isObject;
      // then isObject byte
      input.read(&isObject);

      startLen = input.getBytesRead();  // reset from here need to look value
                                        // part + memberid AND -1 for array type

      // Since it is contained as a part of other results, read arrayType which
      // is arrayList = 65.
      input.read(&arrayType);

      // then its len which is 2
      input.readArrayLen(&len);
    }
  } else {
    // rewind cursor by 1 to what we had read a byte to determine whether to
    // read exception part or read objects.
    input.rewindCursor(1);
  }

  // Read either object or exception string from sendException.
  SerializablePtr value;
  // CacheablePtr memberId;
  if (readPart) {
    input.readObject(value);
    // TODO: track this memberId for PrFxHa
    // input.readObject(memberId);
    int objectlen = input.getBytesRead() - startLen;

    int memberIdLen = partLen - objectlen;
    input.advanceCursor(memberIdLen);
    LOGDEBUG("function partlen = %d , objectlen = %d,  memberidlen = %d ",
             partLen, objectlen, memberIdLen);
    LOGDEBUG("function input.getBytesRemaining() = %d ",
             input.getBytesRemaining());
    // is there any way to assert it, as after that we need to read security
    // header
    /*if(input.getBytesRemaining() !=  0) {
      LOGERROR("Function response not read all bytes");
      throw IllegalStateException("Function Execution didn't read all bytes");
    }*/
  } else {
    value = CacheableString::create("Function exception result.");
  }
  if (m_rc != NULLPTR) {
    CacheablePtr result = NULLPTR;
    if (isExceptionPart) {
      UserFunctionExecutionExceptionPtr uFEPtr(
          new UserFunctionExecutionException(value->toString()));
      result = dynCast<CacheablePtr>(uFEPtr);
    } else {
      result = dynCast<CacheablePtr>(value);
    }
    if (m_resultCollectorLock != NULL) {
      ACE_Guard<ACE_Recursive_Thread_Mutex> guard(*m_resultCollectorLock);
      m_rc->addResult(result);
    } else {
      m_rc->addResult(result);
    }
  }

  m_msg.readSecureObjectPart(input, false, true, isLastChunkWithSecurity);
  //  m_functionExecutionResults->push_back(value);
}

void ChunkedGetAllResponse::reset() {
  m_keysOffset = 0;
  if (m_resultKeys != NULLPTR && m_resultKeys->size() > 0) {
    m_resultKeys->clear();
  }
}

// process a GET_ALL response chunk
void ChunkedGetAllResponse::handleChunk(const uint8_t* chunk, int32_t chunkLen,
                                        uint8_t isLastChunkWithSecurity) {
  DataInput input(chunk, chunkLen);
  input.setPoolName(m_msg.getPoolName());
  uint32_t partLen;
  if (TcrMessageHelper::readChunkPartHeader(
          m_msg, input, GemfireTypeIdsImpl::FixedIDByte,
          GemfireTypeIdsImpl::VersionedObjectPartList, "ChunkedGetAllResponse",
          partLen, isLastChunkWithSecurity) != TcrMessageHelper::OBJECT) {
    // encountered an exception part, so return without reading more
    m_msg.readSecureObjectPart(input, false, true, isLastChunkWithSecurity);
    return;
  }

  VersionedCacheableObjectPartList objectList(
      m_keys, &m_keysOffset, m_values, m_exceptions, m_resultKeys, m_region,
      &m_trackerMap, m_destroyTracker, m_addToLocalCache, m_dsmemId,
      m_responseLock);

  objectList.fromData(input);

  m_msg.readSecureObjectPart(input, false, true, isLastChunkWithSecurity);
}

void ChunkedGetAllResponse::add(const ChunkedGetAllResponse* other) {
  if (m_values != NULLPTR) {
    for (HashMapOfCacheable::Iterator iter = other->m_values->begin();
         iter != other->m_values->end(); iter++) {
      m_values->insert(iter.first(), iter.second());
    }
  }

  if (m_exceptions != NULLPTR) {
    for (HashMapOfException::Iterator iter = other->m_exceptions->begin();
         iter != other->m_exceptions->end(); iter++) {
      m_exceptions->insert(iter.first(), iter.second());
    }
  }

  for (MapOfUpdateCounters::iterator iter = other->m_trackerMap.begin();
       iter != other->m_trackerMap.end(); iter++) {
    m_trackerMap[iter->first] = iter->second;
  }

  if (m_resultKeys != NULLPTR) {
    for (VectorOfCacheableKey::Iterator iter = other->m_resultKeys->begin();
         iter != other->m_resultKeys->end(); iter++) {
      m_resultKeys->push_back(*iter);
    }
  }
}

void ChunkedPutAllResponse::reset() {
  if (m_list != NULLPTR && m_list->size() > 0) {
    m_list->getVersionedTagptr().clear();
  }
}

// process a PUT_ALL response chunk
void ChunkedPutAllResponse::handleChunk(const uint8_t* chunk, int32_t chunkLen,
                                        uint8_t isLastChunkWithSecurity) {
  DataInput input(chunk, chunkLen);
  input.setPoolName(m_msg.getPoolName());
  uint32_t partLen;
  int8_t chunkType;
  if ((chunkType = (TcrMessageHelper::ChunkObjectType)
           TcrMessageHelper::readChunkPartHeader(
               m_msg, input, GemfireTypeIdsImpl::FixedIDByte,
               GemfireTypeIdsImpl::VersionedObjectPartList,
               "ChunkedPutAllResponse", partLen, isLastChunkWithSecurity)) ==
      TcrMessageHelper::NULL_OBJECT) {
    LOGDEBUG("ChunkedPutAllResponse::handleChunk NULL object");
    // No issues it will be empty in case of disabled caching.
    m_msg.readSecureObjectPart(input, false, true, isLastChunkWithSecurity);
    return;
  }

  if (static_cast<TcrMessageHelper::ChunkObjectType>(chunkType) ==
      TcrMessageHelper::OBJECT) {
    LOGDEBUG("ChunkedPutAllResponse::handleChunk object");
    ACE_Recursive_Thread_Mutex responseLock;
    VersionedCacheableObjectPartListPtr vcObjPart(
        new VersionedCacheableObjectPartList(
            m_msg.getChunkedResultHandler()->getEndpointMemId(), responseLock));
    vcObjPart->fromData(input);
    m_list->addAll(vcObjPart);
    m_msg.readSecureObjectPart(input, false, true, isLastChunkWithSecurity);
  } else {
    LOGDEBUG("ChunkedPutAllResponse::handleChunk BYTES PART");
    int8_t byte0;
    input.read(&byte0);
    LOGDEBUG("ChunkedPutAllResponse::handleChunk single-hop bytes byte0 = %d ",
             byte0);
    int8_t byte1;
    input.read(&byte1);
    m_msg.readSecureObjectPart(input, false, true, isLastChunkWithSecurity);

    PoolPtr pool = PoolManager::find(m_msg.getPoolName());
    if (pool != NULLPTR && !pool->isDestroyed() &&
        pool->getPRSingleHopEnabled()) {
      ThinClientPoolDM* poolDM = dynamic_cast<ThinClientPoolDM*>(pool.ptr());
      if ((poolDM != NULL) && (poolDM->getClientMetaDataService() != NULL) &&
          (byte0 != 0)) {
        LOGFINE(
            "enqueued region %s for metadata refresh for singlehop for PUTALL "
            "operation.",
            m_region->getFullPath());
        poolDM->getClientMetaDataService()->enqueueForMetadataRefresh(
            m_region->getFullPath(), byte1);
      }
    }
  }
}

void ChunkedRemoveAllResponse::reset() {
  if (m_list != NULLPTR && m_list->size() > 0) {
    m_list->getVersionedTagptr().clear();
  }
}

// process a REMOVE_ALL response chunk
void ChunkedRemoveAllResponse::handleChunk(const uint8_t* chunk,
                                           int32_t chunkLen,
                                           uint8_t isLastChunkWithSecurity) {
  DataInput input(chunk, chunkLen);
  input.setPoolName(m_msg.getPoolName());
  uint32_t partLen;
  int8_t chunkType;
  if ((chunkType = (TcrMessageHelper::ChunkObjectType)
           TcrMessageHelper::readChunkPartHeader(
               m_msg, input, GemfireTypeIdsImpl::FixedIDByte,
               GemfireTypeIdsImpl::VersionedObjectPartList,
               "ChunkedRemoveAllResponse", partLen, isLastChunkWithSecurity)) ==
      TcrMessageHelper::NULL_OBJECT) {
    LOGDEBUG("ChunkedRemoveAllResponse::handleChunk NULL object");
    // No issues it will be empty in case of disabled caching.
    m_msg.readSecureObjectPart(input, false, true, isLastChunkWithSecurity);
    return;
  }

  if (static_cast<TcrMessageHelper::ChunkObjectType>(chunkType) ==
      TcrMessageHelper::OBJECT) {
    LOGDEBUG("ChunkedRemoveAllResponse::handleChunk object");
    ACE_Recursive_Thread_Mutex responseLock;
    VersionedCacheableObjectPartListPtr vcObjPart(
        new VersionedCacheableObjectPartList(
            m_msg.getChunkedResultHandler()->getEndpointMemId(), responseLock));
    vcObjPart->fromData(input);
    m_list->addAll(vcObjPart);
    m_msg.readSecureObjectPart(input, false, true, isLastChunkWithSecurity);
  } else {
    LOGDEBUG("ChunkedRemoveAllResponse::handleChunk BYTES PART");
    int8_t byte0;
    input.read(&byte0);
    LOGDEBUG(
        "ChunkedRemoveAllResponse::handleChunk single-hop bytes byte0 = %d ",
        byte0);
    int8_t byte1;
    input.read(&byte1);
    m_msg.readSecureObjectPart(input, false, true, isLastChunkWithSecurity);

    PoolPtr pool = PoolManager::find(m_msg.getPoolName());
    if (pool != NULLPTR && !pool->isDestroyed() &&
        pool->getPRSingleHopEnabled()) {
      ThinClientPoolDM* poolDM = dynamic_cast<ThinClientPoolDM*>(pool.ptr());
      if ((poolDM != NULL) && (poolDM->getClientMetaDataService() != NULL) &&
          (byte0 != 0)) {
        LOGFINE(
            "enqueued region %s for metadata refresh for singlehop for "
            "REMOVEALL operation.",
            m_region->getFullPath());
        poolDM->getClientMetaDataService()->enqueueForMetadataRefresh(
            m_region->getFullPath(), byte1);
      }
    }
  }
}

void ChunkedDurableCQListResponse::reset() {
  if (m_resultList != NULLPTR && m_resultList->length() > 0) {
    m_resultList->clear();
  }
}

// handles the chunk response for GETDURABLECQS_MSG_TYPE
void ChunkedDurableCQListResponse::handleChunk(
    const uint8_t* chunk, int32_t chunkLen, uint8_t isLastChunkWithSecurity) {
  DataInput input(chunk, chunkLen);
  input.setPoolName(m_msg.getPoolName());

  // read part length
  uint32_t partLen;
  input.readInt(&partLen);

  bool isObj;
  input.readBoolean(&isObj);

  if (!isObj) {
    // we're currently always expecting an object
    char exMsg[256];
    ACE_OS::snprintf(
        exMsg, 255,
        "ChunkedDurableCQListResponse::handleChunk: part is not object");
    throw MessageException(exMsg);
  }

  input.advanceCursor(1);  // skip the CacheableArrayList type ID byte

  int8_t stringParts;

  input.read(&stringParts);  // read the number of strings in the message this
                             // is one byte

  CacheableStringPtr strTemp;
  for (int i = 0; i < stringParts; i++) {
    input.readObject(strTemp);
    m_resultList->push_back(strTemp);
  }
}
