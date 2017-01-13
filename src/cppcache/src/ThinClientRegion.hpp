#ifndef __GEMFIRE_THINCLIENTREGION_H__
#define __GEMFIRE_THINCLIENTREGION_H__

/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*
* The specification of function behaviors is found in the corresponding .cpp
*file.
*
*
*========================================================================
*/

#include "LocalRegion.hpp"
#include "TcrMessage.hpp"
#include "TcrEndpoint.hpp"
#include "RegionGlobalLocks.hpp"
#include "Queue.hpp"
#include "TcrChunkedContext.hpp"
#include "CacheableObjectPartList.hpp"
#include <gfcpp/ResultCollector.hpp>

#include <ace/Task.h>

/**
* @file
*/

namespace gemfire {

class ThinClientBaseDM;

/**
* @class ThinClientRegion ThinClientRegion.hpp
*
* This class manages all the functionalities related with thin client
* region. It will inherit from DistributedRegion and overload some methods
*
*/

class CPPCACHE_EXPORT ThinClientRegion : public LocalRegion {
 public:
  /**
   * @brief constructor/initializer/destructor
   */
  ThinClientRegion(const std::string& name, CacheImpl* cache,
                   RegionInternal* rPtr, const RegionAttributesPtr& attributes,
                   const CacheStatisticsPtr& stats, bool shared = false);
  virtual void initTCR();
  virtual ~ThinClientRegion();

  /** @brief Public Methods from Region
   */
  // Unhide function to prevent SunPro Warnings
  using RegionInternal::registerKeys;
  virtual void registerKeys(const VectorOfCacheableKey& keys,
                            bool isDurable = false,
                            bool getInitialValues = false,
                            bool receiveValues = true);
  virtual void unregisterKeys(const VectorOfCacheableKey& keys);
  virtual void registerAllKeys(bool isDurable = false,
                               VectorOfCacheableKeyPtr resultKeys = NULLPTR,
                               bool getInitialValues = false,
                               bool receiveValues = true);
  virtual void unregisterAllKeys();
  virtual void registerRegex(const char* regex, bool isDurable = false,
                             VectorOfCacheableKeyPtr resultKeys = NULLPTR,
                             bool getInitialValues = false,
                             bool receiveValues = true);
  virtual void unregisterRegex(const char* regex);
  virtual void serverKeys(VectorOfCacheableKey& v);
  virtual void clear(const UserDataPtr& aCallbackArgument = NULLPTR);

  virtual SelectResultsPtr query(
      const char* predicate, uint32_t timeout = DEFAULT_QUERY_RESPONSE_TIMEOUT);
  virtual bool existsValue(const char* predicate,
                           uint32_t timeout = DEFAULT_QUERY_RESPONSE_TIMEOUT);
  virtual SerializablePtr selectValue(
      const char* predicate, uint32_t timeout = DEFAULT_QUERY_RESPONSE_TIMEOUT);

  /** @brief Public Methods from RegionInternal
   *  These are all virtual methods
   */
  GfErrType putAllNoThrow_remote(
      const HashMapOfCacheable& map,
      VersionedCacheableObjectPartListPtr& versionedObjPartList,
      uint32_t timeout = DEFAULT_RESPONSE_TIMEOUT,
      const UserDataPtr& aCallbackArgument = NULLPTR);
  GfErrType removeAllNoThrow_remote(
      const VectorOfCacheableKey& keys,
      VersionedCacheableObjectPartListPtr& versionedObjPartList,
      const UserDataPtr& aCallbackArgument = NULLPTR);
  GfErrType registerKeys(TcrEndpoint* endpoint = NULL,
                         const TcrMessage* request = NULL,
                         TcrMessageReply* reply = NULL);
  GfErrType unregisterKeys();
  void addKeys(const VectorOfCacheableKey& keys, bool isDurable,
               bool receiveValues, InterestResultPolicy interestpolicy);
  void addRegex(const std::string& regex, bool isDurable, bool receiveValues,
                InterestResultPolicy interestpolicy);
  GfErrType findRegex(const std::string& regex);
  void clearRegex(const std::string& regex);

  bool containsKeyOnServer(const CacheableKeyPtr& keyPtr) const;
  virtual bool containsValueForKey_remote(const CacheableKeyPtr& keyPtr) const;
  virtual void getInterestList(VectorOfCacheableKey& vlist) const;
  virtual void getInterestListRegex(VectorOfCacheableString& vregex) const;

  /** @brief Public Methods from RegionInternal
   *  These are all virtual methods
   */
  void receiveNotification(TcrMessage* msg);

  /** @brief Misc utility methods. */
  static GfErrType handleServerException(const char* func,
                                         const char* exceptionMsg);

  virtual void acquireGlobals(bool failover);
  virtual void releaseGlobals(bool failover);

  void localInvalidateFailover();

  inline ThinClientBaseDM* getDistMgr() { return m_tcrdm; }

  CacheableVectorPtr reExecuteFunction(
      const char* func, const CacheablePtr& args, CacheableVectorPtr routingObj,
      uint8_t getResult, ResultCollectorPtr rc, int32_t retryAttempts,
      CacheableHashSetPtr& failedNodes,
      uint32_t timeout = DEFAULT_QUERY_RESPONSE_TIMEOUT);
  bool executeFunctionSH(
      const char* func, const CacheablePtr& args, uint8_t getResult,
      ResultCollectorPtr rc,
      HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>* locationMap,
      CacheableHashSetPtr& failedNodes,
      uint32_t timeout = DEFAULT_QUERY_RESPONSE_TIMEOUT,
      bool allBuckets = false);
  void executeFunction(const char* func, const CacheablePtr& args,
                       CacheableVectorPtr routingObj, uint8_t getResult,
                       ResultCollectorPtr rc, int32_t retryAttempts,
                       uint32_t timeout = DEFAULT_QUERY_RESPONSE_TIMEOUT);
  GfErrType getFuncAttributes(const char* func, std::vector<int8>** attr);

  ACE_RW_Thread_Mutex& getMataDataMutex() { return m_RegionMutex; }

  bool const& getMetaDataRefreshed() { return m_isMetaDataRefreshed; }

  void setMetaDataRefreshed(bool aMetaDataRefreshed) {
    m_isMetaDataRefreshed = aMetaDataRefreshed;
  }

  uint32_t size_remote();

  virtual void txDestroy(const CacheableKeyPtr& key,
                         const UserDataPtr& callBack, VersionTagPtr versionTag);
  virtual void txInvalidate(const CacheableKeyPtr& key,
                            const UserDataPtr& callBack,
                            VersionTagPtr versionTag);
  virtual void txPut(const CacheableKeyPtr& key, const CacheablePtr& value,
                     const UserDataPtr& callBack, VersionTagPtr versionTag);

 protected:
  /** @brief the methods need to be overloaded in TCR
   */
  GfErrType getNoThrow_remote(const CacheableKeyPtr& keyPtr,
                              CacheablePtr& valPtr,
                              const UserDataPtr& aCallbackArgument,
                              VersionTagPtr& versionTag);
  GfErrType putNoThrow_remote(const CacheableKeyPtr& keyPtr,
                              const CacheablePtr& cvalue,
                              const UserDataPtr& aCallbackArgument,
                              VersionTagPtr& versionTag,
                              bool checkDelta = true);
  GfErrType createNoThrow_remote(const CacheableKeyPtr& keyPtr,
                                 const CacheablePtr& cvalue,
                                 const UserDataPtr& aCallbackArgument,
                                 VersionTagPtr& versionTag);
  GfErrType destroyNoThrow_remote(const CacheableKeyPtr& keyPtr,
                                  const UserDataPtr& aCallbackArgument,
                                  VersionTagPtr& versionTag);
  GfErrType removeNoThrow_remote(const CacheableKeyPtr& keyPtr,
                                 const CacheablePtr& cvalue,
                                 const UserDataPtr& aCallbackArgument,
                                 VersionTagPtr& versionTag);
  GfErrType removeNoThrowEX_remote(const CacheableKeyPtr& keyPtr,
                                   const UserDataPtr& aCallbackArgument,
                                   VersionTagPtr& versionTag);
  GfErrType invalidateNoThrow_remote(const CacheableKeyPtr& keyPtr,
                                     const UserDataPtr& aCallbackArgument,
                                     VersionTagPtr& versionTag);
  GfErrType getAllNoThrow_remote(const VectorOfCacheableKey* keys,
                                 const HashMapOfCacheablePtr& values,
                                 const HashMapOfExceptionPtr& exceptions,
                                 const VectorOfCacheableKeyPtr& resultKeys,
                                 bool addToLocalCache,
                                 const UserDataPtr& aCallbackArgument);
  GfErrType destroyRegionNoThrow_remote(const UserDataPtr& aCallbackArgument);
  GfErrType registerKeysNoThrow(
      const VectorOfCacheableKey& keys, bool attemptFailover = true,
      TcrEndpoint* endpoint = NULL, bool isDurable = false,
      InterestResultPolicy interestPolicy = InterestResultPolicy::NONE,
      bool receiveValues = true, TcrMessageReply* reply = NULL);
  GfErrType unregisterKeysNoThrow(const VectorOfCacheableKey& keys,
                                  bool attemptFailover = true);
  GfErrType unregisterKeysNoThrowLocalDestroy(const VectorOfCacheableKey& keys,
                                              bool attemptFailover = true);
  GfErrType registerRegexNoThrow(
      const std::string& regex, bool attemptFailover = true,
      TcrEndpoint* endpoint = NULL, bool isDurable = false,
      VectorOfCacheableKeyPtr resultKeys = NULLPTR,
      InterestResultPolicy interestPolicy = InterestResultPolicy::NONE,
      bool receiveValues = true, TcrMessageReply* reply = NULL);
  GfErrType unregisterRegexNoThrow(const std::string& regex,
                                   bool attemptFailover = true);
  GfErrType unregisterRegexNoThrowLocalDestroy(const std::string& regex,
                                               bool attemptFailover = true);
  GfErrType clientNotificationHandler(TcrMessage& msg);

  virtual void localInvalidateRegion_internal();

  virtual void localInvalidateForRegisterInterest(
      const VectorOfCacheableKey& keys);

  InterestResultPolicy copyInterestList(
      VectorOfCacheableKey& keysVector,
      std::unordered_map<CacheableKeyPtr, InterestResultPolicy>& interestList)
      const;
  virtual void release(bool invokeCallbacks = true);

  GfErrType unregisterKeysBeforeDestroyRegion();

  bool isDurableClient() { return m_isDurableClnt; }
  /** @brief Protected fields. */
  ThinClientBaseDM* m_tcrdm;
  ACE_Recursive_Thread_Mutex m_keysLock;
  mutable ACE_RW_Thread_Mutex m_rwDestroyLock;
  std::unordered_map<CacheableKeyPtr, InterestResultPolicy> m_interestList;
  std::unordered_map<std::string, InterestResultPolicy> m_interestListRegex;
  std::unordered_map<CacheableKeyPtr, InterestResultPolicy>
      m_durableInterestList;
  std::unordered_map<std::string, InterestResultPolicy>
      m_durableInterestListRegex;
  std::unordered_map<CacheableKeyPtr, InterestResultPolicy>
      m_interestListForUpdatesAsInvalidates;
  std::unordered_map<std::string, InterestResultPolicy>
      m_interestListRegexForUpdatesAsInvalidates;
  std::unordered_map<CacheableKeyPtr, InterestResultPolicy>
      m_durableInterestListForUpdatesAsInvalidates;
  std::unordered_map<std::string, InterestResultPolicy>
      m_durableInterestListRegexForUpdatesAsInvalidates;

  bool m_notifyRelease;
  ACE_Semaphore m_notificationSema;

  bool m_isDurableClnt;

  virtual void handleMarker() {}

  virtual void destroyDM(bool keepEndpoints = false);
  virtual void setProcessedMarker(bool mark = true){};

 private:
  bool isRegexRegistered(
      std::unordered_map<std::string, InterestResultPolicy>& interestListRegex,
      const std::string& regex, bool allKeys);
  GfErrType registerStoredRegex(
      TcrEndpoint*,
      std::unordered_map<std::string, InterestResultPolicy>& interestListRegex,
      bool isDurable = false, bool receiveValues = true);
  GfErrType unregisterStoredRegex(
      std::unordered_map<std::string, InterestResultPolicy>& interestListRegex);
  GfErrType unregisterStoredRegexLocalDestroy(
      std::unordered_map<std::string, InterestResultPolicy>& interestListRegex);
  void invalidateInterestList(
      std::unordered_map<CacheableKeyPtr, InterestResultPolicy>& interestList);
  GfErrType createOnServer(const CacheableKeyPtr& keyPtr,
                           const CacheablePtr& cvalue,
                           const UserDataPtr& aCallbackArgument);
  // method to get the values for a register interest
  void registerInterestGetValues(const char* method,
                                 const VectorOfCacheableKey* keys,
                                 const VectorOfCacheableKeyPtr& resultKeys);
  GfErrType getNoThrow_FullObject(EventIdPtr eventId, CacheablePtr& fullObject,
                                  VersionTagPtr& versionTag);

  // Disallow copy constructor and assignment operator.
  ThinClientRegion(const ThinClientRegion&);
  ThinClientRegion& operator=(const ThinClientRegion&);
  GfErrType singleHopPutAllNoThrow_remote(
      ThinClientPoolDM* tcrdm, const HashMapOfCacheable& map,
      VersionedCacheableObjectPartListPtr& versionedObjPartList,
      uint32_t timeout = DEFAULT_RESPONSE_TIMEOUT,
      const UserDataPtr& aCallbackArgument = NULLPTR);
  GfErrType multiHopPutAllNoThrow_remote(
      const HashMapOfCacheable& map,
      VersionedCacheableObjectPartListPtr& versionedObjPartList,
      uint32_t timeout = DEFAULT_RESPONSE_TIMEOUT,
      const UserDataPtr& aCallbackArgument = NULLPTR);

  GfErrType singleHopRemoveAllNoThrow_remote(
      ThinClientPoolDM* tcrdm, const VectorOfCacheableKey& keys,
      VersionedCacheableObjectPartListPtr& versionedObjPartList,
      const UserDataPtr& aCallbackArgument = NULLPTR);
  GfErrType multiHopRemoveAllNoThrow_remote(
      const VectorOfCacheableKey& keys,
      VersionedCacheableObjectPartListPtr& versionedObjPartList,
      const UserDataPtr& aCallbackArgument = NULLPTR);

  ACE_RW_Thread_Mutex m_RegionMutex;
  bool m_isMetaDataRefreshed;
};

// Chunk processing classes

/**
 * Handle each chunk of the chunked interest registration response.
 *
 *
 */
class ChunkedInterestResponse : public TcrChunkedResult {
 private:
  TcrMessage& m_msg;
  TcrMessage& m_replyMsg;
  VectorOfCacheableKeyPtr m_resultKeys;

  // disabled
  ChunkedInterestResponse(const ChunkedInterestResponse&);
  ChunkedInterestResponse& operator=(const ChunkedInterestResponse&);

 public:
  inline ChunkedInterestResponse(TcrMessage& msg,
                                 const VectorOfCacheableKeyPtr& resultKeys,
                                 TcrMessageReply& replyMsg)
      : TcrChunkedResult(),
        m_msg(msg),
        m_replyMsg(replyMsg),
        m_resultKeys(resultKeys) {}

  inline const VectorOfCacheableKeyPtr& getResultKeys() const {
    return m_resultKeys;
  }

  virtual void handleChunk(const uint8_t* chunk, int32_t chunkLen,
                           uint8_t isLastChunkWithSecurity);
  virtual void reset();
};

typedef SharedPtr<ChunkedInterestResponse> ChunkedInterestResponsePtr;

/**
 * Handle each chunk of the chunked query response.
 *
 *
 */
class ChunkedQueryResponse : public TcrChunkedResult {
 private:
  TcrMessage& m_msg;
  CacheableVectorPtr m_queryResults;
  std::vector<CacheableStringPtr> m_structFieldNames;

  void skipClass(DataInput& input);

  // disabled
  ChunkedQueryResponse(const ChunkedQueryResponse&);
  ChunkedQueryResponse& operator=(const ChunkedQueryResponse&);

 public:
  inline ChunkedQueryResponse(TcrMessage& msg)
      : TcrChunkedResult(),
        m_msg(msg),
        m_queryResults(CacheableVector::create()) {}

  inline const CacheableVectorPtr& getQueryResults() const {
    return m_queryResults;
  }

  inline const std::vector<CacheableStringPtr>& getStructFieldNames() const {
    return m_structFieldNames;
  }

  virtual void handleChunk(const uint8_t* chunk, int32_t chunkLen,
                           uint8_t isLastChunkWithSecurity);
  virtual void reset();

  void readObjectPartList(DataInput& input, bool isResultSet);
};

typedef SharedPtr<ChunkedQueryResponse> ChunkedQueryResponsePtr;
/**
 * Handle each chunk of the chunked function execution response.
 *
 *
 */
class ChunkedFunctionExecutionResponse : public TcrChunkedResult {
 private:
  TcrMessage& m_msg;
  // CacheableVectorPtr  m_functionExecutionResults;
  bool m_getResult;
  ResultCollectorPtr m_rc;
  ACE_Recursive_Thread_Mutex* m_resultCollectorLock;

  // disabled
  ChunkedFunctionExecutionResponse(const ChunkedFunctionExecutionResponse&);
  ChunkedFunctionExecutionResponse& operator=(
      const ChunkedFunctionExecutionResponse&);

 public:
  inline ChunkedFunctionExecutionResponse(
      TcrMessage& msg, bool getResult, ResultCollectorPtr rc,
      ACE_Recursive_Thread_Mutex* resultCollectorLock = NULL)
      : TcrChunkedResult(),
        m_msg(msg),
        m_getResult(getResult),
        m_rc(rc),
        m_resultCollectorLock(resultCollectorLock) {}

  /* inline const CacheableVectorPtr& getFunctionExecutionResults() const
   {
     return m_functionExecutionResults;
   }*/

  /* adongre
   * CID 28805: Parse warning (PW.USELESS_TYPE_QUALIFIER_ON_RETURN_TYPE)
   */
  // inline const bool getResult() const
  inline bool getResult() const { return m_getResult; }

  virtual void handleChunk(const uint8_t* chunk, int32_t chunkLen,
                           uint8_t isLastChunkWithSecurity);
  virtual void reset();
};
typedef SharedPtr<ChunkedFunctionExecutionResponse>
    ChunkedFunctionExecutionResponsePtr;

/**
 * Handle each chunk of the chunked getAll response.
 *
 *
 */
class ChunkedGetAllResponse : public TcrChunkedResult {
 private:
  TcrMessage& m_msg;
  ThinClientRegion* m_region;
  const VectorOfCacheableKey* m_keys;
  HashMapOfCacheablePtr m_values;
  HashMapOfExceptionPtr m_exceptions;
  VectorOfCacheableKeyPtr m_resultKeys;
  MapOfUpdateCounters& m_trackerMap;
  int32_t m_destroyTracker;
  bool m_addToLocalCache;
  uint32_t m_keysOffset;
  ACE_Recursive_Thread_Mutex& m_responseLock;
  // disabled
  ChunkedGetAllResponse(const ChunkedGetAllResponse&);
  ChunkedGetAllResponse& operator=(const ChunkedGetAllResponse&);

 public:
  inline ChunkedGetAllResponse(TcrMessage& msg, ThinClientRegion* region,
                               const VectorOfCacheableKey* keys,
                               const HashMapOfCacheablePtr& values,
                               const HashMapOfExceptionPtr& exceptions,
                               const VectorOfCacheableKeyPtr& resultKeys,
                               MapOfUpdateCounters& trackerMap,
                               int32_t destroyTracker, bool addToLocalCache,
                               ACE_Recursive_Thread_Mutex& responseLock)
      : TcrChunkedResult(),
        m_msg(msg),
        m_region(region),
        m_keys(keys),
        m_values(values),
        m_exceptions(exceptions),
        m_resultKeys(resultKeys),
        m_trackerMap(trackerMap),
        m_destroyTracker(destroyTracker),
        m_addToLocalCache(addToLocalCache),
        m_keysOffset(0),
        m_responseLock(responseLock) {}

  virtual void handleChunk(const uint8_t* chunk, int32_t chunkLen,
                           uint8_t isLastChunkWithSecurity);
  virtual void reset();

  void add(const ChunkedGetAllResponse* other);
  bool getAddToLocalCache() { return m_addToLocalCache; }
  HashMapOfCacheablePtr getValues() { return m_values; }
  HashMapOfExceptionPtr getExceptions() { return m_exceptions; }
  VectorOfCacheableKeyPtr getResultKeys() { return m_resultKeys; }
  MapOfUpdateCounters& getUpdateCounters() { return m_trackerMap; }
  ACE_Recursive_Thread_Mutex& getResponseLock() { return m_responseLock; }
};

typedef SharedPtr<ChunkedGetAllResponse> ChunkedGetAllResponsePtr;

/**
* Handle each chunk of the chunked putAll response.
*/
class ChunkedPutAllResponse : public TcrChunkedResult {
 private:
  TcrMessage& m_msg;
  const RegionPtr m_region;
  ACE_Recursive_Thread_Mutex& m_responseLock;
  VersionedCacheableObjectPartListPtr m_list;
  // disabled
  ChunkedPutAllResponse(const ChunkedPutAllResponse&);
  ChunkedPutAllResponse& operator=(const ChunkedPutAllResponse&);

 public:
  inline ChunkedPutAllResponse(const RegionPtr& region, TcrMessage& msg,
                               ACE_Recursive_Thread_Mutex& responseLock,
                               VersionedCacheableObjectPartListPtr& list)
      : TcrChunkedResult(),
        m_msg(msg),
        m_region(region),
        m_responseLock(responseLock),
        m_list(list) {}

  virtual void handleChunk(const uint8_t* chunk, int32_t chunkLen,
                           uint8_t isLastChunkWithSecurity);
  virtual void reset();
  VersionedCacheableObjectPartListPtr getList() { return m_list; }
  ACE_Recursive_Thread_Mutex& getResponseLock() { return m_responseLock; }
};

typedef SharedPtr<ChunkedPutAllResponse> ChunkedPutAllResponsePtr;

/**
* Handle each chunk of the chunked removeAll response.
*/
class ChunkedRemoveAllResponse : public TcrChunkedResult {
 private:
  TcrMessage& m_msg;
  const RegionPtr m_region;
  ACE_Recursive_Thread_Mutex& m_responseLock;
  VersionedCacheableObjectPartListPtr m_list;
  // disabled
  ChunkedRemoveAllResponse(const ChunkedRemoveAllResponse&);
  ChunkedRemoveAllResponse& operator=(const ChunkedRemoveAllResponse&);

 public:
  inline ChunkedRemoveAllResponse(const RegionPtr& region, TcrMessage& msg,
                                  ACE_Recursive_Thread_Mutex& responseLock,
                                  VersionedCacheableObjectPartListPtr& list)
      : TcrChunkedResult(),
        m_msg(msg),
        m_region(region),
        m_responseLock(responseLock),
        m_list(list) {}

  virtual void handleChunk(const uint8_t* chunk, int32_t chunkLen,
                           uint8_t isLastChunkWithSecurity);
  virtual void reset();
  VersionedCacheableObjectPartListPtr getList() { return m_list; }
  ACE_Recursive_Thread_Mutex& getResponseLock() { return m_responseLock; }
};

typedef SharedPtr<ChunkedRemoveAllResponse> ChunkedRemoveAllResponsePtr;

/**
 * Handle each chunk of the chunked interest registration response.
 *
 *
 */
class ChunkedKeySetResponse : public TcrChunkedResult {
 private:
  TcrMessage& m_msg;
  TcrMessage& m_replyMsg;
  VectorOfCacheableKey& m_resultKeys;

  // disabled
  ChunkedKeySetResponse(const ChunkedKeySetResponse&);
  ChunkedKeySetResponse& operator=(const ChunkedKeySetResponse&);

 public:
  inline ChunkedKeySetResponse(TcrMessage& msg,
                               VectorOfCacheableKey& resultKeys,
                               TcrMessageReply& replyMsg)
      : TcrChunkedResult(),
        m_msg(msg),
        m_replyMsg(replyMsg),
        m_resultKeys(resultKeys) {}

  virtual void handleChunk(const uint8_t* chunk, int32_t chunkLen,
                           uint8_t isLastChunkWithSecurity);
  virtual void reset();
};

typedef SharedPtr<ChunkedKeySetResponse> ChunkedKeySetResponsePtr;

class ChunkedDurableCQListResponse : public TcrChunkedResult {
 private:
  TcrMessage& m_msg;
  CacheableArrayListPtr m_resultList;

  // disabled
  ChunkedDurableCQListResponse(const ChunkedDurableCQListResponse&);
  ChunkedDurableCQListResponse& operator=(const ChunkedDurableCQListResponse&);

 public:
  inline ChunkedDurableCQListResponse(TcrMessage& msg)
      : TcrChunkedResult(),
        m_msg(msg),
        m_resultList(CacheableArrayList::create()) {}
  inline CacheableArrayListPtr getResults() { return m_resultList; }

  virtual void handleChunk(const uint8_t* chunk, int32_t chunkLen,
                           uint8_t isLastChunkWithSecurity);
  virtual void reset();
};

typedef SharedPtr<ChunkedDurableCQListResponse> ChunkedDurableCQListResponsePtr;

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_THINCLIENTREGION_H__
