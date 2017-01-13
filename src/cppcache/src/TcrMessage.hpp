/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __TCR_MESSAGE_HPP__
#define __TCR_MESSAGE_HPP__

#include <gfcpp/gfcpp_globals.hpp>
#include "AtomicInc.hpp"
#include <gfcpp/Cacheable.hpp>
#include <gfcpp/CacheableKey.hpp>
#include <gfcpp/CacheableString.hpp>
#include <gfcpp/UserData.hpp>
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/DataInput.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include "InterestResultPolicy.hpp"
#include "EventId.hpp"
#include "EventIdMap.hpp"
#include <gfcpp/CacheableBuiltins.hpp>
#include "TcrChunkedContext.hpp"
#include <gfcpp/VectorT.hpp>
#include "GemfireTypeIdsImpl.hpp"
#include "BucketServerLocation.hpp"
#include "FixedPartitionAttributesImpl.hpp"
#include "VersionTag.hpp"
#include "VersionedCacheableObjectPartList.hpp"
#include <string>
#include <map>
#include <vector>

namespace gemfire {
class TcrMessage;
class ThinClientRegion;
class ThinClientBaseDM;
class TcrMessageHelper;
class TcrConnection;
class TcrMessagePing;
class CPPCACHE_EXPORT TcrMessage {
 private:
  inline static void writeInt(uint8_t* buffer, uint16_t value);
  inline static void writeInt(uint8_t* buffer, uint32_t value);
  inline static void readInt(uint8_t* buffer, uint16_t* value);
  inline static void readInt(uint8_t* buffer, uint32_t* value);

 public:
  typedef enum {
    /* Server couldn't read message; handle it like a server side
       exception that needs retries */
    NOT_PUBLIC_API_WITH_TIMEOUT = -2,
    INVALID = -1,
    REQUEST = 0,
    RESPONSE /* 1 */,
    EXCEPTION /* 2 */,
    REQUEST_DATA_ERROR /* 3 */,
    DATA_NOT_FOUND_ERROR /* 4 Not in use */,
    PING /* 5 */,
    REPLY /* 6 */,
    PUT /* 7 */,
    PUT_DATA_ERROR /* 8 */,
    DESTROY /* 9 */,
    DESTROY_DATA_ERROR /* 10 */,
    DESTROY_REGION /* 11 */,
    DESTROY_REGION_DATA_ERROR /* 12 */,
    CLIENT_NOTIFICATION /* 13 */,
    UPDATE_CLIENT_NOTIFICATION /* 14 */,
    LOCAL_INVALIDATE /* 15 */,
    LOCAL_DESTROY /* 16 */,
    LOCAL_DESTROY_REGION /* 17 */,
    CLOSE_CONNECTION /* 18 */,
    PROCESS_BATCH /* 19 */,
    REGISTER_INTEREST /* 20 */,
    REGISTER_INTEREST_DATA_ERROR /* 21 */,
    UNREGISTER_INTEREST /* 22 */,
    UNREGISTER_INTEREST_DATA_ERROR /* 23 */,
    REGISTER_INTEREST_LIST /* 24 */,
    UNREGISTER_INTEREST_LIST /* 25 */,
    UNKNOWN_MESSAGE_TYPE_ERROR /* 26 */,
    LOCAL_CREATE /* 27 */,
    LOCAL_UPDATE /* 28 */,
    CREATE_REGION /* 29 */,
    CREATE_REGION_DATA_ERROR /* 30 */,
    MAKE_PRIMARY /* 31 */,
    RESPONSE_FROM_PRIMARY /* 32 */,
    RESPONSE_FROM_SECONDARY /* 33 */,
    QUERY /* 34 */,
    QUERY_DATA_ERROR /* 35 */,
    CLEAR_REGION /* 36 */,
    CLEAR_REGION_DATA_ERROR /* 37 */,
    CONTAINS_KEY /* 38 */,
    CONTAINS_KEY_DATA_ERROR /* 39 */,
    KEY_SET /* 40 */,
    KEY_SET_DATA_ERROR /* 41 */,
    EXECUTECQ_MSG_TYPE /* 42 */,
    EXECUTECQ_WITH_IR_MSG_TYPE /*43 */,
    STOPCQ_MSG_TYPE /*44*/,
    CLOSECQ_MSG_TYPE /*45 */,
    CLOSECLIENTCQS_MSG_TYPE /*46*/,
    CQDATAERROR_MSG_TYPE /*47 */,
    GETCQSTATS_MSG_TYPE /*48 */,
    MONITORCQ_MSG_TYPE /*49 */,
    CQ_EXCEPTION_TYPE /*50 */,
    REGISTER_INSTANTIATORS = 51 /* 51 */,
    PERIODIC_ACK = 52 /* 52 */,
    CLIENT_READY /* 53 */,
    CLIENT_MARKER /* 54 */,
    INVALIDATE_REGION /* 55 */,
    PUTALL /* 56 */,
    GET_ALL_DATA_ERROR = 58 /* 58 */,
    EXECUTE_REGION_FUNCTION = 59 /* 59 */,
    EXECUTE_REGION_FUNCTION_RESULT /* 60 */,
    EXECUTE_REGION_FUNCTION_ERROR /* 61 */,
    EXECUTE_FUNCTION /* 62 */,
    EXECUTE_FUNCTION_RESULT /* 63 */,
    EXECUTE_FUNCTION_ERROR /* 64 */,
    CLIENT_REGISTER_INTEREST = 65 /* 65 */,
    CLIENT_UNREGISTER_INTEREST = 66,
    REGISTER_DATASERIALIZERS = 67,
    REQUEST_EVENT_VALUE = 68,
    REQUEST_EVENT_VALUE_ERROR = 69,             /*69*/
    PUT_DELTA_ERROR = 70,                       /*70*/
    GET_CLIENT_PR_METADATA = 71,                /*71*/
    RESPONSE_CLIENT_PR_METADATA = 72,           /*72*/
    GET_CLIENT_PARTITION_ATTRIBUTES = 73,       /*73*/
    RESPONSE_CLIENT_PARTITION_ATTRIBUTES = 74,  /*74*/
    GET_CLIENT_PR_METADATA_ERROR = 75,          /*75*/
    GET_CLIENT_PARTITION_ATTRIBUTES_ERROR = 76, /*76*/
    USER_CREDENTIAL_MESSAGE = 77,
    REMOVE_USER_AUTH = 78,
    EXECUTE_REGION_FUNCTION_SINGLE_HOP = 79,
    QUERY_WITH_PARAMETERS = 80,
    SIZE = 81,
    SIZE_ERROR = 82,
    INVALIDATE = 83,
    INVALIDATE_ERROR = 84,
    COMMIT = 85,
    COMMIT_ERROR = 86,
    ROLLBACK = 87,
    TX_FAILOVER = 88,
    GET_ENTRY = 89,
    TX_SYNCHRONIZATION = 90,
    GET_FUNCTION_ATTRIBUTES = 91,
    GET_PDX_TYPE_BY_ID = 92,
    GET_PDX_ID_FOR_TYPE = 93,
    ADD_PDX_TYPE = 94,
    ADD_PDX_ENUM = 96,
    GET_PDX_ID_FOR_ENUM = 97,
    GET_PDX_ENUM_BY_ID = 98,
    SERVER_TO_CLIENT_PING = 99,
    // GATEWAY_RECEIVER_COMMAND = 99,
    GET_ALL_70 = 100,
    TOMBSTONE_OPERATION = 103,
    GETDURABLECQS_MSG_TYPE = 105,
    GET_DURABLE_CQS_DATA_ERROR = 106,
    GET_ALL_WITH_CALLBACK = 107,
    PUT_ALL_WITH_CALLBACK = 108,
    REMOVE_ALL = 109

  } MsgType;

  static bool init();
  static void cleanup();
  static bool isKeepAlive() { return *m_keepalive; }
  static bool isUserInitiativeOps(const TcrMessage& msg) {
    int32_t msgType = msg.getMessageType();

    if (!msg.isMetaRegion() &&
        !(msgType == TcrMessage::PING || msgType == TcrMessage::PERIODIC_ACK ||
          msgType == TcrMessage::MAKE_PRIMARY ||
          msgType == TcrMessage::CLOSE_CONNECTION ||
          msgType == TcrMessage::CLIENT_READY ||
          msgType == TcrMessage::INVALID ||
          msgType == TcrMessage::MONITORCQ_MSG_TYPE ||
          msgType == TcrMessage::GETCQSTATS_MSG_TYPE ||
          msgType == TcrMessage::REQUEST_EVENT_VALUE ||
          msgType == TcrMessage::GET_CLIENT_PR_METADATA ||
          msgType == TcrMessage::GET_CLIENT_PARTITION_ATTRIBUTES ||
          msgType == TcrMessage::GET_PDX_ID_FOR_TYPE ||
          msgType == TcrMessage::GET_PDX_TYPE_BY_ID ||
          msgType == TcrMessage::ADD_PDX_TYPE || msgType == TcrMessage::SIZE ||
          msgType == TcrMessage::TX_FAILOVER ||
          msgType == TcrMessage::GET_ENTRY ||
          msgType == TcrMessage::TX_SYNCHRONIZATION ||
          msgType == TcrMessage::GET_FUNCTION_ATTRIBUTES ||
          msgType == TcrMessage::ADD_PDX_ENUM ||
          msgType == TcrMessage::GET_PDX_ENUM_BY_ID ||
          msgType == TcrMessage::GET_PDX_ID_FOR_ENUM ||
          msgType == TcrMessage::COMMIT || msgType == TcrMessage::ROLLBACK)) {
      return true;
    }
    return false;
  }
  static VersionTagPtr readVersionTagPart(DataInput& input,
                                          uint16_t endpointMemId);

  /* constructors */
  void setData(const char* bytearray, int32_t len, uint16_t memId);

  void startProcessChunk(ACE_Semaphore& finalizeSema);
  // NULL chunk means that this is the last chunk
  void processChunk(const uint8_t* chunk, int32_t chunkLen,
                    uint16_t endpointmemId,
                    const uint8_t isLastChunkAndisSecurityHeader = 0x00);
  /* For creating a region on the java server */
  /* Note through this you can only create a sub region on the cache server */
  /* also for creating REGISTER_INTEREST regex request */
  // TcrMessage( TcrMessage::MsgType msgType, const std::string& str1,
  //  const std::string& str2, InterestResultPolicy interestPolicy =
  //  InterestResultPolicy::NONE, bool isDurable = false, bool isCachingEnabled
  //  = false, bool receiveValues = true, ThinClientBaseDM *connectionDM =
  //  NULL);

  void InitializeGetallMsg(const UserDataPtr& aCallbackArgument);
  // for multiuser cache close

  // Updates the early ack byte of the message to reflect that it is a retry op
  void updateHeaderForRetry();

  inline const VectorOfCacheableKey* getKeys() const { return m_keyList; }

  inline const std::string& getRegex() const { return m_regex; }

  inline InterestResultPolicy getInterestResultPolicy() const {
    if (m_interestPolicy == 2)
      return InterestResultPolicy::KEYS_VALUES;
    else if (m_interestPolicy == 1)
      return InterestResultPolicy::KEYS;
    else
      return InterestResultPolicy::NONE;
  }

  const char* getPoolName();

  /**
   * Whether the request is meant to be
   * sent to PR primary node for single hop.
   */
  inline bool forPrimary() const {
    return m_msgType == TcrMessage::PUT || m_msgType == TcrMessage::DESTROY ||
           m_msgType == TcrMessage::EXECUTE_REGION_FUNCTION;
  }

  inline void initCqMap() { m_cqs = new std::map<std::string, int>(); }

  inline bool forSingleHop() const {
    return m_msgType == TcrMessage::PUT || m_msgType == TcrMessage::DESTROY ||
           m_msgType == TcrMessage::REQUEST ||
           m_msgType == TcrMessage::GET_ALL_70 ||
           m_msgType == TcrMessage::GET_ALL_WITH_CALLBACK ||
           m_msgType == TcrMessage::EXECUTE_REGION_FUNCTION ||
           m_msgType == TcrMessage::PUTALL ||
           m_msgType == TcrMessage::PUT_ALL_WITH_CALLBACK;
  }

  inline bool forTransaction() const { return m_txId != -1; }

  /*
  inline void getSingleHopFlags(bool& forSingleHop, bool& forPrimary) const
  {
    if (m_msgType == TcrMessage::PUT ||
         m_msgType == TcrMessage::DESTROY ||
         m_msgType == TcrMessage::REQUEST) {

           forSingleHop = true;

           if (m_msgType == TcrMessage::REQUEST) {
             forPrimary = false;
           } else {
             forPrimary = true;
           }

    } else {

      forSingleHop = false;
      forPrimary = false;

    }
  }
  */

  /* destroy the connection */
  virtual ~TcrMessage();

  const std::string& getRegionName() const;
  Region* getRegion() const;
  int32_t getMessageType() const;
  void setMessageType(int32_t msgType);
  void setMessageTypeRequest(int32_t msgType);  // the msgType of the request
                                                // that was made if this is a
                                                // reply
  int32_t getMessageTypeRequest() const;
  CacheableKeyPtr getKey() const;
  const CacheableKeyPtr& getKeyRef() const;
  CacheablePtr getValue() const;
  const CacheablePtr& getValueRef() const;
  CacheablePtr getCallbackArgument() const;
  const CacheablePtr& getCallbackArgumentRef() const;

  const std::map<std::string, int>* getCqs() const;
  bool getBoolValue() const { return m_boolValue; };
  inline const char* getException() {
    exceptionMessage = (m_value == NULLPTR ? CacheableString::create("(null)")
                                           : m_value->toString());
    return exceptionMessage->asChar();
  }
  const char* getMsgData() const;
  const char* getMsgHeader() const;
  const char* getMsgBody() const;
  uint32_t getMsgLength() const;
  uint32_t getMsgBodyLength() const;
  EventIdPtr getEventId() const;

  int32_t getTransId() const;
  void setTransId(int32_t txId);

  uint32_t getTimeout() const;
  void setTimeout(uint32_t timeout);

  /* we need a static method to generate ping */
  /* The caller should not delete the message since it is global. */
  static TcrMessagePing* getPingMessage();
  static TcrMessage* getAllEPDisMess();
  /* we need a static method to generate close connection message */
  /* The caller should not delete the message since it is global. */
  static TcrMessage* getCloseConnMessage();
  static void setKeepAlive(bool keepalive);
  bool isDurable() const { return m_isDurable; }
  bool receiveValues() const { return m_receiveValues; }
  bool hasCqPart() const { return m_hasCqsPart; }
  uint32_t getMessageTypeForCq() const { return m_msgTypeForCq; }
  bool isInterestListPassed() const { return m_isInterestListPassed; }
  bool shouldIgnore() const { return m_shouldIgnore; }
  int8 getMetaDataVersion() const { return m_metaDataVersion; }
  uint32_t getEntryNotFound() const { return m_entryNotFound; }
  int8 getserverGroupVersion() const { return m_serverGroupVersion; }
  std::vector<int8>* getFunctionAttributes() { return m_functionAttributes; }

  // set the DM for chunked response messages
  void setDM(ThinClientBaseDM* dm) { m_tcdm = dm; }

  ThinClientBaseDM* getDM() { return m_tcdm; }
  // set the chunked response handler
  void setChunkedResultHandler(TcrChunkedResult* chunkedResult) {
    this->m_isLastChunkAndisSecurityHeader = 0x0;
    m_chunkedResult = chunkedResult;
  }
  TcrChunkedResult* getChunkedResultHandler() { return m_chunkedResult; }
  void setVersionedObjectPartList(
      VersionedCacheableObjectPartListPtr versionObjPartListptr) {
    m_versionObjPartListptr = versionObjPartListptr;
  }

  VersionedCacheableObjectPartListPtr getVersionedObjectPartList() {
    return m_versionObjPartListptr;
  }

  DataInput* getDelta() { return m_delta; }

  //  getDeltaBytes( ) is called *only* by CqService, returns a CacheableBytes
  //  that
  // takes ownership of delta bytes.
  CacheableBytesPtr getDeltaBytes() {
    if (m_deltaBytes == NULL) {
      return NULLPTR;
    }
    CacheableBytesPtr retVal(
        CacheableBytes::createNoCopy(m_deltaBytes, m_deltaBytesLen));
    m_deltaBytes = NULL;
    return retVal;
  }

  bool hasDelta() { return (m_delta != NULL); }

  void addSecurityPart(int64_t connectionId, int64_t unique_id,
                       TcrConnection* conn);

  void addSecurityPart(int64_t connectionId, TcrConnection* conn);

  int64_t getConnectionId(TcrConnection* conn);

  int64_t getUniqueId(TcrConnection* conn);

  void createUserCredentialMessage(TcrConnection* conn);

  void readSecureObjectPart(DataInput& input, bool defaultString = false,
                            bool isChunk = false,
                            uint8_t isLastChunkWithSecurity = 0);

  void readUniqueIDObjectPart(DataInput& input);

  void setMetaRegion(bool isMetaRegion) { m_isMetaRegion = isMetaRegion; }

  bool isMetaRegion() const { return m_isMetaRegion; }

  int32_t getNumBuckets() const { return m_bucketCount; }

  CacheableStringPtr getColocatedWith() const { return m_colocatedWith; }

  CacheableStringPtr getPartitionResolver() const {
    return m_partitionResolverName;
  }

  std::vector<std::vector<BucketServerLocationPtr> >* getMetadata() {
    return m_metadata;
  }

  std::vector<FixedPartitionAttributesImplPtr>* getFpaSet() { return m_fpaSet; }

  CacheableHashSetPtr getFailedNode() { return m_failedNode; }

  bool isCallBackArguement() const { return m_isCallBackArguement; }

  void setCallBackArguement(bool aCallBackArguement) {
    m_isCallBackArguement = aCallBackArguement;
  }

  void setBucketServerLocation(BucketServerLocationPtr serverLocation) {
    m_bucketServerLocation = serverLocation;
  }
  void setVersionTag(VersionTagPtr versionTag) { m_versionTag = versionTag; }
  VersionTagPtr getVersionTag() { return m_versionTag; }
  uint8_t hasResult() const { return m_hasResult; }
  CacheableHashMapPtr getTombstoneVersions() const {
    return m_tombstoneVersions;
  }
  CacheableHashSetPtr getTombstoneKeys() const { return m_tombstoneKeys; }

  bool isFEAnotherHop();

 protected:
  TcrMessage()
      : m_feAnotherHop(false),
        m_connectionIDBytes(NULLPTR),
        isSecurityOn(false),
        m_isLastChunkAndisSecurityHeader(0),
        m_isSecurityHeaderAdded(false),
        m_creds(),
        m_securityHeaderLength(0),
        m_isMetaRegion(false),
        exceptionMessage(),
        m_request(new DataOutput),
        m_msgType(TcrMessage::INVALID),
        m_msgLength(-1),
        m_msgTypeRequest(0),
        m_txId(-1),
        m_decodeAll(false),
        m_tcdm(NULL),
        m_chunkedResult(NULL),
        m_keyList(NULL),
        m_key(),
        m_value(NULLPTR),
        m_failedNode(),
        m_callbackArgument(NULLPTR),
        m_versionTag(),
        m_eventid(NULLPTR),
        m_regionName("INVALID_REGION_NAME"),
        m_region(NULL),
        m_regex(),
        m_interestPolicy(0),
        m_timeout(15 /*DEFAULT_TIMEOUT_SECONDS*/),
        m_isDurable(false),
        m_receiveValues(false),
        m_hasCqsPart(false),
        m_isInterestListPassed(false),
        m_shouldIgnore(false),
        m_metaDataVersion(0),
        m_serverGroupVersion(0),
        m_bucketServerLocations(),
        m_metadata(),
        m_bucketCount(0),
        m_colocatedWith(),
        m_partitionResolverName(),
        m_vectorPtr(),
        m_numCqPart(0),
        m_msgTypeForCq(0),
        m_cqs(NULL),
        m_messageResponseTimeout(-1),
        m_boolValue(0),
        m_delta(NULL),
        m_deltaBytes(NULL),
        m_deltaBytesLen(0),
        m_isCallBackArguement(false),
        m_bucketServerLocation(NULLPTR),
        m_entryNotFound(0),
        m_fpaSet(),
        m_functionAttributes(),
        m_hasResult(0),
        m_tombstoneVersions(),
        m_tombstoneKeys(),
        m_versionObjPartListptr() {}

  void handleSpecialFECase();
  bool m_feAnotherHop;
  void writeBytesOnly(const SerializablePtr& se);
  SerializablePtr readCacheableBytes(DataInput& input, int lenObj);
  SerializablePtr readCacheableString(DataInput& input, int lenObj);

  static AtomicInc m_transactionId;
  static TcrMessagePing* m_pingMsg;
  static TcrMessage* m_closeConnMsg;
  static TcrMessage* m_allEPDisconnected;
  static uint8_t* m_keepalive;
  const static int m_flag_empty;
  const static int m_flag_concurrency_checks;

  CacheableBytesPtr m_connectionIDBytes;
  bool isSecurityOn;
  uint8_t m_isLastChunkAndisSecurityHeader;
  bool m_isSecurityHeaderAdded;
  PropertiesPtr m_creds;
  int32_t m_securityHeaderLength;
  bool m_isMetaRegion;

  CacheableStringPtr exceptionMessage;

  // Disallow copy constructor and assignment operator.
  TcrMessage(const TcrMessage&);
  TcrMessage& operator=(const TcrMessage&);

  // some private methods to handle things internally.
  void handleByteArrayResponse(const char* bytearray, int32_t len,
                               uint16_t endpointMemId);
  void readObjectPart(DataInput& input, bool defaultString = false);
  void readFailedNodePart(DataInput& input, bool defaultString = false);
  void readCallbackObjectPart(DataInput& input, bool defaultString = false);
  void readKeyPart(DataInput& input);
  void readBooleanPartAsObject(DataInput& input, bool* boolVal);
  void readIntPart(DataInput& input, uint32_t* intValue);
  void readLongPart(DataInput& input, uint64_t* intValue);
  bool readExceptionPart(DataInput& input, uint8_t isLastChunk,
                         bool skipFirstPart = true);
  void readVersionTag(DataInput& input, uint16_t endpointMemId);
  void readOldValue(DataInput& input);
  void readPrMetaData(DataInput& input);
  void writeObjectPart(const SerializablePtr& se, bool isDelta = false,
                       bool callToData = false,
                       const VectorOfCacheableKey* getAllKeyList = NULL);
  void writeHeader(uint32_t msgType, uint32_t numOfParts);
  void writeRegionPart(const std::string& regionName);
  void writeStringPart(const std::string& regionName);
  void writeEventIdPart(int reserveSize = 0,
                        bool fullValueAfterDeltaFail = false);
  void writeMessageLength();
  void writeInterestResultPolicyPart(InterestResultPolicy policy);
  void writeIntPart(int32_t intValue);
  void writeBytePart(uint8_t byteValue);
  void writeByteAndTimeOutPart(uint8_t byteValue, int32_t timeout);
  void chunkSecurityHeader(int skipParts, const uint8_t* bytes, int32_t len,
                           uint8_t isLastChunkAndSecurityHeader);

  void readEventIdPart(DataInput& input, bool skip = false,
                       int32_t parts = 1);  // skip num parts then read eventid

  void skipParts(DataInput& input, int32_t numParts = 1);
  void readStringPart(DataInput& input, uint32_t* len, char** str);
  void readCqsPart(DataInput& input);
  void readHashMapForGCVersions(gemfire::DataInput& input,
                                CacheableHashMapPtr& value);
  void readHashSetForGCVersions(gemfire::DataInput& input,
                                CacheableHashSetPtr& value);
  DSMemberForVersionStampPtr readDSMember(gemfire::DataInput& input);
  DataOutput* m_request;
  int32_t m_msgType;
  int32_t m_msgLength;
  int32_t m_msgTypeRequest;  // the msgType of the request if this TcrMessage is
                             // a reply msg

  int32_t m_txId;
  bool m_decodeAll;  // used only when decoding reply message, if false, decode
                     // header only

  // the associated region that is handling processing of chunked responses
  ThinClientBaseDM* m_tcdm;
  TcrChunkedResult* m_chunkedResult;

  const VectorOfCacheableKey* m_keyList;
  CacheableKeyPtr m_key;
  CacheablePtr m_value;
  CacheableHashSetPtr m_failedNode;
  CacheablePtr m_callbackArgument;
  VersionTagPtr m_versionTag;
  EventIdPtr m_eventid;
  std::string m_regionName;
  const Region* m_region;
  std::string m_regex;
  char m_interestPolicy;
  uint32_t m_timeout;
  bool m_isDurable;
  bool m_receiveValues;
  bool m_hasCqsPart;
  bool m_isInterestListPassed;
  bool m_shouldIgnore;
  int8 m_metaDataVersion;
  int8 m_serverGroupVersion;
  std::vector<BucketServerLocationPtr> m_bucketServerLocations;
  std::vector<std::vector<BucketServerLocationPtr> >* m_metadata;
  int32_t m_bucketCount;
  CacheableStringPtr m_colocatedWith;
  CacheableStringPtr m_partitionResolverName;
  CacheableVectorPtr m_vectorPtr;
  uint32_t m_numCqPart;
  uint32_t m_msgTypeForCq;  // new part since 7.0 for cq event message type.
  std::map<std::string, int>* m_cqs;
  int32_t m_messageResponseTimeout;
  bool m_boolValue;
  DataInput* m_delta;
  uint8_t* m_deltaBytes;
  int32_t m_deltaBytesLen;
  bool m_isCallBackArguement;
  BucketServerLocationPtr m_bucketServerLocation;
  uint32_t m_entryNotFound;
  std::vector<FixedPartitionAttributesImplPtr>* m_fpaSet;
  std::vector<int8>* m_functionAttributes;
  uint8_t m_hasResult;
  CacheableHashMapPtr m_tombstoneVersions;
  CacheableHashSetPtr m_tombstoneKeys;
  /*Added this member for PutALL versioning changes*/
  VersionedCacheableObjectPartListPtr m_versionObjPartListptr;

  friend class TcrMessageHelper;
};

class TcrMessageDestroyRegion : public TcrMessage {
 public:
  TcrMessageDestroyRegion(const Region* region,
                          const UserDataPtr& aCallbackArgument,
                          int messageResponsetimeout,
                          ThinClientBaseDM* connectionDM);

  virtual ~TcrMessageDestroyRegion() {}
};

class TcrMessageClearRegion : public TcrMessage {
 public:
  TcrMessageClearRegion(const Region* region,
                        const UserDataPtr& aCallbackArgument,
                        int messageResponsetimeout,
                        ThinClientBaseDM* connectionDM);

  virtual ~TcrMessageClearRegion() {}
};

class TcrMessageQuery : public TcrMessage {
 public:
  TcrMessageQuery(const std::string& regionName, int messageResponsetimeout,
                  ThinClientBaseDM* connectionDM);

  virtual ~TcrMessageQuery() {}
};

class TcrMessageStopCQ : public TcrMessage {
 public:
  TcrMessageStopCQ(const std::string& regionName, int messageResponsetimeout,
                   ThinClientBaseDM* connectionDM);

  virtual ~TcrMessageStopCQ() {}
};

class TcrMessageCloseCQ : public TcrMessage {
 public:
  TcrMessageCloseCQ(const std::string& regionName, int messageResponsetimeout,
                    ThinClientBaseDM* connectionDM);

  virtual ~TcrMessageCloseCQ() {}
};

class TcrMessageQueryWithParameters : public TcrMessage {
 public:
  TcrMessageQueryWithParameters(const std::string& regionName,
                                const UserDataPtr& aCallbackArgument,
                                CacheableVectorPtr paramList,
                                int messageResponsetimeout,
                                ThinClientBaseDM* connectionDM);

  virtual ~TcrMessageQueryWithParameters() {}
};

class TcrMessageContainsKey : public TcrMessage {
 public:
  TcrMessageContainsKey(const Region* region, const CacheableKeyPtr& key,
                        const UserDataPtr& aCallbackArgument,
                        bool isContainsKey, ThinClientBaseDM* connectionDM);

  virtual ~TcrMessageContainsKey() {}
};

class TcrMessageGetDurableCqs : public TcrMessage {
 public:
  TcrMessageGetDurableCqs(ThinClientBaseDM* connectionDM);

  virtual ~TcrMessageGetDurableCqs() {}
};

class TcrMessageRequest : public TcrMessage {
 public:
  TcrMessageRequest(const Region* region, const CacheableKeyPtr& key,
                    const UserDataPtr& aCallbackArgument,
                    ThinClientBaseDM* connectionDM = NULL);

  virtual ~TcrMessageRequest() {}
};

class TcrMessageInvalidate : public TcrMessage {
 public:
  TcrMessageInvalidate(const Region* region, const CacheableKeyPtr& key,
                       const UserDataPtr& aCallbackArgument,
                       ThinClientBaseDM* connectionDM = NULL);
};

class TcrMessageDestroy : public TcrMessage {
 public:
  TcrMessageDestroy(const Region* region, const CacheableKeyPtr& key,
                    const CacheablePtr& value,
                    const UserDataPtr& aCallbackArgument,
                    ThinClientBaseDM* connectionDM = NULL);
};

class TcrMessageRegisterInterestList : public TcrMessage {
 public:
  TcrMessageRegisterInterestList(
      const Region* region, const VectorOfCacheableKey& keys,
      bool isDurable = false, bool isCachingEnabled = false,
      bool receiveValues = true,
      InterestResultPolicy interestPolicy = InterestResultPolicy::NONE,
      ThinClientBaseDM* connectionDM = NULL);

  virtual ~TcrMessageRegisterInterestList() {}
};

class TcrMessageUnregisterInterestList : public TcrMessage {
 public:
  TcrMessageUnregisterInterestList(
      const Region* region, const VectorOfCacheableKey& keys,
      bool isDurable = false, bool isCachingEnabled = false,
      bool receiveValues = true,
      InterestResultPolicy interestPolicy = InterestResultPolicy::NONE,
      ThinClientBaseDM* connectionDM = NULL);

  virtual ~TcrMessageUnregisterInterestList() {}
};

class TcrMessagePut : public TcrMessage {
 public:
  TcrMessagePut(const Region* region, const CacheableKeyPtr& key,
                const CacheablePtr& value, const UserDataPtr& aCallbackArgument,
                bool isDelta = false, ThinClientBaseDM* connectionDM = NULL,
                bool isMetaRegion = false, bool fullValueAfterDeltaFail = false,
                const char* regionName = NULL);

  virtual ~TcrMessagePut() {}
};

class TcrMessageCreateRegion : public TcrMessage {
 public:
  TcrMessageCreateRegion(
      const std::string& str1, const std::string& str2,
      InterestResultPolicy interestPolicy = InterestResultPolicy::NONE,
      bool isDurable = false, bool isCachingEnabled = false,
      bool receiveValues = true, ThinClientBaseDM* connectionDM = NULL);

  virtual ~TcrMessageCreateRegion() {}
};

class TcrMessageRegisterInterest : public TcrMessage {
 public:
  TcrMessageRegisterInterest(
      const std::string& str1, const std::string& str2,
      InterestResultPolicy interestPolicy = InterestResultPolicy::NONE,
      bool isDurable = false, bool isCachingEnabled = false,
      bool receiveValues = true, ThinClientBaseDM* connectionDM = NULL);

  virtual ~TcrMessageRegisterInterest() {}
};

class TcrMessageUnregisterInterest : public TcrMessage {
 public:
  TcrMessageUnregisterInterest(
      const std::string& str1, const std::string& str2,
      InterestResultPolicy interestPolicy = InterestResultPolicy::NONE,
      bool isDurable = false, bool isCachingEnabled = false,
      bool receiveValues = true, ThinClientBaseDM* connectionDM = NULL);

  virtual ~TcrMessageUnregisterInterest() {}
};

class TcrMessageTxSynchronization : public TcrMessage {
 public:
  TcrMessageTxSynchronization(int ordinal, int txid, int status);

  virtual ~TcrMessageTxSynchronization() {}
};

class TcrMessageClientReady : public TcrMessage {
 public:
  TcrMessageClientReady();

  virtual ~TcrMessageClientReady() {}
};

class TcrMessageCommit : public TcrMessage {
 public:
  TcrMessageCommit();

  virtual ~TcrMessageCommit() {}
};

class TcrMessageRollback : public TcrMessage {
 public:
  TcrMessageRollback();

  virtual ~TcrMessageRollback() {}
};

class TcrMessageTxFailover : public TcrMessage {
 public:
  TcrMessageTxFailover();

  virtual ~TcrMessageTxFailover() {}
};

class TcrMessageMakePrimary : public TcrMessage {
 public:
  TcrMessageMakePrimary(bool processedMarker);

  virtual ~TcrMessageMakePrimary() {}
};

class TcrMessagePutAll : public TcrMessage {
 public:
  TcrMessagePutAll(const Region* region, const HashMapOfCacheable& map,
                   int messageResponsetimeout, ThinClientBaseDM* connectionDM,
                   const UserDataPtr& aCallbackArgument);

  virtual ~TcrMessagePutAll() {}
};

class TcrMessageRemoveAll : public TcrMessage {
 public:
  TcrMessageRemoveAll(const Region* region, const VectorOfCacheableKey& keys,
                      const UserDataPtr& aCallbackArgument,
                      ThinClientBaseDM* connectionDM = NULL);

  virtual ~TcrMessageRemoveAll() {}
};

class TcrMessageExecuteCq : public TcrMessage {
 public:
  TcrMessageExecuteCq(const std::string& str1, const std::string& str2,
                      int state, bool isDurable,
                      ThinClientBaseDM* connectionDM);

  virtual ~TcrMessageExecuteCq() {}
};

class TcrMessageExecuteCqWithIr : public TcrMessage {
 public:
  TcrMessageExecuteCqWithIr(const std::string& str1, const std::string& str2,
                            int state, bool isDurable,
                            ThinClientBaseDM* connectionDM);

  virtual ~TcrMessageExecuteCqWithIr() {}
};

class TcrMessageExecuteRegionFunction : public TcrMessage {
 public:
  TcrMessageExecuteRegionFunction(
      const std::string& funcName, const Region* region,
      const CacheablePtr& args, CacheableVectorPtr routingObj,
      uint8_t getResult, CacheableHashSetPtr failedNodes, int32_t timeout,
      ThinClientBaseDM* connectionDM = NULL, int8_t reExecute = 0);

  virtual ~TcrMessageExecuteRegionFunction() {}
};

class TcrMessageExecuteRegionFunctionSingleHop : public TcrMessage {
 public:
  TcrMessageExecuteRegionFunctionSingleHop(
      const std::string& funcName, const Region* region,
      const CacheablePtr& args, CacheableHashSetPtr routingObj,
      uint8_t getResult, CacheableHashSetPtr failedNodes, bool allBuckets,
      int32_t timeout, ThinClientBaseDM* connectionDM);

  virtual ~TcrMessageExecuteRegionFunctionSingleHop() {}
};

class TcrMessageGetClientPartitionAttributes : public TcrMessage {
 public:
  TcrMessageGetClientPartitionAttributes(const char* regionName);

  virtual ~TcrMessageGetClientPartitionAttributes() {}
};

class TcrMessageGetClientPrMetadata : public TcrMessage {
 public:
  TcrMessageGetClientPrMetadata(const char* regionName);

  virtual ~TcrMessageGetClientPrMetadata() {}
};

class TcrMessageSize : public TcrMessage {
 public:
  TcrMessageSize(const char* regionName);

  virtual ~TcrMessageSize() {}
};

class TcrMessageUserCredential : public TcrMessage {
 public:
  TcrMessageUserCredential(PropertiesPtr creds,
                           ThinClientBaseDM* connectionDM = NULL);

  virtual ~TcrMessageUserCredential() {}
};

class TcrMessageRemoveUserAuth : public TcrMessage {
 public:
  TcrMessageRemoveUserAuth(bool keepAlive, ThinClientBaseDM* connectionDM);

  virtual ~TcrMessageRemoveUserAuth() {}
};

class TcrMessageGetPdxIdForType : public TcrMessage {
 public:
  TcrMessageGetPdxIdForType(const CacheablePtr& pdxType,
                            ThinClientBaseDM* connectionDM,
                            int32_t pdxTypeId = 0);

  virtual ~TcrMessageGetPdxIdForType() {}
};

class TcrMessageAddPdxType : public TcrMessage {
 public:
  TcrMessageAddPdxType(const CacheablePtr& pdxType,
                       ThinClientBaseDM* connectionDM, int32_t pdxTypeId = 0);

  virtual ~TcrMessageAddPdxType() {}
};

class TcrMessageGetPdxIdForEnum : public TcrMessage {
 public:
  TcrMessageGetPdxIdForEnum(const CacheablePtr& pdxType,
                            ThinClientBaseDM* connectionDM,
                            int32_t pdxTypeId = 0);

  virtual ~TcrMessageGetPdxIdForEnum() {}
};

class TcrMessageAddPdxEnum : public TcrMessage {
 public:
  TcrMessageAddPdxEnum(const CacheablePtr& pdxType,
                       ThinClientBaseDM* connectionDM, int32_t pdxTypeId = 0);

  virtual ~TcrMessageAddPdxEnum() {}
};

class TcrMessageGetPdxTypeById : public TcrMessage {
 public:
  TcrMessageGetPdxTypeById(int32_t typeId, ThinClientBaseDM* connectionDM);

  virtual ~TcrMessageGetPdxTypeById() {}
};

class TcrMessageGetPdxEnumById : public TcrMessage {
 public:
  TcrMessageGetPdxEnumById(int32_t typeId, ThinClientBaseDM* connectionDM);

  virtual ~TcrMessageGetPdxEnumById() {}
};

class TcrMessageGetFunctionAttributes : public TcrMessage {
 public:
  TcrMessageGetFunctionAttributes(const std::string& funcName,
                                  ThinClientBaseDM* connectionDM = NULL);

  virtual ~TcrMessageGetFunctionAttributes() {}
};

class TcrMessageKeySet : public TcrMessage {
 public:
  TcrMessageKeySet(const std::string& funcName,
                   ThinClientBaseDM* connectionDM = NULL);

  virtual ~TcrMessageKeySet() {}
};

class TcrMessageRequestEventValue : public TcrMessage {
 public:
  TcrMessageRequestEventValue(EventIdPtr eventId);

  virtual ~TcrMessageRequestEventValue() {}
};

class TcrMessagePeriodicAck : public TcrMessage {
 public:
  TcrMessagePeriodicAck(const EventIdMapEntryList& entries);

  virtual ~TcrMessagePeriodicAck() {}
};

class TcrMessageUpdateClientNotification : public TcrMessage {
 public:
  TcrMessageUpdateClientNotification(int32_t port);

  virtual ~TcrMessageUpdateClientNotification() {}
};

class TcrMessageGetAll : public TcrMessage {
 public:
  TcrMessageGetAll(const Region* region, const VectorOfCacheableKey* keys,
                   ThinClientBaseDM* connectionDM = NULL,
                   const UserDataPtr& aCallbackArgument = NULLPTR);

  virtual ~TcrMessageGetAll() {}
};

class TcrMessageExecuteFunction : public TcrMessage {
 public:
  TcrMessageExecuteFunction(const std::string& funcName,
                            const CacheablePtr& args, uint8_t getResult,
                            ThinClientBaseDM* connectionDM, int32_t timeout);

  virtual ~TcrMessageExecuteFunction() {}
};

class TcrMessagePing : public TcrMessage {
 public:
  TcrMessagePing(bool decodeAll);

  virtual ~TcrMessagePing() {}
};

class TcrMessageCloseConnection : public TcrMessage {
 public:
  TcrMessageCloseConnection(bool decodeAll);

  virtual ~TcrMessageCloseConnection() {}
};

class TcrMessageClientMarker : public TcrMessage {
 public:
  TcrMessageClientMarker(bool decodeAll);

  virtual ~TcrMessageClientMarker() {}
};

class TcrMessageReply : public TcrMessage {
 public:
  TcrMessageReply(bool decodeAll, ThinClientBaseDM* connectionDM);

  virtual ~TcrMessageReply() {}
};

/**
 * Helper class to invoke some internal methods of TcrMessage. Add any
 * methods that response processor methods require to access here.
 */
class TcrMessageHelper {
 public:
  /**
   * result types returned by readChunkPartHeader
   */
  enum ChunkObjectType { OBJECT = 0, EXCEPTION = 1, NULL_OBJECT = 2 };

  /**
   * Tries to read an exception part and returns true if the exception
   * was successfully read.
   */
  inline static bool readExceptionPart(TcrMessage& msg, DataInput& input,
                                       uint8_t isLastChunk) {
    return msg.readExceptionPart(input, isLastChunk);
  }

  inline static void skipParts(TcrMessage& msg, DataInput& input,
                               int32_t numParts = 1) {
    msg.skipParts(input, numParts);
  }

  /**
   * Reads header of a chunk part. Returns true if header was successfully
   * read and false if it is a chunk exception part.
   * Throws a MessageException with relevant message if an unknown
   * message type is encountered in the header.
   */
  inline static ChunkObjectType readChunkPartHeader(
      TcrMessage& msg, DataInput& input, uint8_t expectedFirstType,
      int32_t expectedPartType, const char* methodName, uint32_t& partLen,
      uint8_t isLastChunk) {
    input.readInt(&partLen);
    bool isObj;
    input.readBoolean(&isObj);

    if (partLen == 0) {
      // special null object is case for scalar query result
      return NULL_OBJECT;
    } else if (!isObj) {
      // otherwise we're currently always expecting an object
      char exMsg[256];
      ACE_OS::snprintf(exMsg, 255,
                       "TcrMessageHelper::readChunkPartHeader: "
                       "%s: part is not object",
                       methodName);
      LOGDEBUG("%s ", exMsg);
      // throw MessageException(exMsg);
      return EXCEPTION;
    }

    uint8_t partType;
    input.read(&partType);
    int32_t compId = partType;

    //  ugly hack to check for exception chunk
    if (partType == GemfireTypeIdsImpl::JavaSerializable) {
      input.reset();
      if (TcrMessageHelper::readExceptionPart(msg, input, isLastChunk)) {
        msg.setMessageType(TcrMessage::EXCEPTION);
        return EXCEPTION;
      } else {
        char exMsg[256];
        ACE_OS::snprintf(
            exMsg, 255,
            "TcrMessageHelper::readChunkPartHeader: %s: cannot handle "
            "java serializable object from server",
            methodName);
        throw MessageException(exMsg);
      }
    } else if (partType == GemfireTypeIds::NullObj) {
      // special null object is case for scalar query result
      return NULL_OBJECT;
    }

    if (expectedFirstType > 0) {
      if (partType != expectedFirstType) {
        char exMsg[256];
        ACE_OS::snprintf(exMsg, 255,
                         "TcrMessageHelper::readChunkPartHeader: "
                         "%s: got unhandled object class = %d",
                         methodName, partType);
        throw MessageException(exMsg);
      }
      // This is for GETALL
      if (expectedFirstType == GemfireTypeIdsImpl::FixedIDShort) {
        int16_t shortId;
        input.readInt(&shortId);
        compId = shortId;
      }  // This is for QUERY or REGISTER INTEREST.
      else if (expectedFirstType == GemfireTypeIdsImpl::FixedIDByte ||
               expectedFirstType == 0) {
        input.read(&partType);
        compId = partType;
      }
    }
    if (compId != expectedPartType) {
      char exMsg[256];
      ACE_OS::snprintf(exMsg, 255,
                       "TcrMessageHelper::readChunkPartHeader: "
                       "%s: got unhandled object type = %d",
                       methodName, compId);
      throw MessageException(exMsg);
    }
    return OBJECT;
  }
  inline static int8_t readChunkPartHeader(TcrMessage& msg, DataInput& input,
                                           const char* methodName,
                                           uint32_t& partLen,
                                           uint8_t isLastChunk) {
    input.readInt(&partLen);
    bool isObj;
    input.readBoolean(&isObj);

    if (partLen == 0) {
      // special null object is case for scalar query result
      return (int8_t)NULL_OBJECT;
    } else if (!isObj) {
      // otherwise we're currently always expecting an object
      char exMsg[256];
      ACE_OS::snprintf(exMsg, 255,
                       "TcrMessageHelper::readChunkPartHeader: "
                       "%s: part is not object",
                       methodName);
      throw MessageException(exMsg);
    }

    int8_t partType;
    input.read(&partType);
    //  ugly hack to check for exception chunk
    if (partType == GemfireTypeIdsImpl::JavaSerializable) {
      input.reset();
      if (TcrMessageHelper::readExceptionPart(msg, input, isLastChunk)) {
        msg.setMessageType(TcrMessage::EXCEPTION);
        return (int8_t)EXCEPTION;
      } else {
        char exMsg[256];
        ACE_OS::snprintf(
            exMsg, 255,
            "TcrMessageHelper::readChunkPartHeader: %s: cannot handle "
            "java serializable object from server",
            methodName);
        throw MessageException(exMsg);
      }
    } else if (partType == GemfireTypeIds::NullObj) {
      // special null object is case for scalar query result
      return (int8_t)NULL_OBJECT;
    }
    return partType;
  }
};
}

#endif  // __TCR_MESSAGE_HPP__
