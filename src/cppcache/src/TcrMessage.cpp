/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "TcrMessage.hpp"
#include <gfcpp/Assert.hpp>
#include <gfcpp/CacheableBuiltins.hpp>
#include <gfcpp/DistributedSystem.hpp>
#include <gfcpp/SystemProperties.hpp>
#include "TcrConnection.hpp"
#include "AutoDelete.hpp"
#include "TcrChunkedContext.hpp"
#include <gfcpp/CacheableObjectArray.hpp>
#include "ThinClientRegion.hpp"
#include "ThinClientBaseDM.hpp"
#include "StackTrace.hpp"
#include "TcrConnection.hpp"
#include "ThinClientPoolDM.hpp"
#include "TSSTXStateWrapper.hpp"
#include "TXState.hpp"
#include "DiskStoreId.hpp"
#include "DiskVersionTag.hpp"

using namespace gemfire;
static const uint32_t REGULAR_EXPRESSION =
    1;  // come from Java InterestType.REGULAR_EXPRESSION

namespace {
uint32_t g_headerLen = 17;
}

// AtomicInc TcrMessage::m_transactionId = 0;
TcrMessagePing* TcrMessage::m_pingMsg = NULL;
TcrMessage* TcrMessage::m_closeConnMsg = NULL;
TcrMessage* TcrMessage::m_allEPDisconnected = NULL;
uint8_t* TcrMessage::m_keepalive = NULL;
const int TcrMessage::m_flag_empty = 0x01;
const int TcrMessage::m_flag_concurrency_checks = 0x02;

bool TcrMessage::init() {
  bool ret = true;
  if (m_pingMsg == NULL) {
    try {
      m_pingMsg = new TcrMessagePing(true);
      m_closeConnMsg = new TcrMessageCloseConnection(true);

    } catch (std::exception& ex) {
      ret = false;
      LOGERROR(ex.what());
    } catch (Exception& ex) {
      ret = false;
      LOGERROR(ex.getMessage());
    } catch (...) {
      ret = false;
      LOGERROR("unknown exception");
    }
  }
  if (m_allEPDisconnected == NULL) {
    m_allEPDisconnected = new TcrMessageReply(true, NULL);
  }
  return ret;
}

void TcrMessage::cleanup() {
  GF_SAFE_DELETE(m_pingMsg);
  GF_SAFE_DELETE(m_closeConnMsg);
}

/* we need a static method to generate ping */
TcrMessagePing* TcrMessage::getPingMessage() { return m_pingMsg; }

TcrMessage* TcrMessage::getAllEPDisMess() { return m_allEPDisconnected; }
TcrMessage* TcrMessage::getCloseConnMessage() { return m_closeConnMsg; }

void TcrMessage::setKeepAlive(bool keepalive) {
  if (TcrMessage::m_keepalive != NULL) {
    *TcrMessage::m_keepalive = keepalive ? 1 : 0;
  }
}

void TcrMessage::writeInterestResultPolicyPart(InterestResultPolicy policy) {
  m_request->writeInt((int32_t)3);           // size
  m_request->write(static_cast<int8_t>(1));  // isObject
  m_request->write(static_cast<int8_t>(GemfireTypeIdsImpl::FixedIDByte));
  m_request->write(
      static_cast<int8_t>(GemfireTypeIdsImpl::InterestResultPolicy));
  m_request->write(static_cast<int8_t>(policy.getOrdinal()));
}

void TcrMessage::writeIntPart(int32_t intValue) {
  m_request->writeInt((int32_t)4);
  m_request->write(static_cast<int8_t>(0));
  m_request->writeInt(intValue);
}

void TcrMessage::writeBytePart(uint8_t byteValue) {
  m_request->writeInt((int32_t)1);
  m_request->write(static_cast<int8_t>(0));
  m_request->write(byteValue);
}

void TcrMessage::writeByteAndTimeOutPart(uint8_t byteValue, int32_t timeout) {
  m_request->writeInt((int32_t)5);  // 1 (byte) + 4 (timeout)
  m_request->write(static_cast<int8_t>(0));
  m_request->write(byteValue);
  m_request->writeInt(timeout);
}

void TcrMessage::readBooleanPartAsObject(DataInput& input, bool* boolVal) {
  int32_t lenObj;
  input.readInt(&lenObj);
  bool isObj;
  input.readBoolean(&isObj);
  if (lenObj > 0) {
    if (isObj) {
      // CacheableBooleanPtr cip;
      // input.readObject(cip);
      //*boolVal = cip->value();
      bool bVal = input.readNativeBool();
      *boolVal = bVal;
    }
  }
  /*
int32_t lenObj;
input.readInt( &lenObj );
if(lenObj!=2)
  throw Exception("Invalid boolean length, should have been 2");

bool isObj;
input.readBoolean( &isObj );
if(!isObj)
  throw Exception("boolean is not object");
char tmp[2];
input.readBytesOnly((int8_t*)tmp, (int32_t)2);
*boolVal = tmp[1]== 0? false : true;
*/
}

void TcrMessage::readOldValue(DataInput& input) {
  int32_t lenObj;
  int8_t isObj;
  input.readInt(&lenObj);
  input.read(&isObj);
  CacheablePtr value;
  input.readObject(value);  // we are not using this value currently
}

void TcrMessage::readPrMetaData(DataInput& input) {
  int32_t lenObj;
  int8_t isObj;
  input.readInt(&lenObj);
  input.read(&isObj);
  input.read(&isObj);  // read refresh meta data byte
  m_metaDataVersion = isObj;
  if (lenObj == 2) {
    input.read(&m_serverGroupVersion);
    LOGDEBUG("Single-hop m_serverGroupVersion in message reply is %d",
             m_serverGroupVersion);
  }
}

VersionTagPtr TcrMessage::readVersionTagPart(DataInput& input,
                                             uint16_t endpointMemId) {
  int8_t isObj;
  input.read(&isObj);
  VersionTagPtr versionTag;

  if (isObj == GemfireTypeIds::NullObj) return versionTag;

  if (isObj == GemfireTypeIdsImpl::FixedIDByte) {
    versionTag = new VersionTag();
    int8_t fixedId;
    input.read(&fixedId);
    if (fixedId == GemfireTypeIdsImpl::VersionTag) {
      versionTag->fromData(input);
      versionTag->replaceNullMemberId(endpointMemId);
      return versionTag;
    }
  } else if (isObj == GemfireTypeIdsImpl::FixedIDShort) {
    int16_t fixedId;
    input.readInt(&fixedId);
    if (fixedId == GemfireTypeIdsImpl::DiskVersionTag) {
      DiskVersionTag* disk = new DiskVersionTag();
      disk->fromData(input);
      versionTag = disk;
      return versionTag;
    }
  }
  return versionTag;
}

void TcrMessage::readVersionTag(DataInput& input, uint16_t endpointMemId) {
  int32_t lenObj;
  int8_t isObj;
  input.readInt(&lenObj);
  input.read(&isObj);

  if (lenObj == 0) return;
  VersionTagPtr versionTag(
      TcrMessage::readVersionTagPart(input, endpointMemId));
  this->setVersionTag(versionTag);
}

void TcrMessage::readIntPart(DataInput& input, uint32_t* intValue) {
  uint32_t intLen;
  input.readInt(&intLen);
  if (intLen != 4) throw Exception("int length should have been 4");
  int8_t isObj;
  input.read(&isObj);
  if (isObj) throw Exception("Integer is not an object");
  input.readInt(intValue);
}

void TcrMessage::readLongPart(DataInput& input, uint64_t* intValue) {
  uint32_t longLen;
  input.readInt(&longLen);
  if (longLen != 8) throw Exception("long length should have been 8");
  int8_t isObj;
  input.read(&isObj);
  if (isObj) throw Exception("Long is not an object");
  input.readInt(intValue);
}

void TcrMessage::readStringPart(DataInput& input, uint32_t* len, char** str) {
  char* ts;
  int32_t sl;
  input.readInt(&sl);
  ts = new char[sl];
  int8_t isObj;
  input.read(&isObj);
  if (isObj) throw Exception("String is not an object");
  input.readBytesOnly(reinterpret_cast<int8_t*>(ts), sl);
  *len = sl;
  *str = ts;
}
void TcrMessage::readCqsPart(DataInput& input) {
  m_cqs->clear();
  readIntPart(input, &m_numCqPart);
  for (uint32_t cqCnt = 0; cqCnt < m_numCqPart;) {
    char* cqName;
    uint32_t len;
    readStringPart(input, &len, &cqName);
    std::string cq(cqName, len);
    delete[] cqName;
    cqCnt++;
    int32_t cqOp;
    readIntPart(input, reinterpret_cast<uint32_t*>(&cqOp));
    cqCnt++;
    (*m_cqs)[cq] = cqOp;
    //	 LOGINFO("read cqName[%s],cqOp[%d]", cq.c_str(), cqOp);
  }
  // LOGDEBUG("mapsize = %d", m_cqs.size());
}

inline void TcrMessage::readCallbackObjectPart(DataInput& input,
                                               bool defaultString) {
  int32_t lenObj;
  input.readInt(&lenObj);
  bool isObj;
  input.readBoolean(&isObj);
  if (lenObj > 0) {
    if (isObj) {
      input.readObject(m_callbackArgument);
    } else {
      if (defaultString) {
        // TODO:
        // m_callbackArgument = CacheableString::create(
        //  (char*)input.currentBufferPosition( ), lenObj );
        m_callbackArgument = readCacheableString(input, lenObj);
      } else {
        // TODO::
        // m_callbackArgument = CacheableBytes::create(
        //  input.currentBufferPosition( ), lenObj );
        m_callbackArgument = readCacheableBytes(input, lenObj);
      }
      // input.advanceCursor( lenObj );
    }
  }
}

inline void TcrMessage::readObjectPart(DataInput& input, bool defaultString) {
  int32_t lenObj;
  input.readInt(&lenObj);
  // bool isObj;
  // input.readBoolean( &isObj );
  int8_t isObj;
  input.read(&isObj);
  if (lenObj > 0) {
    if (isObj == 1) {
      input.readObject(m_value);
    } else {
      if (defaultString) {
        // m_value = CacheableString::create(
        //  (char*)input.currentBufferPosition( ), lenObj );
        m_value = readCacheableString(input, lenObj);
      } else {
        // m_value = CacheableBytes::create(
        //  input.currentBufferPosition( ), lenObj );
        m_value = readCacheableBytes(input, lenObj);
      }
      // input.advanceCursor( lenObj );
    }
  } else if (lenObj == 0 && isObj == 2) {  // EMPTY BYTE ARRAY
    m_value = CacheableBytes::create();
  } else if (isObj == 0) {
    m_value = NULLPTR;
  }
}

void TcrMessage::readSecureObjectPart(DataInput& input, bool defaultString,
                                      bool isChunk,
                                      uint8_t isLastChunkWithSecurity) {
  LOGDEBUG(
      "TcrMessage::readSecureObjectPart isChunk = %d isLastChunkWithSecurity = "
      "%d",
      isChunk, isLastChunkWithSecurity);
  if (isChunk) {
    if (!(isLastChunkWithSecurity & 0x2)) {
      return;
    }
  }

  int32_t lenObj;
  input.readInt(&lenObj);
  bool isObj;
  input.readBoolean(&isObj);
  LOGDEBUG(
      "TcrMessage::readSecureObjectPart lenObj = %d isObj = %d, "
      "m_msgTypeRequest = %d defaultString = %d ",
      lenObj, isObj, m_msgTypeRequest, defaultString);
  if (lenObj > 0) {
    if (isObj) {
      // TODO: ??
      input.readObject(m_value);
    } else {
      if (defaultString) {
        // TODO: ??
        // m_value = CacheableString::create(
        //   (char*)input.currentBufferPosition( ), lenObj );
        m_value = readCacheableString(input, lenObj);
      } else {
        LOGDEBUG("reading connectionid");
        // TODO: this will execute always
        // input.rea.readInt(&connectionId);
        // m_connectionIDBytes =
        // CacheableBytes::create(input.currentBufferPosition(), lenObj);
        // m_connectionIDBytes = readCacheableBytes(input, lenObj);
        m_connectionIDBytes =
            CacheableBytes::create(input.currentBufferPosition(), lenObj);
        input.advanceCursor(lenObj);
      }
    }
  }
  if (input.getBytesRemaining() != 0) {
    LOGERROR("readSecureObjectPart: we not read all bytes. Messagetype:%d",
             m_msgType);
    throw IllegalStateException("didn't read all bytes");
  }
}

void TcrMessage::readUniqueIDObjectPart(DataInput& input) {
  LOGDEBUG("TcrMessage::readUniqueIDObjectPart");

  int32_t lenObj;
  input.readInt(&lenObj);
  bool isObj;
  input.readBoolean(&isObj);
  LOGDEBUG("TcrMessage::readUniqueIDObjectPart lenObj = %d isObj = %d", lenObj,
           isObj);
  if (lenObj > 0) {
    m_value = CacheableBytes::create(input.currentBufferPosition(), lenObj);
    input.advanceCursor(lenObj);
  }
}

int64_t TcrMessage::getConnectionId(TcrConnection* conn) {
  if (m_connectionIDBytes != NULLPTR) {
    CacheableBytesPtr tmp = conn->decryptBytes(m_connectionIDBytes);
    DataInput di(tmp->value(), tmp->length());
    int64_t connid;
    di.readInt(&connid);
    return connid;
  } else {
    LOGWARN("Returning 0 as internal connection ID msgtype = %d ", m_msgType);
    return 0;
  }
}

int64_t TcrMessage::getUniqueId(TcrConnection* conn) {
  if (m_value != NULLPTR) {
    CacheableBytesPtr encryptBytes =
        static_cast<CacheableBytes*>(m_value.ptr());

    CacheableBytesPtr tmp = conn->decryptBytes(encryptBytes);

    DataInput di(tmp->value(), tmp->length());
    int64_t uniqueid;
    di.readInt(&uniqueid);

    return uniqueid;
  }
  return 0;
}

inline void TcrMessage::readFailedNodePart(DataInput& input,
                                           bool defaultString) {
  int32_t lenObj;
  input.readInt(&lenObj);
  bool isObj;
  input.readBoolean(&isObj);
  m_failedNode = CacheableHashSet::create();
  int8_t typeId = 0;
  input.read(&typeId);
  // input.readDirectObject(m_failedNode, typeId);
  m_failedNode->fromData(input);
  LOGDEBUG("readFailedNodePart m_failedNode size = %d ", m_failedNode->size());
}

inline void TcrMessage::readKeyPart(DataInput& input) {
  int32_t lenObj;
  input.readInt(&lenObj);
  bool isObj;
  input.readBoolean(&isObj);
  if (lenObj > 0) {
    if (isObj) {
      input.readObject(m_key);
    } else {
      CacheableKeyPtr ckPtr(dynamic_cast<CacheableKey*>(
          readCacheableString(input, lenObj).ptr()));
      m_key = ckPtr;
      /* // check whether unicode or ASCII string (bug #356)
       uint16_t decodedLen = DataInput::getDecodedLength(
           input.currentBufferPosition(), lenObj);
       if (decodedLen == lenObj) {
         // ASCII string
         m_key = CacheableString::create((char*) input.currentBufferPosition(),
             lenObj);
         input.advanceCursor(lenObj);
       }
       else {
         wchar_t* wideStr;
         input.readUTFNoLen(&wideStr, decodedLen);
         m_key = CacheableString::createNoCopy(wideStr, decodedLen);
       }*/
    }
  }
}

inline void TcrMessage::writeInt(uint8_t* buffer, uint16_t value) {
  *(buffer++) = static_cast<uint8_t>(value >> 8);
  *(buffer++) = static_cast<uint8_t>(value);
}

inline void TcrMessage::writeInt(uint8_t* buffer, uint32_t value) {
  *(buffer++) = static_cast<uint8_t>(value >> 24);
  *(buffer++) = static_cast<uint8_t>(value >> 16);
  *(buffer++) = static_cast<uint8_t>(value >> 8);
  *(buffer++) = static_cast<uint8_t>(value);
}

SerializablePtr TcrMessage::readCacheableString(DataInput& input, int lenObj) {
  SerializablePtr sPtr;
  // check whether unicode or ASCII string (bug #356)
  unsigned int decodedLen =
      DataInput::getDecodedLength(input.currentBufferPosition(), lenObj);

  if (decodedLen == static_cast<unsigned int>(lenObj)) {
    if (decodedLen <= 0xffff) {
      input.rewindCursor(2);
      writeInt(const_cast<uint8_t*>(input.currentBufferPosition()),
               static_cast<uint16_t>(lenObj));
      input.readDirectObject(
          sPtr,
          static_cast<int8_t>(gemfire::GemfireTypeIds::CacheableASCIIString));
    } else {
      input.rewindCursor(4);
      writeInt(const_cast<uint8_t*>(input.currentBufferPosition()),
               static_cast<uint32_t>(lenObj));
      input.readDirectObject(
          sPtr, static_cast<int8_t>(
                    gemfire::GemfireTypeIds::CacheableASCIIStringHuge));
    }
  } else {
    if (decodedLen <= 0xffff) {
      input.rewindCursor(2);
      writeInt(const_cast<uint8_t*>(input.currentBufferPosition()),
               static_cast<uint16_t>(lenObj));
      input.readDirectObject(
          sPtr, static_cast<int8_t>(gemfire::GemfireTypeIds::CacheableString));
    } else {
      input.rewindCursor(4);
      writeInt(const_cast<uint8_t*>(input.currentBufferPosition()),
               static_cast<uint32_t>(lenObj));
      input.readDirectObject(
          sPtr,
          static_cast<int8_t>(gemfire::GemfireTypeIds::CacheableStringHuge));
    }
  }

  return sPtr;
}

SerializablePtr TcrMessage::readCacheableBytes(DataInput& input, int lenObj) {
  SerializablePtr sPtr;

  if (lenObj <= 252) {  // 252 is java's ((byte)-4 && 0xFF)
    input.rewindCursor(1);
    uint8_t* buffer = const_cast<uint8_t*>(input.currentBufferPosition());
    buffer[0] = static_cast<uint8_t>(lenObj);
  } else if (lenObj <= 0xFFFF) {
    input.rewindCursor(3);
    uint8_t* buffer = const_cast<uint8_t*>(input.currentBufferPosition());
    buffer[0] = static_cast<uint8_t>(-2);
    writeInt(buffer + 1, static_cast<uint16_t>(lenObj));
  } else {
    input.rewindCursor(5);
    uint8_t* buffer = const_cast<uint8_t*>(input.currentBufferPosition());
    buffer[0] = static_cast<uint8_t>(-3);
    writeInt(buffer + 1, static_cast<uint32_t>(lenObj));
  }

  input.readDirectObject(
      sPtr, static_cast<int8_t>(gemfire::GemfireTypeIds::CacheableBytes));

  return sPtr;
}

bool TcrMessage::readExceptionPart(DataInput& input, uint8_t isLastChunk,
                                   bool skipFirstPart) {
  // Reading exception message sent from java cache server.
  // Read the first part which is serialized java exception and ignore it.
  // Then read the second part which is string and use it to construct the
  // exception.

  isLastChunk = (isLastChunk >> 5);
  LOGDEBUG("TcrMessage::readExceptionPart: isLastChunk = %d", isLastChunk);
  if (skipFirstPart == true) {
    skipParts(input, 1);
    isLastChunk--;
  }

  if (/*input.getBytesRemaining() > 0 */ isLastChunk > 0) {
    //  Do a best effort read for exception since it is possible
    // that this is invoked for non-exception chunk message which has
    // only one part.
    isLastChunk--;
    readObjectPart(input, true);
    // if (input.getBytesRemaining() > 0 && m_msgTypeRequest ==
    // TcrMessage::EXECUTE_REGION_FUNCTION && m_msgType !=
    // TcrMessage::EXCEPTION) {
    if (isLastChunk > 0) {
      readFailedNodePart(input, true);
      return true;  // 3 parts
    } else {
      return true;  // 2 parts
    }
  }
  return false;
}

void TcrMessage::writeObjectPart(const SerializablePtr& se, bool isDelta,
                                 bool callToData,
                                 const VectorOfCacheableKey* getAllKeyList) {
  //  no NULL check since for some messages NULL object may be valid
  uint32_t size = 0;
  m_request->writeInt(
      static_cast<int32_t>(size));  // write a dummy size of 4 bytes.
  // check if the type is a CacheableBytes
  int8_t isObject = 1;

  if (se != NULLPTR && se->typeId() == GemfireTypeIds::CacheableBytes) {
    // for an emty byte array write EMPTY_BYTEARRAY_CODE(2) to is object
    try {
      int byteArrLength = -1;

      if (instanceOf<CacheableBytesPtr>(se)) {
        CacheableBytesPtr cacheableBytes = dynCast<CacheableBytesPtr>(se);
        byteArrLength = cacheableBytes->length();
      } else {
        std::string classname(Utils::getCacheableKeyString(se)->asChar());
        if (classname.find("gemfire::ManagedCacheableKey") !=
            std::string::npos) {
          byteArrLength = se->objectSize();
        }
      }

      if (byteArrLength == 0) {
        isObject = 2;
        m_request->write(isObject);
        return;
      }
    } catch (const gemfire::Exception& ex) {
      LOGDEBUG("Exception in writing EMPTY_BYTE_ARRAY : %s", ex.getMessage());
    }
    isObject = 0;
  }

  if (isDelta) {
    m_request->write(static_cast<int8_t>(0));
  } else {
    m_request->write(isObject);
  }

  uint32_t sizeBeforeWritingObj = m_request->getBufferLength();
  if (isDelta) {
    DeltaPtr deltaPtr = dynCast<DeltaPtr>(se);
    deltaPtr->toDelta(*m_request);
  } else if (isObject) {
    if (!callToData) {
      if (getAllKeyList != NULL) {
        int8_t typeId = GemfireTypeIds::CacheableObjectArray;
        m_request->write(typeId);
        int32_t len = getAllKeyList->size();
        m_request->writeArrayLen(len);
        m_request->write(static_cast<int8_t>(GemfireTypeIdsImpl::Class));
        m_request->write(
            static_cast<int8_t>(GemfireTypeIds::CacheableASCIIString));
        m_request->writeASCII("java.lang.Object");
        for (int32_t index = 0; index < getAllKeyList->size(); ++index) {
          m_request->writeObject(getAllKeyList->operator[](index));
        }
      } else {
        m_request->writeObject(se, isDelta);
      }
    } else {
      se->toData(*m_request);
    }
  } else {
    // TODO::
    // CacheableBytes* rawByteArray = static_cast<CacheableBytes*>(se.ptr());
    // m_request->writeBytesOnly(rawByteArray->value(), rawByteArray->length());
    writeBytesOnly(se);
  }
  uint32_t sizeAfterWritingObj = m_request->getBufferLength();
  uint32_t sizeOfSerializedObj = sizeAfterWritingObj - sizeBeforeWritingObj;
  m_request->rewindCursor(sizeOfSerializedObj + 1 + 4);  //
  m_request->writeInt(static_cast<int32_t>(sizeOfSerializedObj));
  m_request->advanceCursor(sizeOfSerializedObj + 1);
}

void TcrMessage::readInt(uint8_t* buffer, uint16_t* value) {
  uint16_t tmp = *(buffer++);
  tmp = (tmp << 8) | *(buffer);
  *value = tmp;
}

void TcrMessage::readInt(uint8_t* buffer, uint32_t* value) {
  uint32_t tmp = *(buffer++);
  tmp = (tmp << 8) | *(buffer++);
  tmp = (tmp << 8) | *(buffer++);
  tmp = (tmp << 8) | *(buffer++);
  *value = tmp;
}

void TcrMessage::writeBytesOnly(const SerializablePtr& se) {
  uint32_t cBufferLength = m_request->getBufferLength();
  uint8_t* startBytes = NULL;
  m_request->writeObject(se);
  uint8_t* cursor =
      const_cast<uint8_t*>(m_request->getBuffer()) + cBufferLength;

  int pos = 1;  // one byte for typeid

  uint8_t code;
  code = cursor[pos++];

  if (code == 0xFF) {
    m_request->rewindCursor(2);
  } else {
    int32_t result = code;
    if (result > 252) {  // 252 is java's ((byte)-4 && 0xFF)
      if (code == 0xFE) {
        uint16_t val;
        readInt(cursor + pos, &val);
        startBytes = cursor + 4;
        result = val;
        m_request->rewindCursor(4);
      } else if (code == 0xFD) {
        uint32_t val;
        readInt(cursor + pos, &val);
        startBytes = cursor + 6;
        result = val;
        m_request->rewindCursor(6);
      }
    } else {
      startBytes = cursor + 2;
      m_request->rewindCursor(2);
    }
    for (int i = 0; i < result; i++) cursor[i] = startBytes[i];
  }
}

void TcrMessage::writeHeader(uint32_t msgType, uint32_t numOfParts) {
  m_request->setPoolName(getPoolName());

  int8_t earlyAck = 0x0;
  LOGDEBUG("TcrMessage::writeHeader m_isMetaRegion = %d", m_isMetaRegion);
  if (m_tcdm != NULL) {
    if ((isSecurityOn =
             (m_tcdm->isSecurityOn() &&
              TcrMessage::isUserInitiativeOps(*this) && !m_isMetaRegion))) {
      earlyAck |= 0x2;
    }
  }

  LOGDEBUG("TcrMessage::writeHeader earlyAck = %d", earlyAck);

  m_request->writeInt(static_cast<int32_t>(msgType));
  m_request->writeInt((int32_t)0);  // write a dummy message len('0' here). At
                                    // the end write the length at the (buffer +
                                    // 4) offset.
  m_request->writeInt(static_cast<int32_t>(numOfParts));
  TXState* txState = TSSTXStateWrapper::s_gemfireTSSTXState->getTXState();
  if (txState == NULL) {
    m_txId = -1;
  } else {
    m_txId = txState->getTransactionId()->getId();
  }
  m_request->writeInt(m_txId);

  // updateHeaderForRetry assumes that 16 bytes are written before earlyAck
  // byte. In case,
  // write header definition changes, updateHeaderForRetry should change
  // accordingly.
  m_request->write(earlyAck);
}

// Updates the early ack byte of the message to reflect that it is a retry op
// This function assumes that 16 bytes are written before earlyAck byte. In
// case,
// write header definition changes, this function should change accordingly.
void TcrMessage::updateHeaderForRetry() {
  uint8_t earlyAck = m_request->getValueAtPos(16);
  // set the isRetryBit
  m_request->updateValueAtPos(16, earlyAck | 0x4);
}

void TcrMessage::writeRegionPart(const std::string& regionName) {
  int32_t len = static_cast<int32_t>(regionName.length());
  m_request->writeInt(len);
  m_request->write(static_cast<int8_t>(0));  // isObject = 0
  m_request->writeBytesOnly((int8_t*)regionName.c_str(), len);
}

void TcrMessage::writeStringPart(const std::string& regionName) {
  m_request->writeFullUTF(regionName.c_str());
}

void TcrMessage::writeEventIdPart(int reserveSize,
                                  bool fullValueAfterDeltaFail) {
  EventId eid(true, reserveSize, fullValueAfterDeltaFail);  // set true so we
                                                            // auto-gen next
                                                            // per-thread
                                                            // sequence number
  //  Write EventId threadid and seqno.
  eid.writeIdsData(*m_request);
}

void TcrMessage::writeMessageLength() {
  uint32_t totalLen = m_request->getBufferLength();
  uint32_t msgLen = totalLen - g_headerLen;
  m_request->rewindCursor(
      totalLen -
      4);  // msg len is written after the msg type which is of 4 bytes ...
  m_request->writeInt(static_cast<int32_t>(msgLen));
  m_request->advanceCursor(totalLen - 8);  // after writing 4 bytes for msg len
                                           // you are already 8 bytes ahead from
                                           // the beginning.
}

void TcrMessage::startProcessChunk(ACE_Semaphore& finalizeSema) {
  if (m_msgTypeRequest == TcrMessage::EXECUTECQ_MSG_TYPE ||
      m_msgTypeRequest == TcrMessage::STOPCQ_MSG_TYPE ||
      m_msgTypeRequest == TcrMessage::CLOSECQ_MSG_TYPE ||
      m_msgTypeRequest == TcrMessage::CLOSECLIENTCQS_MSG_TYPE ||
      m_msgTypeRequest == TcrMessage::GETCQSTATS_MSG_TYPE ||
      m_msgTypeRequest == TcrMessage::MONITORCQ_MSG_TYPE) {
    return;
  }
  if (m_chunkedResult == NULL) {
    throw FatalInternalException(
        "TcrMessage::startProcessChunk: null "
        "result processor!");
  }
  switch (m_msgTypeRequest) {
    case TcrMessage::REGISTER_INTEREST:
    case TcrMessage::REGISTER_INTEREST_LIST:
    case TcrMessage::QUERY:
    case TcrMessage::QUERY_WITH_PARAMETERS:
    case TcrMessage::EXECUTE_FUNCTION:
    case TcrMessage::EXECUTE_REGION_FUNCTION:
    case TcrMessage::EXECUTE_REGION_FUNCTION_SINGLE_HOP:
    case TcrMessage::EXECUTECQ_WITH_IR_MSG_TYPE:
    case TcrMessage::GETDURABLECQS_MSG_TYPE:
    case TcrMessage::KEY_SET:
    case TcrMessage::GET_ALL_70:
    case TcrMessage::GET_ALL_WITH_CALLBACK:
    case TcrMessage::PUTALL:
    case TcrMessage::PUT_ALL_WITH_CALLBACK:
    case TcrMessage::REMOVE_ALL: {
      m_chunkedResult->reset();
      break;
    }
    default: {
      LOGERROR(
          "Got unexpected request message type %d while starting to process "
          "response",
          m_msgTypeRequest);
      throw IllegalStateException(
          "Got unexpected request msg type while starting to process response");
    }
  }
  m_chunkedResult->setFinalizeSemaphore(&finalizeSema);
}

bool TcrMessage::isFEAnotherHop() { return m_feAnotherHop; }
void TcrMessage::handleSpecialFECase() {
  LOGDEBUG("handleSpecialFECase1 %d", this->m_isLastChunkAndisSecurityHeader);
  if ((this->m_isLastChunkAndisSecurityHeader & 0x01) == 0x01) {
    LOGDEBUG("handleSpecialFECase2 %d", this->m_isLastChunkAndisSecurityHeader);
    if (!((this->m_isLastChunkAndisSecurityHeader & 0x04) == 0x04)) {
      LOGDEBUG("handleSpecialFECase3 %d",
               this->m_isLastChunkAndisSecurityHeader);
      m_feAnotherHop = true;
    }
  }
}

void TcrMessage::processChunk(const uint8_t* bytes, int32_t len,
                              uint16_t endpointmemId,
                              const uint8_t isLastChunkAndisSecurityHeader) {
  // TODO: see if security header is there
  LOGDEBUG(
      "TcrMessage::processChunk isLastChunkAndisSecurityHeader = %d chunklen = "
      "%d m_msgType = %d",
      isLastChunkAndisSecurityHeader, len, m_msgType);

  this->m_isLastChunkAndisSecurityHeader = isLastChunkAndisSecurityHeader;
  handleSpecialFECase();

  if (m_tcdm == NULL) {
    throw FatalInternalException("TcrMessage::processChunk: null DM!");
  }

  switch (m_msgType) {
    case TcrMessage::REPLY: {
      LOGDEBUG("processChunk - got reply for request %d", m_msgTypeRequest);
      chunkSecurityHeader(1, bytes, len, isLastChunkAndisSecurityHeader);
      break;
    }
    case TcrMessage::RESPONSE: {
      if (m_msgTypeRequest == TcrMessage::EXECUTECQ_MSG_TYPE ||
          m_msgTypeRequest == TcrMessage::STOPCQ_MSG_TYPE ||
          m_msgTypeRequest == TcrMessage::CLOSECQ_MSG_TYPE ||
          m_msgTypeRequest == TcrMessage::CLOSECLIENTCQS_MSG_TYPE ||
          m_msgTypeRequest == TcrMessage::GETCQSTATS_MSG_TYPE ||
          m_msgTypeRequest == TcrMessage::MONITORCQ_MSG_TYPE) {
        LOGDEBUG("processChunk - got CQ response for request %d",
                 m_msgTypeRequest);
        // TODO: do we need to do anything here
        break;
      } else if (m_msgTypeRequest == TcrMessage::PUTALL ||
                 m_msgTypeRequest == TcrMessage::PUT_ALL_WITH_CALLBACK) {
        TcrChunkedContext* chunk = new TcrChunkedContext(
            bytes, len, m_chunkedResult, isLastChunkAndisSecurityHeader);
        m_chunkedResult->setEndpointMemId(endpointmemId);
        m_tcdm->queueChunk(chunk);
        if (bytes == NULL) {
          // last chunk -- wait for processing of all the chunks to complete
          m_chunkedResult->waitFinalize();
          ExceptionPtr ex = m_chunkedResult->getException();
          if (ex != NULLPTR) {
            ex->raise();
          }
        }
        break;
      }
      // fall-through for other cases
    }
    case EXECUTE_REGION_FUNCTION_RESULT:
    case EXECUTE_FUNCTION_RESULT:
    case TcrMessage::CQDATAERROR_MSG_TYPE:  // one part
    case TcrMessage::CQ_EXCEPTION_TYPE:     // one part
    case TcrMessage::RESPONSE_FROM_PRIMARY: {
      if (m_chunkedResult != NULL) {
        LOGDEBUG("tcrmessage in case22 ");
        TcrChunkedContext* chunk = new TcrChunkedContext(
            bytes, len, m_chunkedResult, isLastChunkAndisSecurityHeader);
        m_chunkedResult->setEndpointMemId(endpointmemId);
        m_tcdm->queueChunk(chunk);
        if (bytes == NULL) {
          // last chunk -- wait for processing of all the chunks to complete
          m_chunkedResult->waitFinalize();
          //  Throw any exception during processing here.
          // Do not throw it immediately since we want to read the
          // full data from socket in any case.
          // Notice that TcrChunkedContext::handleChunk stops any
          // further processing as soon as an exception is encountered.
          // This can cause behaviour like partially filled cache in case
          // of populating cache with registerAllKeys(), so that should be
          // documented since rolling that back may not be a good idea either.
          ExceptionPtr& ex = m_chunkedResult->getException();
          if (ex != NULLPTR) {
            ex->raise();
          }
        }
      } else if (TcrMessage::CQ_EXCEPTION_TYPE == m_msgType ||
                 TcrMessage::CQDATAERROR_MSG_TYPE == m_msgType ||
                 TcrMessage::GET_ALL_DATA_ERROR == m_msgType) {
        if (bytes != NULL) {
          chunkSecurityHeader(1, bytes, len, isLastChunkAndisSecurityHeader);
          GF_SAFE_DELETE_ARRAY(bytes);
        }
      }
      break;
    }
    case TcrMessage::REGISTER_INTEREST_DATA_ERROR:  // for  register interest
                                                    // error
    case EXECUTE_FUNCTION_ERROR:
    case EXECUTE_REGION_FUNCTION_ERROR: {
      if (bytes != NULL) {
        // DeleteArray<const uint8_t> delChunk(bytes);
        //  DataInput input(bytes, len);
        // TODO: this not send two part...
        // looks like this is our exception so only one part will come
        // readExceptionPart(input, false);
        // readSecureObjectPart(input, false, true,
        // isLastChunkAndisSecurityHeader );
        chunkSecurityHeader(1, bytes, len, isLastChunkAndisSecurityHeader);
        GF_SAFE_DELETE_ARRAY(bytes);
      }
      break;
    }
    case TcrMessage::EXCEPTION: {
      if (bytes != NULL) {
        DeleteArray<const uint8_t> delChunk(bytes);
        DataInput input(bytes, len);
        readExceptionPart(input, isLastChunkAndisSecurityHeader);
        readSecureObjectPart(input, false, true,
                             isLastChunkAndisSecurityHeader);
      }
      break;
    }
    case TcrMessage::RESPONSE_FROM_SECONDARY: {
      // TODO: how many parts
      chunkSecurityHeader(1, bytes, len, isLastChunkAndisSecurityHeader);
      if (bytes != NULL) {
        DeleteArray<const uint8_t> delChunk(bytes);
        LOGFINEST("processChunk - got response from secondary, ignoring.");
      }
      break;
    }
    case TcrMessage::GET_ALL_DATA_ERROR: {
      chunkSecurityHeader(1, bytes, len, isLastChunkAndisSecurityHeader);
      if (bytes != NULL) {
        GF_SAFE_DELETE_ARRAY(bytes);
      }
      // nothing else to done since this will be taken care of at higher level
      break;
    }
    default: {
      // TODO: how many parts what should we do here
      if (bytes != NULL) {
        GF_SAFE_DELETE_ARRAY(bytes);
      } else {
        LOGWARN(
            "Got unhandled message type %d while processing response, possible "
            "serialization mismatch",
            m_msgType);
        throw MessageException(
            "TcrMessage::processChunk: "
            "got unhandled message type");
      }
      break;
    }
  }
}

const char* TcrMessage::getPoolName() {
  if (m_region != NULL) {
    const PoolPtr& p = (const_cast<Region*>(m_region))->getPool();
    if (p != NULLPTR) {
      return p->getName();
    } else {
      return NULL;
    }
  }
  return NULL;
  /*ThinClientPoolDM* pool = dynamic_cast<ThinClientPoolDM*>(m_tcdm);

  if(pool == NULL)
    return NULL;

  return pool->getName();*/
}

void TcrMessage::chunkSecurityHeader(int skipPart, const uint8_t* bytes,
                                     int32_t len,
                                     uint8_t isLastChunkAndSecurityHeader) {
  LOGDEBUG("TcrMessage::chunkSecurityHeader:: skipParts = %d", skipPart);
  if ((isLastChunkAndSecurityHeader & 0x3) == 0x3) {
    DataInput di(bytes, len);
    skipParts(di, skipPart);
    readSecureObjectPart(di, false, true, isLastChunkAndSecurityHeader);
  }
}

void TcrMessage::handleByteArrayResponse(const char* bytearray, int32_t len,
                                         uint16_t endpointMemId) {
  DataInput input((uint8_t*)bytearray, len);
  // TODO:: this need to make sure that pool is there
  //  if(m_tcdm == NULL)
  //  throw IllegalArgumentException("Pool is NULL in TcrMessage");
  input.setPoolName(getPoolName());
  input.readInt(&m_msgType);
  int32_t msglen;
  input.readInt(&msglen);
  int32_t numparts;
  input.readInt(&numparts);
  input.readInt(&m_txId);
  int8_t earlyack;
  input.read(&earlyack);
  LOGDEBUG(
      "handleByteArrayResponse m_msgType = %d isSecurityOn = %d requesttype "
      "=%d",
      m_msgType, isSecurityOn, m_msgTypeRequest);
  LOGDEBUG(
      "Message type=%d, length=%d, parts=%d, txid=%d and eack %d with data "
      "length=%d",
      m_msgType, msglen, numparts, m_txId, earlyack, len);

  // LOGFINE("Message type=%d, length=%d, parts=%d, txid=%d and eack %d with
  // data length=%d",
  // m_msgType, msglen, numparts, m_txId, earlyack, len);

  switch (m_msgType) {
    case TcrMessage::RESPONSE: {
      if (m_msgTypeRequest == TcrMessage::CONTAINS_KEY) {
        readBooleanPartAsObject(input, &m_boolValue);
      } else if (m_msgTypeRequest == TcrMessage::USER_CREDENTIAL_MESSAGE) {
        readUniqueIDObjectPart(input);
      } else if (m_msgTypeRequest == TcrMessage::GET_PDX_ID_FOR_TYPE ||
                 m_msgTypeRequest == TcrMessage::GET_PDX_ID_FOR_ENUM) {
        // int will come in response
        uint32_t typeId;
        readIntPart(input, &typeId);
        m_value = CacheableInt32::create(typeId);
      } else if (m_msgTypeRequest == TcrMessage::GET_PDX_TYPE_BY_ID) {
        // PdxType will come in response
        input.advanceCursor(5);  // part header
        m_value =
            SerializationRegistry::deserialize(input, GemfireTypeIds::PdxType);
      } else if (m_msgTypeRequest == TcrMessage::GET_PDX_ENUM_BY_ID) {
        // PdxType will come in response
        input.advanceCursor(5);  // part header
        m_value = SerializationRegistry::deserialize(input);
      } else if (m_msgTypeRequest == TcrMessage::GET_FUNCTION_ATTRIBUTES) {
        int32_t lenObj;
        input.readInt(&lenObj);
        int8_t isObj;
        input.read(&isObj);
        int8_t hR;
        input.read(&hR);
        int8_t isHA;
        input.read(&isHA);
        int8_t oFW;
        input.read(&oFW);
        m_functionAttributes = new std::vector<int8>();
        m_functionAttributes->push_back(hR);
        m_functionAttributes->push_back(isHA);
        m_functionAttributes->push_back(oFW);
      } else if (m_msgTypeRequest == TcrMessage::REQUEST) {
        int32_t receivednumparts = 2;
        readObjectPart(input);
        uint32_t flag = 0;
        readIntPart(input, &flag);
        if (flag & 0x01) {
          readCallbackObjectPart(input);
          receivednumparts++;
        }

        if ((m_value == NULLPTR) && (flag & 0x08 /*VALUE_IS_INVALID*/)) {
          m_value = CacheableToken::invalid();
        }

        if (flag & 0x02) {
          readVersionTag(input, endpointMemId);
          receivednumparts++;
        }

        if (flag & 0x04 /*KEY_NOT_PRESENT*/) {
          m_value = CacheableToken::tombstone();
        }

        if (numparts > receivednumparts) readPrMetaData(input);

      } else if (m_decodeAll) {
        readObjectPart(input);
        if (numparts == 2) {
          if (m_isCallBackArguement) {
            readCallbackObjectPart(input);
          } else {
            int32_t lenObj;
            input.readInt(&lenObj);
            bool isObj;
            input.readBoolean(&isObj);
            input.read(&m_metaDataVersion);
            if (lenObj == 2) {
              input.read(&m_serverGroupVersion);
              LOGDEBUG(
                  "Single-hop m_serverGroupVersion in message response is %d",
                  m_serverGroupVersion);
            }
          }
        } else if (numparts > 2) {
          skipParts(input, 1);
          int32_t lenObj;
          input.readInt(&lenObj);
          bool isObj;
          input.readBoolean(&isObj);
          input.read(&m_metaDataVersion);
          LOGFINE("Single-hop metadata version in message response is %d",
                  m_metaDataVersion);
          if (lenObj == 2) {
            input.read(&m_serverGroupVersion);
            LOGDEBUG(
                "Single-hop m_serverGroupVersion in message response is %d",
                m_serverGroupVersion);
          }
        }
      }
      break;
    }

    case TcrMessage::EXCEPTION: {
      uint8_t lastChunk = static_cast<uint8_t>(numparts);
      lastChunk = (lastChunk << 5);
      readExceptionPart(input, lastChunk);
      // if (isSecurityOn)
      // readSecureObjectPart( input );
      break;
    }

    case TcrMessage::INVALID: {
      // Read the string in the reply
      LOGWARN("Received invalid message type as reply from server");
      readObjectPart(input, true);
      break;
    }

    case TcrMessage::CLIENT_REGISTER_INTEREST:
    case TcrMessage::CLIENT_UNREGISTER_INTEREST:
    case TcrMessage::SERVER_TO_CLIENT_PING:
    case TcrMessage::REGISTER_INSTANTIATORS: {
      // ignore this
      m_shouldIgnore = true;
      break;
    }

    case TcrMessage::REGISTER_INTEREST_DATA_ERROR:
    case TcrMessage::UNREGISTER_INTEREST_DATA_ERROR:
    case TcrMessage::PUT_DATA_ERROR:
    case TcrMessage::KEY_SET_DATA_ERROR:
    case TcrMessage::REQUEST_DATA_ERROR:
    case TcrMessage::DESTROY_REGION_DATA_ERROR:
    case TcrMessage::CLEAR_REGION_DATA_ERROR:
    case TcrMessage::CONTAINS_KEY_DATA_ERROR:
    case TcrMessage::PUT_DELTA_ERROR: {
      // do nothing. (?) TODO Do we need to process further.
      m_shouldIgnore = true;
      break;
    }

    case TcrMessage::REPLY: {
      switch (m_msgTypeRequest) {
        case TcrMessage::PUT: {
          readPrMetaData(input);
          uint32_t flags = 0;
          readIntPart(input, &flags);
          if (flags & 0x01) {  //  has old value
            readOldValue(input);
          }
          if (flags & 0x04) {
            readVersionTag(input, endpointMemId);
          }
          break;
        }
        case TcrMessage::INVALIDATE: {
          uint32_t flags = 0;
          readIntPart(input, &flags);
          if (flags & 0x01) readVersionTag(input, endpointMemId);
          readPrMetaData(input);

          break;
        }
        case TcrMessage::DESTROY: {
          uint32_t flags = 0;
          readIntPart(input, &flags);
          if (flags & 0x01) readVersionTag(input, endpointMemId);
          readPrMetaData(input);
          // skip the Destroy65.java response entryNotFound int part so
          // that the readSecureObjectPart() call below gets the security part
          // skipParts(input, 1);
          readIntPart(input, &m_entryNotFound);
          LOGDEBUG("Inside TcrMessage::REPLY::DESTROY m_entryNotFound = %d ",
                   m_entryNotFound);
          break;
        }
        case TcrMessage::PING:
        default: {
          readPrMetaData(input);
          break;
        }
      }
      break;
    }
    case TcrMessage::LOCAL_INVALIDATE:
    case TcrMessage::LOCAL_DESTROY: {
      int32_t regionLen;
      input.readInt(&regionLen);
      int8_t isObj;
      input.read(&isObj);
      char* regname = NULL;
      regname = new char[regionLen + 1];
      DeleteArray<char> delRegName(regname);
      input.readBytesOnly(reinterpret_cast<int8_t*>(regname), regionLen);
      regname[regionLen] = '\0';
      m_regionName = regname;

      readKeyPart(input);

      // skipParts(input, 1); // skip callbackarg parts
      readCallbackObjectPart(input);
      readVersionTag(input, endpointMemId);
      readBooleanPartAsObject(input, &m_isInterestListPassed);
      readBooleanPartAsObject(input, &m_hasCqsPart);
      if (m_hasCqsPart) {
        if (m_msgType == TcrMessage::LOCAL_INVALIDATE) {
          readIntPart(input, &m_msgTypeForCq);
        } else {
          m_msgTypeForCq = static_cast<uint32_t>(m_msgType);
        }
        // LOGINFO("got cq local local_invalidate/local_destroy read
        // m_hasCqsPart");
        readCqsPart(input);
      }

      // read eventid part
      readEventIdPart(input, false);

      break;
    }

    case TcrMessage::LOCAL_CREATE:
    case TcrMessage::LOCAL_UPDATE: {
      int32_t regionLen;
      input.readInt(&regionLen);
      int8_t isObj;
      input.read(&isObj);
      char* regname = NULL;
      regname = new char[regionLen + 1];
      DeleteArray<char> delRegName(regname);
      input.readBytesOnly(reinterpret_cast<int8_t*>(regname), regionLen);
      regname[regionLen] = '\0';
      m_regionName = regname;

      readKeyPart(input);
      //  Read delta flag
      bool isDelta = false;
      readBooleanPartAsObject(input, &isDelta);
      if (isDelta) {
        input.readInt(&m_deltaBytesLen);

        int8_t isObj;
        input.read(&isObj);
        m_deltaBytes = new uint8_t[m_deltaBytesLen];
        input.readBytesOnly(m_deltaBytes, m_deltaBytesLen);
        m_delta = new DataInput(m_deltaBytes, m_deltaBytesLen);
      } else {
        readObjectPart(input);
      }

      // skip callbackarg part
      // skipParts(input, 1);
      readCallbackObjectPart(input);
      readVersionTag(input, endpointMemId);
      readBooleanPartAsObject(input, &m_isInterestListPassed);
      readBooleanPartAsObject(input, &m_hasCqsPart);

      if (m_hasCqsPart) {
        // LOGINFO("got cq local_create/local_create");
        readCqsPart(input);
        m_msgTypeForCq = static_cast<uint32_t>(m_msgType);
      }

      // read eventid part
      readEventIdPart(input, false);
      GF_SAFE_DELETE_ARRAY(regname);  // COVERITY ---> 30299 Resource leak

      break;
    }
    case TcrMessage::CLIENT_MARKER: {
      // dont skip (non-existent) callbackarg part, just read eventid part
      readEventIdPart(input, false);
      break;
    }

    case TcrMessage::LOCAL_DESTROY_REGION:
    case TcrMessage::CLEAR_REGION: {
      int32_t regionLen;
      input.readInt(&regionLen);
      int8_t isObj;
      input.read(&isObj);
      char* regname = NULL;
      regname = new char[regionLen + 1];
      DeleteArray<char> delRegName(regname);
      input.readBytesOnly(reinterpret_cast<int8_t*>(regname), regionLen);
      regname[regionLen] = '\0';
      m_regionName = regname;
      // skip callbackarg part
      // skipParts(input, 1);
      readCallbackObjectPart(input);
      readBooleanPartAsObject(input, &m_hasCqsPart);
      if (m_hasCqsPart) {
        // LOGINFO("got cq region_destroy read m_hasCqsPart");
        readCqsPart(input);
      }
      // read eventid part
      readEventIdPart(input, false);
      break;
    }

    case TcrMessage::RESPONSE_CLIENT_PR_METADATA: {
      if (len == 17) {
        LOGDEBUG("RESPONSE_CLIENT_PR_METADATA len is 17");
        return;
      }
      m_metadata = new std::vector<std::vector<BucketServerLocationPtr> >();
      for (int32_t i = 0; i < numparts; i++) {
        int32_t bits32;
        input.readInt(&bits32);  // partlen;
        int8_t bits8;
        input.read(&bits8);  // isObj;
        input.read(&bits8);  // cacheable vector typeid
        LOGDEBUG("Expected typeID %d, got %d",
                 GemfireTypeIds::CacheableArrayList, bits8);

        input.readArrayLen(&bits32);  // array length
        LOGDEBUG("Array length = %d ", bits32);
        if (bits32 > 0) {
          std::vector<BucketServerLocationPtr> bucketServerLocations;
          for (int32_t index = 0; index < bits32; index++) {
            int8_t header;
            input.read(&header);  // ignore DS typeid
            input.read(&header);  // ignore CLASS typeid
            input.read(&header);  // ignore string typeid
            uint16_t classLen;
            input.readInt(&classLen);  // Read classLen
            input.advanceCursor(classLen);
            BucketServerLocationPtr location(new BucketServerLocation());
            location->fromData(input);
            LOGFINE("location contains %d\t%s\t%d\t%d\t%s",
                    location->getBucketId(), location->getServerName().c_str(),
                    location->getPort(), location->getVersion(),
                    (location->isPrimary() ? "true" : "false"));
            bucketServerLocations.push_back(location);
          }
          m_metadata->push_back(bucketServerLocations);
        }
        LOGFINER("Metadata size is %d", m_metadata->size());
      }
      break;
    }

    case TcrMessage::GET_CLIENT_PR_METADATA_ERROR: {
      LOGERROR("Failed to get single-hop meta data");
      break;
    }

    case TcrMessage::RESPONSE_CLIENT_PARTITION_ATTRIBUTES: {
      int32_t bits32;
      input.readInt(&bits32);  // partlen;
      int8_t bits8;
      input.read(&bits8);  // isObj;

      m_bucketCount = input.readNativeInt32();  // PART1 = bucketCount

      input.readInt(&bits32);  // partlen;
      input.read(&bits8);      // isObj;
      if (bits32 > 0) {
        input.readNativeString(m_colocatedWith);  // PART2 = colocatedwith
      }

      if (numparts == 4) {
        input.readInt(&bits32);  // partlen;
        input.read(&bits8);      // isObj;
        if (bits32 > 0) {
          input.readNativeString(
              m_partitionResolverName);  // PART3 = partitionresolvername
        }

        input.readInt(&bits32);  // partlen;
        input.read(&bits8);      // isObj;
        input.read(&bits8);      // cacheable CacheableHashSet typeid

        input.readArrayLen(&bits32);  // array length
        if (bits32 > 0) {
          m_fpaSet = new std::vector<FixedPartitionAttributesImplPtr>();
          for (int32_t index = 0; index < bits32; index++) {
            int8_t header;
            input.read(&header);  // ignore DS typeid
            input.read(&header);  // ignore CLASS typeid
            input.read(&header);  // ignore string typeid
            uint16_t classLen;
            input.readInt(&classLen);  // Read classLen
            input.advanceCursor(classLen);
            FixedPartitionAttributesImplPtr fpa(
                new FixedPartitionAttributesImpl());
            fpa->fromData(input);  // PART4 = set of FixedAttributes.
            LOGDEBUG("fpa contains %d\t%s\t%d\t%d", fpa->getNumBuckets(),
                     fpa->getPartitionName().c_str(), fpa->isPrimary(),
                     fpa->getStartingBucketID());
            m_fpaSet->push_back(fpa);
          }
        }
      }
      break;
    }
    case TcrMessage::TOMBSTONE_OPERATION: {
      uint32_t tombstoneOpType;
      int32_t regionLen;
      input.readInt(&regionLen);
      int8_t isObj;
      input.read(&isObj);
      char* regname = NULL;

      regname = new char[regionLen + 1];
      DeleteArray<char> delRegName(regname);
      input.readBytesOnly(reinterpret_cast<int8_t*>(regname), regionLen);
      regname[regionLen] = '\0';
      m_regionName = regname;
      readIntPart(input, &tombstoneOpType);  // partlen;
      int32_t len;
      input.readInt(&len);
      input.read(&isObj);

      if (tombstoneOpType == 0) {
        if (m_tombstoneVersions == NULLPTR) {
          m_tombstoneVersions = CacheableHashMap::create();
        }
        readHashMapForGCVersions(input, m_tombstoneVersions);
      } else if (tombstoneOpType == 1) {
        if (m_tombstoneKeys == NULLPTR) {
          m_tombstoneKeys = CacheableHashSet::create();
        }
        // input.readObject(m_tombstoneKeys);
        readHashSetForGCVersions(input, m_tombstoneKeys);
      } else {
        LOGERROR("Failed to read the tombstone versions");
        break;
      }
      // readEventId Part
      readEventIdPart(input, false);
      break;
    }
    case TcrMessage::GET_CLIENT_PARTITION_ATTRIBUTES_ERROR: {
      LOGERROR("Failed to get server partitioned region attributes");
      break;
    }

    case TcrMessage::UNKNOWN_MESSAGE_TYPE_ERROR: {
      // do nothing
      break;
    }

    case TcrMessage::REQUEST_EVENT_VALUE_ERROR: {
      LOGERROR("Error while requesting full value for delta");
      break;
    }

    default:
      LOGERROR(
          "Unknown message type %d in response, possible serialization "
          "mismatch",
          m_msgType);
      StackTrace st;
      st.print();
      throw MessageException("handleByteArrayResponse: unknown message type");
  }
  LOGDEBUG("handleByteArrayResponse earlyack = %d ", earlyack);
  if (earlyack & 0x2) readSecureObjectPart(input);
}

TcrMessageDestroyRegion::TcrMessageDestroyRegion(
    const Region* region, const UserDataPtr& aCallbackArgument,
    int messageResponsetimeout, ThinClientBaseDM* connectionDM) {
  m_msgType = TcrMessage::DESTROY_REGION;
  m_tcdm = connectionDM;
  m_regionName = region == NULL ? "INVALID_REGION_NAME" : region->getFullPath();
  m_region = region;
  m_timeout = DEFAULT_TIMEOUT_SECONDS;
  m_messageResponseTimeout = messageResponsetimeout;

  uint32_t numOfParts = 1;
  if (aCallbackArgument != NULLPTR) {
    ++numOfParts;
  }

  numOfParts++;

  if (m_messageResponseTimeout != -1) numOfParts++;
  writeHeader(m_msgType, numOfParts);
  writeRegionPart(m_regionName);
  writeEventIdPart();
  if (aCallbackArgument != NULLPTR) {
    writeObjectPart(aCallbackArgument);
  }
  if (m_messageResponseTimeout != -1) {
    writeIntPart(m_messageResponseTimeout);
  }

  writeMessageLength();
}

TcrMessageClearRegion::TcrMessageClearRegion(
    const Region* region, const UserDataPtr& aCallbackArgument,
    int messageResponsetimeout, ThinClientBaseDM* connectionDM) {
  m_msgType = TcrMessage::CLEAR_REGION;
  m_tcdm = connectionDM;
  m_regionName = region == NULL ? "INVALID_REGION_NAME" : region->getFullPath();
  m_region = region;
  m_timeout = DEFAULT_TIMEOUT_SECONDS;
  m_messageResponseTimeout = messageResponsetimeout;

  isSecurityOn = false;
  m_isSecurityHeaderAdded = false;

  uint32_t numOfParts = 1;
  if (aCallbackArgument != NULLPTR) {
    ++numOfParts;
  }

  numOfParts++;

  if (m_messageResponseTimeout != -1) numOfParts++;
  writeHeader(m_msgType, numOfParts);
  writeRegionPart(m_regionName);
  writeEventIdPart();
  if (aCallbackArgument != NULLPTR) {
    writeObjectPart(aCallbackArgument);
  }
  if (m_messageResponseTimeout != -1) {
    writeIntPart(m_messageResponseTimeout);
  }

  writeMessageLength();
}

TcrMessageQuery::TcrMessageQuery(const std::string& regionName,
                                 int messageResponsetimeout,
                                 ThinClientBaseDM* connectionDM) {
  m_request = new DataOutput;
  m_msgType = TcrMessage::QUERY;
  m_tcdm = connectionDM;
  m_regionName = regionName;  // this is querystri;
  m_timeout = DEFAULT_TIMEOUT_SECONDS;
  m_messageResponseTimeout = messageResponsetimeout;
  m_region = NULL;
  uint32_t numOfParts = 1;

  numOfParts++;

  if (m_messageResponseTimeout != -1) numOfParts++;
  writeHeader(m_msgType, numOfParts);
  writeRegionPart(m_regionName);
  writeEventIdPart();
  if (m_messageResponseTimeout != -1) {
    writeIntPart(m_messageResponseTimeout);
  }
  writeMessageLength();
}

TcrMessageStopCQ::TcrMessageStopCQ(const std::string& regionName,
                                   int messageResponsetimeout,
                                   ThinClientBaseDM* connectionDM) {
  m_msgType = TcrMessage::STOPCQ_MSG_TYPE;
  m_tcdm = connectionDM;
  m_regionName = regionName;  // this is querystring
  m_timeout = DEFAULT_TIMEOUT_SECONDS;
  m_messageResponseTimeout = messageResponsetimeout;
  m_region = NULL;
  m_isSecurityHeaderAdded = false;
  m_isMetaRegion = false;

  uint32_t numOfParts = 1;

  numOfParts++;

  if (m_messageResponseTimeout != -1) numOfParts++;
  writeHeader(m_msgType, numOfParts);
  writeRegionPart(m_regionName);
  writeEventIdPart();
  if (m_messageResponseTimeout != -1) {
    writeIntPart(m_messageResponseTimeout);
  }
  writeMessageLength();
}

TcrMessageCloseCQ::TcrMessageCloseCQ(const std::string& regionName,
                                     int messageResponsetimeout,
                                     ThinClientBaseDM* connectionDM) {
  m_msgType = TcrMessage::CLOSECQ_MSG_TYPE;
  m_tcdm = connectionDM;
  m_regionName = regionName;  // this is querystring
  m_timeout = DEFAULT_TIMEOUT_SECONDS;
  m_messageResponseTimeout = messageResponsetimeout;
  m_region = NULL;
  uint32_t numOfParts = 1;

  numOfParts++;

  if (m_messageResponseTimeout != -1) numOfParts++;
  writeHeader(m_msgType, numOfParts);
  writeRegionPart(m_regionName);
  writeEventIdPart();
  if (m_messageResponseTimeout != -1) {
    writeIntPart(m_messageResponseTimeout);
  }
  writeMessageLength();
}

TcrMessageQueryWithParameters::TcrMessageQueryWithParameters(
    const std::string& regionName, const UserDataPtr& aCallbackArgument,
    CacheableVectorPtr paramList, int messageResponsetimeout,
    ThinClientBaseDM* connectionDM) {
  m_msgType = TcrMessage::QUERY_WITH_PARAMETERS;
  m_tcdm = connectionDM;
  m_regionName = regionName;
  m_timeout = DEFAULT_TIMEOUT_SECONDS;
  m_messageResponseTimeout = messageResponsetimeout;
  m_region = NULL;

  // Find out the numOfParts
  uint32_t numOfParts = 4 + paramList->size();
  writeHeader(m_msgType, numOfParts);
  // Part-1: Query String
  writeRegionPart(m_regionName);

  // Part-2: Number or length of the parameters
  writeIntPart(paramList->size());

  // Part-3: X (COMPILE_QUERY_CLEAR_TIMEOUT) parameter
  writeIntPart(15);

  // Part-4: Request specific timeout
  if (m_messageResponseTimeout != -1) {
    writeIntPart(m_messageResponseTimeout);
  }
  // Part-5: Parameters
  if (paramList != NULLPTR) {
    for (int32_t i = 0; i < paramList->size(); i++) {
      CacheablePtr value = (*paramList)[i];
      writeObjectPart(value);
    }
  }
  writeMessageLength();
}

TcrMessageContainsKey::TcrMessageContainsKey(
    const Region* region, const CacheableKeyPtr& key,
    const UserDataPtr& aCallbackArgument, bool isContainsKey,
    ThinClientBaseDM* connectionDM) {
  m_msgType = TcrMessage::CONTAINS_KEY;
  m_tcdm = connectionDM;
  m_regionName = region == NULL ? "INVALID_REGION_NAME" : region->getFullPath();
  m_region = region;
  m_timeout = DEFAULT_TIMEOUT_SECONDS;

  uint32_t numOfParts = 2;
  if (aCallbackArgument != NULLPTR) {
    ++numOfParts;
  }

  numOfParts++;

  if (key == NULLPTR) {
    delete m_request;
    throw IllegalArgumentException(
        "key passed to the constructor can't be NULL");
  }

  writeHeader(m_msgType, numOfParts);
  writeRegionPart(m_regionName);
  writeObjectPart(key);
  // write 0 to indicate containskey (1 for containsvalueforkey)
  writeIntPart(isContainsKey ? 0 : 1);
  if (aCallbackArgument != NULLPTR) {
    writeObjectPart(aCallbackArgument);
  }
  writeMessageLength();
}

TcrMessageGetDurableCqs::TcrMessageGetDurableCqs(
    ThinClientBaseDM* connectionDM) {
  m_msgType = TcrMessage::GETDURABLECQS_MSG_TYPE;
  m_tcdm = connectionDM;
  m_timeout = DEFAULT_TIMEOUT_SECONDS;
  m_region = NULL;
  // wrirting msgtype with part length =1
  writeHeader(m_msgType, 1);
  // the server expects at least 1 part, so writing a dummy byte part
  writeBytePart(0);
  writeMessageLength();
}

TcrMessageRequest::TcrMessageRequest(const Region* region,
                                     const CacheableKeyPtr& key,
                                     const UserDataPtr& aCallbackArgument,
                                     ThinClientBaseDM* connectionDM) {
  m_msgType = TcrMessage::REQUEST;
  m_tcdm = connectionDM;
  m_key = key;
  m_regionName =
      (region == NULL ? "INVALID_REGION_NAME" : region->getFullPath());
  m_region = region;
  m_timeout = DEFAULT_TIMEOUT_SECONDS;

  uint32_t numOfParts = 2;
  if (aCallbackArgument != NULLPTR) {
    ++numOfParts;
  }

  numOfParts++;

  if (key == NULLPTR) {
    delete m_request;
    throw IllegalArgumentException(
        "key passed to the constructor can't be NULL");
  }

  numOfParts--;  // no event id for request
  writeHeader(TcrMessage::REQUEST, numOfParts);
  writeRegionPart(m_regionName);
  writeObjectPart(key);
  if (aCallbackArgument != NULLPTR) {
    // set bool variable to true.
    m_isCallBackArguement = true;
    writeObjectPart(aCallbackArgument);
  }
  writeMessageLength();
}

TcrMessageInvalidate::TcrMessageInvalidate(const Region* region,
                                           const CacheableKeyPtr& key,
                                           const UserDataPtr& aCallbackArgument,
                                           ThinClientBaseDM* connectionDM) {
  m_msgType = TcrMessage::INVALIDATE;
  m_tcdm = connectionDM;
  m_key = key;
  m_regionName =
      (region == NULL ? "INVALID_REGION_NAME" : region->getFullPath());
  m_region = region;
  m_timeout = DEFAULT_TIMEOUT_SECONDS;

  uint32_t numOfParts = 2;
  if (aCallbackArgument != NULLPTR) {
    ++numOfParts;
  }

  numOfParts++;

  if (key == NULLPTR) {
    delete m_request;
    throw IllegalArgumentException(
        "key passed to the constructor can't be NULL");
  }

  writeHeader(TcrMessage::INVALIDATE, numOfParts);
  writeRegionPart(m_regionName);
  writeObjectPart(key);
  writeEventIdPart();
  if (aCallbackArgument != NULLPTR) {
    // set bool variable to true.
    m_isCallBackArguement = true;
    writeObjectPart(aCallbackArgument);
  }
  writeMessageLength();
}

TcrMessageDestroy::TcrMessageDestroy(const Region* region,
                                     const CacheableKeyPtr& key,
                                     const CacheablePtr& value,
                                     const UserDataPtr& aCallbackArgument,
                                     ThinClientBaseDM* connectionDM) {
  m_msgType = TcrMessage::DESTROY;
  m_tcdm = connectionDM;
  m_key = key;
  m_regionName =
      (region == NULL ? "INVALID_REGION_NAME" : region->getFullPath());
  m_region = region;
  m_timeout = DEFAULT_TIMEOUT_SECONDS;
  uint32_t numOfParts = 2;
  if (aCallbackArgument != NULLPTR) {
    ++numOfParts;
  }

  numOfParts++;

  if (key == NULLPTR) {
    delete m_request;
    throw IllegalArgumentException(
        "key passed to the constructor can't be NULL");
  }

  if (value != NULLPTR) {
    numOfParts += 2;  // for GFE Destroy65.java
    writeHeader(TcrMessage::DESTROY, numOfParts);
    writeRegionPart(m_regionName);
    writeObjectPart(key);
    writeObjectPart(value);  // expectedOldValue part
    uint8_t removeByte = 8;  // OP_TYPE_DESTROY value from Operation.java
    CacheableBytePtr removeBytePart = CacheableByte::create(removeByte);
    writeObjectPart(removeBytePart);  // operation part
    writeEventIdPart();
    if (aCallbackArgument != NULLPTR) {
      writeObjectPart(aCallbackArgument);
    }
    writeMessageLength();
  } else {
    numOfParts += 2;  // for GFE Destroy65.java
    writeHeader(TcrMessage::DESTROY, numOfParts);
    writeRegionPart(m_regionName);
    writeObjectPart(key);
    writeObjectPart(NULLPTR);  // expectedOldValue part
    writeObjectPart(NULLPTR);  // operation part
    writeEventIdPart();
    if (aCallbackArgument != NULLPTR) {
      writeObjectPart(aCallbackArgument);
    }
    writeMessageLength();
  }
}

TcrMessagePut::TcrMessagePut(const Region* region, const CacheableKeyPtr& key,
                             const CacheablePtr& value,
                             const UserDataPtr& aCallbackArgument, bool isDelta,
                             ThinClientBaseDM* connectionDM, bool isMetaRegion,
                             bool fullValueAfterDeltaFail,
                             const char* regionName) {
  // m_securityHeaderLength = 0;
  m_isMetaRegion = isMetaRegion;
  m_msgType = TcrMessage::PUT;
  m_tcdm = connectionDM;
  m_key = key;
  m_regionName = region != NULL ? region->getFullPath() : regionName;
  m_region = region;
  m_timeout = DEFAULT_TIMEOUT_SECONDS;

  // TODO check the number of parts in this constructor. doubt because in PUT
  // value can be NULL also.
  uint32_t numOfParts = 5;
  if (aCallbackArgument != NULLPTR) {
    ++numOfParts;
  }

  numOfParts++;

  if (key == NULLPTR) {
    delete m_request;
    throw IllegalArgumentException(
        "key passed to the constructor can't be NULL");
  }

  numOfParts++;
  writeHeader(m_msgType, numOfParts);
  writeRegionPart(m_regionName);
  writeObjectPart(NULLPTR);  // operation = null
  writeIntPart(0);           // flags = 0
  writeObjectPart(key);
  writeObjectPart(CacheableBoolean::create(isDelta));
  writeObjectPart(value, isDelta);
  writeEventIdPart(0, fullValueAfterDeltaFail);
  if (aCallbackArgument != NULLPTR) {
    writeObjectPart(aCallbackArgument);
  }
  writeMessageLength();
}

TcrMessageReply::TcrMessageReply(bool decodeAll,
                                 ThinClientBaseDM* connectionDM) {
  m_msgType = TcrMessage::INVALID;
  m_decodeAll = decodeAll;
  m_tcdm = connectionDM;

  if (connectionDM != NULL) isSecurityOn = connectionDM->isSecurityOn();
}

TcrMessagePing::TcrMessagePing(bool decodeAll) {
  m_msgType = TcrMessage::PING;
  m_decodeAll = decodeAll;

  m_request->writeInt(m_msgType);
  m_request->writeInt(
      (int32_t)0);  // 17 is fixed message len ...  PING only has a header.
  m_request->writeInt((int32_t)0);  // Number of parts.
  // int32_t txId = TcrMessage::m_transactionId++;
  // Setting the txId to 0 for all ping message as it is not being used on the
  // SERVER side or the
  // client side.
  m_request->writeInt((int32_t)0);
  m_request->write(static_cast<int8_t>(0));  // Early ack is '0'.
  m_msgLength = g_headerLen;
  m_txId = 0;
}

TcrMessageCloseConnection::TcrMessageCloseConnection(bool decodeAll) {
  m_msgType = TcrMessage::CLOSE_CONNECTION;
  m_decodeAll = decodeAll;

  m_request->writeInt(m_msgType);
  m_request->writeInt((int32_t)6);
  m_request->writeInt((int32_t)1);  // Number of parts.
  // int32_t txId = TcrMessage::m_transactionId++;
  m_request->writeInt((int32_t)0);
  m_request->write(static_cast<int8_t>(0));  // Early ack is '0'.
  // last two parts are not used ... setting zero in both the parts.
  m_request->writeInt((int32_t)1);           // len is 1
  m_request->write(static_cast<int8_t>(0));  // is obj is '0'.
  // cast away constness here since we want to modify this
  TcrMessage::m_keepalive = const_cast<uint8_t*>(m_request->getCursor());
  m_request->write(static_cast<int8_t>(0));  // keepalive is '0'.
}

TcrMessageClientMarker::TcrMessageClientMarker(bool decodeAll) {
  m_msgType = TcrMessage::CLIENT_MARKER;
  m_decodeAll = decodeAll;
}

TcrMessageRegisterInterestList::TcrMessageRegisterInterestList(
    const Region* region, const VectorOfCacheableKey& keys, bool isDurable,
    bool isCachingEnabled, bool receiveValues,
    InterestResultPolicy interestPolicy, ThinClientBaseDM* connectionDM) {
  m_msgType = TcrMessage::REGISTER_INTEREST_LIST;
  m_tcdm = connectionDM;
  m_keyList = &keys;
  m_regionName = region == NULL ? "INVALID_REGION_NAME" : region->getFullPath();
  m_region = region;
  m_timeout = DEFAULT_TIMEOUT_SECONDS;
  m_isDurable = isDurable;
  m_receiveValues = receiveValues;

  uint32_t numInItrestList = keys.size();
  GF_R_ASSERT(numInItrestList != 0);
  uint32_t numOfParts = 2 + numInItrestList;

  numOfParts += 2 - numInItrestList;

  numOfParts += 2;
  writeHeader(m_msgType, numOfParts);
  writeRegionPart(m_regionName);
  writeInterestResultPolicyPart(interestPolicy);

  writeBytePart(isDurable ? 1 : 0);  // keepalive
  CacheableArrayListPtr cal(CacheableArrayList::create());

  for (uint32_t i = 0; i < numInItrestList; i++) {
    if (keys[i] == NULLPTR) {
      delete m_request;
      throw IllegalArgumentException(
          "keys in the interest list cannot be NULL");
    }
    cal->push_back(keys[i]);
  }

  writeObjectPart(cal);

  uint8_t bytes[2];
  CacheableBytesPtr byteArr = NULLPTR;
  bytes[0] = receiveValues ? 0 : 1;  // reveive values
  byteArr = CacheableBytes::create(bytes, 1);
  writeObjectPart(byteArr);
  bytes[0] = isCachingEnabled ? 1 : 0;  // region policy
  bytes[1] = 0;                         // serialize values
  byteArr = CacheableBytes::create(bytes, 2);
  writeObjectPart(byteArr);
  writeMessageLength();
  m_interestPolicy = interestPolicy.ordinal;
}

TcrMessageUnregisterInterestList::TcrMessageUnregisterInterestList(
    const Region* region, const VectorOfCacheableKey& keys, bool isDurable,
    bool isCachingEnabled, bool receiveValues,
    InterestResultPolicy interestPolicy, ThinClientBaseDM* connectionDM) {
  m_msgType = TcrMessage::UNREGISTER_INTEREST_LIST;
  m_tcdm = connectionDM;
  m_keyList = &keys;
  m_regionName = region == NULL ? "INVALID_REGION_NAME" : region->getFullPath();
  m_region = region;
  m_timeout = DEFAULT_TIMEOUT_SECONDS;
  m_isDurable = isDurable;
  m_receiveValues = receiveValues;

  uint32_t numInItrestList = keys.size();
  GF_R_ASSERT(numInItrestList != 0);
  uint32_t numOfParts = 2 + numInItrestList;

  numOfParts += 2;
  writeHeader(m_msgType, numOfParts);
  writeRegionPart(m_regionName);
  writeBytePart(0);                  // isClosing
  writeBytePart(isDurable ? 1 : 0);  // keepalive

  writeIntPart(static_cast<int32_t>(numInItrestList));

  for (uint32_t i = 0; i < numInItrestList; i++) {
    if (keys[i] == NULLPTR) {
      delete m_request;
      throw IllegalArgumentException(
          "keys in the interest list cannot be NULL");
    }
    writeObjectPart(keys[i]);
  }

  writeMessageLength();
  m_interestPolicy = interestPolicy.ordinal;
}

TcrMessageCreateRegion::TcrMessageCreateRegion(
    const std::string& str1, const std::string& str2,
    InterestResultPolicy interestPolicy, bool isDurable, bool isCachingEnabled,
    bool receiveValues, ThinClientBaseDM* connectionDM) {
  m_msgType = TcrMessage::CREATE_REGION;
  m_tcdm = connectionDM;
  m_isDurable = isDurable;
  m_receiveValues = receiveValues;

  uint32_t numOfParts = 2;
  writeHeader(m_msgType, numOfParts);
  writeRegionPart(str1);  // parent region name
  writeRegionPart(str2);  // region name
  writeMessageLength();
  m_regionName = str2;
}

TcrMessageRegisterInterest::TcrMessageRegisterInterest(
    const std::string& str1, const std::string& str2,
    InterestResultPolicy interestPolicy, bool isDurable, bool isCachingEnabled,
    bool receiveValues, ThinClientBaseDM* connectionDM) {
  m_msgType = TcrMessage::REGISTER_INTEREST;
  m_tcdm = connectionDM;
  m_isDurable = isDurable;
  m_receiveValues = receiveValues;

  uint32_t numOfParts = 7;

  writeHeader(m_msgType, numOfParts);

  writeRegionPart(str1);                          // region name
  writeIntPart(REGULAR_EXPRESSION);               // InterestType
  writeInterestResultPolicyPart(interestPolicy);  // InterestResultPolicy
  writeBytePart(isDurable ? 1 : 0);
  writeRegionPart(str2);  // regexp string

  uint8_t bytes[2];
  CacheableBytesPtr byteArr = NULLPTR;
  bytes[0] = receiveValues ? 0 : 1;
  byteArr = CacheableBytes::create(bytes, 1);
  writeObjectPart(byteArr);
  bytes[0] = isCachingEnabled ? 1 : 0;  // region data policy
  bytes[1] = 0;                         // serializevalues
  byteArr = CacheableBytes::create(bytes, 2);
  writeObjectPart(byteArr);

  writeMessageLength();
  m_regionName = str1;
  m_regex = str2;
  m_interestPolicy = interestPolicy.ordinal;
}

TcrMessageUnregisterInterest::TcrMessageUnregisterInterest(
    const std::string& str1, const std::string& str2,
    InterestResultPolicy interestPolicy, bool isDurable, bool isCachingEnabled,
    bool receiveValues, ThinClientBaseDM* connectionDM) {
  m_msgType = TcrMessage::UNREGISTER_INTEREST;
  m_tcdm = connectionDM;
  m_isDurable = isDurable;
  m_receiveValues = receiveValues;

  uint32_t numOfParts = 3;
  numOfParts += 2;
  writeHeader(m_msgType, numOfParts);
  writeRegionPart(str1);             // region name
  writeIntPart(REGULAR_EXPRESSION);  // InterestType
  writeRegionPart(str2);             // regexp string
  writeBytePart(0);                  // isClosing
  writeBytePart(isDurable ? 1 : 0);  // keepalive
  writeMessageLength();
  m_regionName = str1;
  m_regex = str2;
  m_interestPolicy = interestPolicy.ordinal;
}

TcrMessageTxSynchronization::TcrMessageTxSynchronization(int ordinal, int txid,
                                                         int status) {
  m_msgType = TcrMessage::TX_SYNCHRONIZATION;

  writeHeader(m_msgType, ordinal == 1 ? 3 : 2);
  writeIntPart(ordinal);
  writeIntPart(txid);
  if (ordinal == 1) {
    writeIntPart(status);
  }

  writeMessageLength();
}

TcrMessageClientReady::TcrMessageClientReady() {
  m_msgType = TcrMessage::CLIENT_READY;

  writeHeader(m_msgType, 1);
  // the server expects at least 1 part, so writing a dummy
  writeBytePart(0);
  writeMessageLength();
}

TcrMessageCommit::TcrMessageCommit() {
  m_msgType = TcrMessage::COMMIT;

  writeHeader(m_msgType, 1);
  // the server expects at least 1 part, so writing a dummy
  writeBytePart(0);
  writeMessageLength();
}

TcrMessageRollback::TcrMessageRollback() {
  m_msgType = TcrMessage::ROLLBACK;

  writeHeader(m_msgType, 1);
  // the server expects at least 1 part, so writing a dummy
  writeBytePart(0);
  writeMessageLength();
}

TcrMessageTxFailover::TcrMessageTxFailover() {
  m_msgType = TcrMessage::TX_FAILOVER;

  writeHeader(m_msgType, 1);
  // the server expects at least 1 part, so writing a dummy
  writeBytePart(0);
  writeMessageLength();
}

// constructor for MAKE_PRIMARY message.
TcrMessageMakePrimary::TcrMessageMakePrimary(bool processedMarker) {
  m_msgType = TcrMessage::MAKE_PRIMARY;

  writeHeader(m_msgType, 1);
  writeBytePart(processedMarker ? 1 : 0);  // boolean processedMarker
  writeMessageLength();
}

// constructor for PERIODIC_ACK of notified eventids
TcrMessagePeriodicAck::TcrMessagePeriodicAck(
    const EventIdMapEntryList& entries) {
  m_msgType = TcrMessage::PERIODIC_ACK;

  uint32_t numParts = static_cast<uint32_t>(entries.size());
  GF_D_ASSERT(numParts > 0);
  writeHeader(m_msgType, numParts);
  for (EventIdMapEntryList::const_iterator entry = entries.begin();
       entry != entries.end(); ++entry) {
    EventSourcePtr src = entry->first;
    EventSequencePtr seq = entry->second;
    EventIdPtr eid = EventId::create(src->getMemId(), src->getMemIdLen(),
                                     src->getThrId(), seq->getSeqNum());
    writeObjectPart(eid);
  }
  writeMessageLength();
}

TcrMessagePutAll::TcrMessagePutAll(const Region* region,
                                   const HashMapOfCacheable& map,
                                   int messageResponsetimeout,
                                   ThinClientBaseDM* connectionDM,
                                   const UserDataPtr& aCallbackArgument) {
  m_tcdm = connectionDM;
  m_regionName = region->getFullPath();
  m_region = region;
  m_messageResponseTimeout = messageResponsetimeout;

  // TODO check the number of parts in this constructor. doubt because in PUT
  // value can be NULL also.
  uint32_t numOfParts = 0;
  // bool skipCallBacks = false;

  if (aCallbackArgument != NULLPTR) {
    m_msgType = TcrMessage::PUT_ALL_WITH_CALLBACK;
    numOfParts = 6 + map.size() * 2;
    // skipCallBacks = false;
  } else {
    m_msgType = TcrMessage::PUTALL;
    numOfParts = 5 + map.size() * 2;
    // skipCallBacks = true;
  }

  // numOfParts++;

  if (m_messageResponseTimeout != -1) numOfParts++;

  writeHeader(m_msgType, numOfParts);
  writeRegionPart(m_regionName);
  writeEventIdPart(map.size() - 1);
  // writeIntPart(skipCallBacks ? 0 : 1);
  writeIntPart(0);

  // Client putAll requests now send a flags int as part #0.  1==region has
  // datapolicy.EMPTY, 2==region has concurrency checks enabled.
  // Version tags are not sent back if dp is EMPTY or concurrency
  // checks are disabled.
  int flags = 0;
  if (!region->getAttributes()->getCachingEnabled()) {
    flags |= TcrMessage::m_flag_empty;
    LOGDEBUG("TcrMessage::PUTALL datapolicy empty flags = %d ", flags);
  }
  if (region->getAttributes()->getConcurrencyChecksEnabled()) {
    flags |= TcrMessage::m_flag_concurrency_checks;
    LOGDEBUG("TcrMessage::PUTALL ConcurrencyChecksEnabled flags = %d ", flags);
  }
  writeIntPart(flags);

  writeIntPart(map.size());

  if (aCallbackArgument != NULLPTR) {
    writeObjectPart(aCallbackArgument);
  }

  for (HashMapOfCacheable::Iterator iter = map.begin(); iter != map.end();
       ++iter) {
    CacheableKeyPtr key = iter.first();
    CacheablePtr value = iter.second();
    writeObjectPart(key);
    writeObjectPart(value);
  }

  if (m_messageResponseTimeout != -1) {
    writeIntPart(m_messageResponseTimeout);
  }
  writeMessageLength();
}

TcrMessageRemoveAll::TcrMessageRemoveAll(const Region* region,
                                         const VectorOfCacheableKey& keys,
                                         const UserDataPtr& aCallbackArgument,
                                         ThinClientBaseDM* connectionDM) {
  m_msgType = TcrMessage::REMOVE_ALL;
  m_tcdm = connectionDM;
  m_regionName = region->getFullPath();
  m_region = region;

  // TODO check the number of parts in this constructor. doubt because in PUT
  // value can be NULL also.
  uint32_t numOfParts = 5 + keys.size();

  if (m_messageResponseTimeout != -1) numOfParts++;
  writeHeader(m_msgType, numOfParts);
  writeRegionPart(m_regionName);
  writeEventIdPart(keys.size() - 1);

  // Client removeall requests now send a flags int as part #0.  1==region has
  // datapolicy.EMPTY, 2==region has concurrency checks enabled.
  // Version tags are not sent back if dp is EMPTY or concurrency
  // checks are disabled.
  int flags = 0;
  if (!region->getAttributes()->getCachingEnabled()) {
    flags |= TcrMessage::m_flag_empty;
    LOGDEBUG("TcrMessage::REMOVE_ALL datapolicy empty flags = %d ", flags);
  }
  if (region->getAttributes()->getConcurrencyChecksEnabled()) {
    flags |= TcrMessage::m_flag_concurrency_checks;
    LOGDEBUG("TcrMessage::REMOVE_ALL ConcurrencyChecksEnabled flags = %d ",
             flags);
  }
  writeIntPart(flags);
  writeObjectPart(aCallbackArgument);
  writeIntPart(keys.size());

  for (VectorOfCacheableKey::Iterator iter = keys.begin(); iter != keys.end();
       ++iter) {
    writeObjectPart(*iter);
  }
  writeMessageLength();
}

TcrMessageUpdateClientNotification::TcrMessageUpdateClientNotification(
    int32_t port) {
  m_msgType = TcrMessage::UPDATE_CLIENT_NOTIFICATION;

  writeHeader(m_msgType, 1);
  writeIntPart(port);
  writeMessageLength();
}

TcrMessageGetAll::TcrMessageGetAll(const Region* region,
                                   const VectorOfCacheableKey* keys,
                                   ThinClientBaseDM* connectionDM,
                                   const UserDataPtr& aCallbackArgument) {
  m_msgType = TcrMessage::GET_ALL_70;
  m_tcdm = connectionDM;
  m_keyList = keys;
  m_callbackArgument = aCallbackArgument;
  m_regionName = region->getFullPath();
  m_region = region;

  /*CacheableObjectArrayPtr keyArr = NULLPTR;
  if (keys != NULL) {
    keyArr = CacheableObjectArray::create();
    for (int32_t index = 0; index < keys->size(); ++index) {
      keyArr->push_back(keys->operator[](index));
    }
  }*/
  if (m_callbackArgument != NULLPTR) {
    m_msgType = TcrMessage::GET_ALL_WITH_CALLBACK;
  } else {
    m_msgType = TcrMessage::GET_ALL_70;
  }

  writeHeader(m_msgType, 3);
  writeRegionPart(m_regionName);
  /*writeHeader(m_msgType, 2);
  writeRegionPart(regionName);
  writeObjectPart(keyArr);
  writeMessageLength();*/
}

void TcrMessage::InitializeGetallMsg(const UserDataPtr& aCallbackArgument) {
  /*CacheableObjectArrayPtr keyArr = NULLPTR;
  if (m_keyList != NULL) {
    keyArr = CacheableObjectArray::create();
    for (int32_t index = 0; index < m_keyList->size(); ++index) {
      keyArr->push_back(m_keyList->operator[](index));
    }
  }*/
  // LOGINFO(" in InitializeGetallMsg %s ", m_regionName.c_str());
  // writeHeader(m_msgType, 2);
  // writeRegionPart(m_regionName);
  writeObjectPart(NULLPTR, false, false, m_keyList);  // will do manually
  if (aCallbackArgument != NULLPTR) {
    writeObjectPart(aCallbackArgument);
  } else {
    writeIntPart(0);
  }
  writeMessageLength();
}

TcrMessageExecuteCq::TcrMessageExecuteCq(const std::string& str1,
                                         const std::string& str2, int state,
                                         bool isDurable,
                                         ThinClientBaseDM* connectionDM) {
  m_msgType = TcrMessage::EXECUTECQ_MSG_TYPE;
  m_tcdm = connectionDM;
  m_isDurable = isDurable;

  uint32_t numOfParts = 5;
  writeHeader(m_msgType, numOfParts);
  writeStringPart(str1);  // cqName
  writeStringPart(str2);  // query string

  writeIntPart(state);  // cq state
  writeBytePart(isDurable ? 1 : 0);
  //  hard-coding region data policy to 1
  // This Part will be removed when server-side changes are made to remove
  // CQ dependency on region data policy. After the changes, set numOfParts
  // to 4 (currently 5).
  writeBytePart(1);
  writeMessageLength();
  m_regionName = str1;
  m_regex = str2;
}

TcrMessageExecuteCqWithIr::TcrMessageExecuteCqWithIr(
    const std::string& str1, const std::string& str2, int state, bool isDurable,
    ThinClientBaseDM* connectionDM) {
  m_msgType = TcrMessage::EXECUTECQ_WITH_IR_MSG_TYPE;
  m_tcdm = connectionDM;
  m_isDurable = isDurable;

  uint32_t numOfParts = 5;
  writeHeader(m_msgType, numOfParts);
  writeStringPart(str1);  // cqName
  writeStringPart(str2);  // query string

  writeIntPart(state);  // cq state
  writeBytePart(isDurable ? 1 : 0);
  //  hard-coding region data policy to 1
  // This Part will be removed when server-side changes are made to remove
  // CQ dependency on region data policy. After the changes, set numOfParts
  // to 4 (currently 5).
  writeBytePart(1);
  writeMessageLength();
  m_regionName = str1;
  m_regex = str2;
}

TcrMessageExecuteFunction::TcrMessageExecuteFunction(
    const std::string& funcName, const CacheablePtr& args, uint8_t getResult,
    ThinClientBaseDM* connectionDM, int32_t timeout) {
  m_msgType = TcrMessage::EXECUTE_FUNCTION;
  m_tcdm = connectionDM;
  m_hasResult = getResult;

  uint32_t numOfParts = 3;
  writeHeader(m_msgType, numOfParts);
  // writeBytePart(getResult ? 1 : 0);
  // if gfcpp property unit set then timeout will be in millisecond
  // otherwise it will be in second
  if ((DistributedSystem::getSystemProperties() != NULL) &&
      (DistributedSystem::getSystemProperties()->readTimeoutUnitInMillis())) {
    writeByteAndTimeOutPart(getResult, timeout);
  } else {
    writeByteAndTimeOutPart(getResult, (timeout * 1000));
  }
  writeRegionPart(funcName);  // function name string
  writeObjectPart(args);
  writeMessageLength();
}

TcrMessageExecuteRegionFunction::TcrMessageExecuteRegionFunction(
    const std::string& funcName, const Region* region, const CacheablePtr& args,
    CacheableVectorPtr routingObj, uint8_t getResult,
    CacheableHashSetPtr failedNodes, int32_t timeout,
    ThinClientBaseDM* connectionDM, int8_t reExecute) {
  m_msgType = TcrMessage::EXECUTE_REGION_FUNCTION;
  m_tcdm = connectionDM;
  m_regionName = region == NULL ? "INVALID_REGION_NAME" : region->getFullPath();
  m_region = region;

  if (routingObj != NULLPTR && routingObj->size() == 1) {
    LOGDEBUG("setting up key");
    m_key = routingObj->at(0);
  }

  uint32_t numOfParts = 6 + (routingObj == NULLPTR ? 0 : routingObj->size());
  numOfParts +=
      2;  // for the FunctionHA isReExecute and removedNodesSize parts.
  if (failedNodes != NULLPTR) {
    numOfParts++;
  }
  writeHeader(m_msgType, numOfParts);

  // if gfcpp property unit set then timeout will be in millisecond
  // otherwise it will be in second
  if ((DistributedSystem::getSystemProperties() != NULL) &&
      (DistributedSystem::getSystemProperties()->readTimeoutUnitInMillis())) {
    writeByteAndTimeOutPart(getResult, timeout);
  } else {
    writeByteAndTimeOutPart(getResult, (timeout * 1000));
  }
  writeRegionPart(m_regionName);
  writeRegionPart(funcName);  // function name string
  writeObjectPart(args);
  // klug for MemberMappedArgs
  writeObjectPart(NULLPTR);
  writeBytePart(reExecute);  // FunctionHA isReExecute = false
  writeIntPart(routingObj == NULLPTR ? 0 : routingObj->size());
  if (routingObj != NULLPTR) {
    for (int32_t i = 0; i < routingObj->size(); i++) {
      CacheablePtr value = routingObj->operator[](i);
      writeObjectPart(value);
    }
  }
  if (failedNodes != NULLPTR) {
    writeIntPart(failedNodes->size());
    writeObjectPart(failedNodes);
  } else {
    writeIntPart(0);  // FunctionHA removedNodesSize = 0
  }
  writeMessageLength();
}

TcrMessageExecuteRegionFunctionSingleHop::
    TcrMessageExecuteRegionFunctionSingleHop(
        const std::string& funcName, const Region* region,
        const CacheablePtr& args, CacheableHashSetPtr routingObj,
        uint8_t getResult, CacheableHashSetPtr failedNodes, bool allBuckets,
        int32_t timeout, ThinClientBaseDM* connectionDM) {
  m_msgType = TcrMessage::EXECUTE_REGION_FUNCTION_SINGLE_HOP;
  m_tcdm = connectionDM;
  m_regionName = region == NULL ? "INVALID_REGION_NAME" : region->getFullPath();
  m_region = region;

  uint32_t numOfParts = 6 + (routingObj == NULLPTR ? 0 : routingObj->size());
  numOfParts +=
      2;  // for the FunctionHA isReExecute and removedNodesSize parts.
  if (failedNodes != NULLPTR) {
    numOfParts++;
  }
  writeHeader(m_msgType, numOfParts);

  // if gfcpp property unit set then timeout will be in millisecond
  // otherwise it will be in second
  if ((DistributedSystem::getSystemProperties() != NULL) &&
      (DistributedSystem::getSystemProperties()->readTimeoutUnitInMillis())) {
    writeByteAndTimeOutPart(getResult, timeout);
  } else {
    writeByteAndTimeOutPart(getResult, (timeout * 1000));
  }
  writeRegionPart(m_regionName);
  writeRegionPart(funcName);  // function name string
  writeObjectPart(args);
  // klug for MemberMappedArgs
  writeObjectPart(NULLPTR);
  writeBytePart(allBuckets ? 1 : 0);
  writeIntPart(routingObj == NULLPTR ? 0 : routingObj->size());
  if (routingObj != NULLPTR) {
    if (allBuckets) {
      LOGDEBUG("All Buckets so putting IntPart for buckets = %d ",
               routingObj->size());
      for (CacheableHashSet::Iterator itr = routingObj->begin();
           itr != routingObj->end(); ++itr) {
        CacheableInt32Ptr value = *itr;
        writeIntPart(value->value());
      }
    } else {
      LOGDEBUG("putting keys as withFilter called, routing Keys size = %d ",
               routingObj->size());
      for (CacheableHashSet::Iterator itr = routingObj->begin();
           itr != routingObj->end(); ++itr) {
        CacheablePtr value = *itr;
        writeObjectPart(value);
      }
    }
  }
  if (failedNodes != NULLPTR) {
    writeIntPart(failedNodes->size());
    writeObjectPart(failedNodes);
  } else {
    writeIntPart(0);  // FunctionHA removedNodesSize = 0
  }
  writeMessageLength();
}

TcrMessageGetClientPartitionAttributes::TcrMessageGetClientPartitionAttributes(
    const char* regionName) {
  m_msgType = TcrMessage::GET_CLIENT_PARTITION_ATTRIBUTES;
  writeHeader(m_msgType, 1);
  writeRegionPart(regionName);
  writeMessageLength();
}

TcrMessageGetClientPrMetadata::TcrMessageGetClientPrMetadata(
    const char* regionName) {
  m_msgType = TcrMessage::GET_CLIENT_PR_METADATA;
  writeHeader(m_msgType, 1);
  writeRegionPart(regionName);
  writeMessageLength();
}

TcrMessageSize::TcrMessageSize(const char* regionName) {
  m_msgType = TcrMessage::SIZE;
  writeHeader(m_msgType, 1);
  writeRegionPart(regionName);
  writeMessageLength();
}

TcrMessageUserCredential::TcrMessageUserCredential(
    PropertiesPtr creds, ThinClientBaseDM* connectionDM) {
  m_msgType = TcrMessage::USER_CREDENTIAL_MESSAGE;
  m_tcdm = connectionDM;

  /*
   * First part will be connection-id ( may be in encrypted form) to avoid
   * replay attack
   * Second part will be credentails (may be in encrypted form)
   */
  m_creds = creds;
  /*LOGDEBUG("Tcrmessage sending creds to server");
  writeHeader(msgType, numOfParts);
  writeObjectPart(creds);
  writeMessageLength();
  LOGDEBUG("TcrMessage addsp = %s ",
  Utils::convertBytesToString(m_request->getBuffer(),
  m_request->getBufferLength())->asChar());*/
}

TcrMessageRemoveUserAuth::TcrMessageRemoveUserAuth(
    bool keepAlive, ThinClientBaseDM* connectionDM) {
  m_msgType = TcrMessage::REMOVE_USER_AUTH;
  m_tcdm = connectionDM;
  LOGDEBUG("Tcrmessage sending REMOVE_USER_AUTH message to server");
  writeHeader(m_msgType, 1);
  // adding dummy part as server has check for numberofparts > 0
  uint8_t dummy = 0;
  if (keepAlive) dummy = 1;
  CacheableBytesPtr cbp = CacheableBytes::create(&dummy, 1);
  writeObjectPart(cbp, false);
  writeMessageLength();
  LOGDEBUG("TcrMessage REMOVE_USER_AUTH = %s ",
           Utils::convertBytesToString(m_request->getBuffer(),
                                       m_request->getBufferLength())
               ->asChar());
}
void TcrMessage::createUserCredentialMessage(TcrConnection* conn) {
  m_request->reset();
  m_isSecurityHeaderAdded = false;
  writeHeader(m_msgType, 1);

  DataOutput dOut;

  if (m_creds != NULLPTR) m_creds->toData(dOut);

  CacheableBytesPtr credBytes =
      CacheableBytes::create(dOut.getBuffer(), dOut.getBufferLength());
  CacheableBytesPtr encryptBytes = conn->encryptBytes(credBytes);
  writeObjectPart(encryptBytes);

  writeMessageLength();
  LOGDEBUG("TcrMessage CUCM() = %s ",
           Utils::convertBytesToString(m_request->getBuffer(),
                                       m_request->getBufferLength())
               ->asChar());
}

void TcrMessage::addSecurityPart(int64_t connectionId, int64_t unique_id,
                                 TcrConnection* conn) {
  LOGDEBUG("TcrMessage::addSecurityPart m_isSecurityHeaderAdded = %d ",
           m_isSecurityHeaderAdded);
  if (m_isSecurityHeaderAdded) {
    m_request->rewindCursor(m_securityHeaderLength);
    writeMessageLength();
    m_securityHeaderLength = 0;
    m_isSecurityHeaderAdded = false;
  }
  m_isSecurityHeaderAdded = true;
  LOGDEBUG("addSecurityPart( , ) ");
  DataOutput dOutput;

  dOutput.writeInt(connectionId);
  dOutput.writeInt(unique_id);

  CacheableBytesPtr bytes =
      CacheableBytes::create(dOutput.getBuffer(), dOutput.getBufferLength());

  CacheableBytesPtr encryptBytes = conn->encryptBytes(bytes);

  writeObjectPart(encryptBytes);
  writeMessageLength();
  m_securityHeaderLength = 4 + 1 + encryptBytes->length();
  LOGDEBUG("TcrMessage addsp = %s ",
           Utils::convertBytesToString(m_request->getBuffer(),
                                       m_request->getBufferLength())
               ->asChar());
}

void TcrMessage::addSecurityPart(int64_t connectionId, TcrConnection* conn) {
  LOGDEBUG("TcrMessage::addSecurityPart m_isSecurityHeaderAdded = %d ",
           m_isSecurityHeaderAdded);
  if (m_isSecurityHeaderAdded) {
    m_request->rewindCursor(m_securityHeaderLength);
    writeMessageLength();
    m_securityHeaderLength = 0;
    m_isSecurityHeaderAdded = false;
  }
  m_isSecurityHeaderAdded = true;
  LOGDEBUG("TcrMessage::addSecurityPart only connid");
  DataOutput dOutput;

  dOutput.writeInt(connectionId);

  CacheableBytesPtr bytes =
      CacheableBytes::create(dOutput.getBuffer(), dOutput.getBufferLength());

  CacheableBytesPtr encryptBytes = conn->encryptBytes(bytes);

  writeObjectPart(encryptBytes);
  writeMessageLength();
  m_securityHeaderLength = 4 + 1 + encryptBytes->length();
  LOGDEBUG("TcrMessage addspCC = %s ",
           Utils::convertBytesToString(m_request->getBuffer(),
                                       m_request->getBufferLength())
               ->asChar());
}

TcrMessageRequestEventValue::TcrMessageRequestEventValue(EventIdPtr eventId) {
  m_msgType = TcrMessage::REQUEST_EVENT_VALUE;

  uint32_t numOfParts = 1;
  writeHeader(m_msgType, numOfParts);
  writeObjectPart(eventId);
  writeMessageLength();
}

TcrMessageGetPdxIdForType::TcrMessageGetPdxIdForType(
    const CacheablePtr& pdxType, ThinClientBaseDM* connectionDM,
    int32_t pdxTypeId) {
  m_msgType = TcrMessage::GET_PDX_ID_FOR_TYPE;
  m_tcdm = connectionDM;

  LOGDEBUG("Tcrmessage sending GET_PDX_ID_FOR_TYPE message to server");
  writeHeader(m_msgType, 1);
  writeObjectPart(pdxType, false, true);
  writeMessageLength();
  LOGDEBUG("TcrMessage GET_PDX_ID_FOR_TYPE = %s ",
           Utils::convertBytesToString(m_request->getBuffer(),
                                       m_request->getBufferLength())
               ->asChar());
}

TcrMessageAddPdxType::TcrMessageAddPdxType(const CacheablePtr& pdxType,
                                           ThinClientBaseDM* connectionDM,
                                           int32_t pdxTypeId) {
  m_msgType = TcrMessage::ADD_PDX_TYPE;
  m_tcdm = connectionDM;

  LOGDEBUG("Tcrmessage sending ADD_PDX_TYPE message to server");
  writeHeader(m_msgType, 2);
  writeObjectPart(pdxType, false, true);
  writeIntPart(pdxTypeId);
  writeMessageLength();
  LOGDEBUG("TcrMessage ADD_PDX_TYPE id = %d = %s ", pdxTypeId,
           Utils::convertBytesToString(m_request->getBuffer(),
                                       m_request->getBufferLength())
               ->asChar());
}

TcrMessageGetPdxIdForEnum::TcrMessageGetPdxIdForEnum(
    const CacheablePtr& pdxType, ThinClientBaseDM* connectionDM,
    int32_t pdxTypeId) {
  m_msgType = TcrMessage::GET_PDX_ID_FOR_ENUM;
  m_tcdm = connectionDM;

  LOGDEBUG("Tcrmessage sending GET_PDX_ID_FOR_ENUM message to server");
  writeHeader(m_msgType, 1);
  writeObjectPart(pdxType, false, false);
  writeMessageLength();
  LOGDEBUG("TcrMessage GET_PDX_ID_FOR_ENUM = %s ",
           Utils::convertBytesToString(m_request->getBuffer(),
                                       m_request->getBufferLength())
               ->asChar());
}

TcrMessageAddPdxEnum::TcrMessageAddPdxEnum(const CacheablePtr& pdxType,
                                           ThinClientBaseDM* connectionDM,
                                           int32_t pdxTypeId) {
  m_msgType = TcrMessage::ADD_PDX_ENUM;
  m_tcdm = connectionDM;

  LOGDEBUG("Tcrmessage sending ADD_PDX_ENUM message to server");
  writeHeader(m_msgType, 2);
  writeObjectPart(pdxType, false, false);
  writeIntPart(pdxTypeId);
  writeMessageLength();
  LOGDEBUG("TcrMessage ADD_PDX_ENUM id = %d = %s ", pdxTypeId,
           Utils::convertBytesToString(m_request->getBuffer(),
                                       m_request->getBufferLength())
               ->asChar());
}

TcrMessageGetPdxTypeById::TcrMessageGetPdxTypeById(
    int32_t typeId, ThinClientBaseDM* connectionDM) {
  m_msgType = TcrMessage::GET_PDX_TYPE_BY_ID;
  m_tcdm = connectionDM;

  LOGDEBUG("Tcrmessage sending GET_PDX_TYPE_BY_ID message to server");
  writeHeader(m_msgType, 1);
  m_request->writeInt(4);
  m_request->writeBoolean(false);
  m_request->writeInt(typeId);
  writeMessageLength();
  LOGDEBUG("TcrMessage GET_PDX_TYPE_BY_ID = %s ",
           Utils::convertBytesToString(m_request->getBuffer(),
                                       m_request->getBufferLength())
               ->asChar());
}

TcrMessageGetPdxEnumById::TcrMessageGetPdxEnumById(
    int32_t typeId, ThinClientBaseDM* connectionDM) {
  m_msgType = TcrMessage::GET_PDX_ENUM_BY_ID;
  m_tcdm = connectionDM;

  LOGDEBUG("Tcrmessage sending GET_PDX_ENUM_BY_ID message to server");
  writeHeader(m_msgType, 1);
  m_request->writeInt(4);
  m_request->writeBoolean(false);
  m_request->writeInt(typeId);
  writeMessageLength();
  LOGDEBUG("TcrMessage GET_PDX_ENUM_BY_ID = %s ",
           Utils::convertBytesToString(m_request->getBuffer(),
                                       m_request->getBufferLength())
               ->asChar());
}

TcrMessageGetFunctionAttributes::TcrMessageGetFunctionAttributes(
    const std::string& funcName, ThinClientBaseDM* connectionDM) {
  m_msgType = TcrMessage::GET_FUNCTION_ATTRIBUTES;
  m_tcdm = connectionDM;

  uint32_t numOfParts = 1;
  writeHeader(m_msgType, numOfParts);
  writeRegionPart(funcName);  // function name string
  writeMessageLength();
}

TcrMessageKeySet::TcrMessageKeySet(const std::string& funcName,
                                   ThinClientBaseDM* connectionDM) {
  m_msgType = TcrMessage::KEY_SET;
  m_tcdm = connectionDM;

  uint32_t numOfParts = 1;
  writeHeader(m_msgType, numOfParts);
  writeRegionPart(funcName);  // function name string
  writeMessageLength();
}

void TcrMessage::setData(const char* bytearray, int32_t len, uint16_t memId) {
  if (bytearray) {
    DeleteArray<const char> delByteArr(bytearray);
    handleByteArrayResponse(bytearray, len, memId);
  }
}

TcrMessage::~TcrMessage() {
  GF_SAFE_DELETE(m_request);
  GF_SAFE_DELETE(m_cqs);
  GF_SAFE_DELETE(m_delta);
  /* adongre
   * CID 29167: Non-array delete for scalars (DELETE_ARRAY)
   * Coverity - II
   */
  // GF_SAFE_DELETE( m_deltaBytes );
  GF_SAFE_DELETE_ARRAY(m_deltaBytes);
}

const std::string& TcrMessage::getRegionName() const { return m_regionName; }

Region* TcrMessage::getRegion() const { return const_cast<Region*>(m_region); }

int32_t TcrMessage::getMessageType() const { return m_msgType; }

void TcrMessage::setMessageType(int32_t msgType) { m_msgType = msgType; }

void TcrMessage::setMessageTypeRequest(int32_t msgType) {
  m_msgTypeRequest = msgType;
}
int32_t TcrMessage::getMessageTypeRequest() const { return m_msgTypeRequest; }

const std::map<std::string, int>* TcrMessage::getCqs() const { return m_cqs; }

CacheableKeyPtr TcrMessage::getKey() const { return m_key; }

const CacheableKeyPtr& TcrMessage::getKeyRef() const { return m_key; }

CacheablePtr TcrMessage::getValue() const { return m_value; }

const CacheablePtr& TcrMessage::getValueRef() const { return m_value; }

CacheablePtr TcrMessage::getCallbackArgument() const {
  return m_callbackArgument;
}

const CacheablePtr& TcrMessage::getCallbackArgumentRef() const {
  return m_callbackArgument;
}

const char* TcrMessage::getMsgData() const {
  return (char*)m_request->getBuffer();
}

const char* TcrMessage::getMsgHeader() const {
  return (char*)m_request->getBuffer();
}

const char* TcrMessage::getMsgBody() const {
  return (char*)m_request->getBuffer() + g_headerLen;
}

uint32_t TcrMessage::getMsgLength() const {
  return m_request->getBufferLength();
}

uint32_t TcrMessage::getMsgBodyLength() const {
  return m_request->getBufferLength() - g_headerLen;
}

EventIdPtr TcrMessage::getEventId() const { return m_eventid; }

int32_t TcrMessage::getTransId() const { return m_txId; }

void TcrMessage::setTransId(int32_t txId) { m_txId = txId; }

uint32_t TcrMessage::getTimeout() const { return m_timeout; }

void TcrMessage::setTimeout(uint32_t timeout) { m_timeout = timeout; }

void TcrMessage::skipParts(DataInput& input, int32_t numParts) {
  while (numParts > 0) {
    numParts--;
    int32_t partLen;
    input.readInt(&partLen);
    LOGDEBUG("TcrMessage::skipParts partLen= %d ", partLen);
    input.advanceCursor(partLen + 1);  // Skip the whole part including "isObj"
  }
}

void TcrMessage::readEventIdPart(DataInput& input, bool skip, int32_t parts) {
  // skip requested number of parts
  if (skip) {
    skipParts(input, parts);
  }

  // read the eventid part

  int32_t eventIdLen;
  int8_t isObj;

  input.readInt(&eventIdLen);
  input.read(&isObj);

  GF_D_ASSERT(isObj != 0);

  input.readObject(m_eventid);
}

DSMemberForVersionStampPtr TcrMessage::readDSMember(gemfire::DataInput& input) {
  uint8_t typeidLen;
  input.read(&typeidLen);
  if (typeidLen == 1) {
    uint8_t typeidofMember;
    input.read(&typeidofMember);
    if (typeidofMember != GemfireTypeIdsImpl::InternalDistributedMember) {
      throw Exception(
          "Reading DSMember. Expecting type id 92 for "
          "InternalDistributedMember. ");
    }

    ClientProxyMembershipIDPtr memId =
        ClientProxyMembershipIDPtr(new ClientProxyMembershipID());
    memId->fromData(input);
    return (DSMemberForVersionStampPtr)memId;
  } else if (typeidLen == 2) {
    uint16_t typeidofMember;
    input.readInt(&typeidofMember);
    if (typeidofMember != GemfireTypeIdsImpl::DiskStoreId) {
      throw Exception(
          "Reading DSMember. Expecting type id 2133 for DiskStoreId. ");
    }
    DiskStoreId* diskStore = new DiskStoreId();
    diskStore->fromData(input);
    return DSMemberForVersionStampPtr(diskStore);
  } else {
    throw Exception(
        "Reading DSMember. Expecting type id length as either one or two "
        "byte.");
  }
}
void TcrMessage::readHashMapForGCVersions(gemfire::DataInput& input,
                                          CacheableHashMapPtr& value) {
  uint8_t hashmaptypeid;

  input.read(&hashmaptypeid);
  if (hashmaptypeid != GemfireTypeIds::CacheableHashMap) {
    throw Exception(
        "Reading HashMap For GC versions. Expecting type id of hash map. ");
  }
  int32_t len;
  input.readArrayLen(&len);

  if (len > 0) {
    CacheableKeyPtr key;
    CacheablePtr val;
    for (int32_t index = 0; index < len; index++) {
      key = readDSMember(input);
      uint8_t versiontype;
      int64_t version;
      input.read(&versiontype);
      input.readInt(&version);
      CacheablePtr valVersion = CacheableInt64::create(version);
      CacheableKeyPtr keyPtr = dynCast<CacheableKeyPtr>(key);

      CacheablePtr valVersionPtr = dynCast<CacheablePtr>(valVersion);

      if (value != NULLPTR) {
        value->insert(keyPtr, valVersionPtr);
      } else {
        throw Exception(
            "Inserting values in HashMap For GC versions. value must not be "
            "NULLPTR. ");
      }
    }
  }
}

void TcrMessage::readHashSetForGCVersions(gemfire::DataInput& input,
                                          CacheableHashSetPtr& value) {
  uint8_t hashsettypeid;

  input.read(&hashsettypeid);
  if (hashsettypeid != GemfireTypeIds::CacheableHashSet) {
    throw Exception(
        "Reading HashSet For GC versions. Expecting type id of hash set. ");
  }
  int32_t len;
  input.readArrayLen(&len);

  if (len > 0) {
    CacheableKeyPtr key;
    CacheablePtr val;
    for (int32_t index = 0; index < len; index++) {
      CacheableKeyPtr keyPtr;
      input.readObject(keyPtr);
      value->insert(keyPtr);
    }
  }
}
