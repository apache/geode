#pragma once

#ifndef GEODE_CLIENTPROXYMEMBERSHIPID_H_
#define GEODE_CLIENTPROXYMEMBERSHIPID_H_

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
#include <gfcpp/DataOutput.hpp>
#include "GeodeTypeIdsImpl.hpp"
#include "DSMemberForVersionStamp.hpp"
#include <ace/OS.h>
#include <string>

namespace apache {
namespace geode {
namespace client {
class ClientProxyMembershipID;
typedef SharedPtr<ClientProxyMembershipID> ClientProxyMembershipIDPtr;

class ClientProxyMembershipID : public DSMemberForVersionStamp {
 public:
  const char* getDSMemberId(uint32_t& mesgLength) const;
  const char* getDSMemberIdForCS43(uint32_t& mesgLength) const;
  ClientProxyMembershipID(const char* hostname, uint32_t hostAddr,
                          uint32_t hostPort, const char* durableClientId = NULL,
                          const uint32_t durableClntTimeOut = 0);

  // This constructor is only for testing and should not be used for any
  // other purpose. See testEntriesMapForVersioning.cpp for more details
  ClientProxyMembershipID(uint8_t* hostAddr, uint32_t hostAddrLen,
                          uint32_t hostPort, const char* dsname,
                          const char* uniqueTag, uint32_t vmViewId);
  // ClientProxyMembershipID(const char *durableClientId = NULL, const uint32_t
  // durableClntTimeOut = 0);
  ClientProxyMembershipID();
  ~ClientProxyMembershipID();
  void getClientProxyMembershipID();
  // Initialize for random data and set the DS name.
  // This method is not thread-safe.
  static void init(const std::string& dsName);
  static const std::string& getRandStringId();
  static void increaseSynchCounter();
  static Serializable* createDeserializable() {
    return new ClientProxyMembershipID();
  }
  // Do an empty check on the returned value. Only use after handshake is done.
  const std::string& getDSMemberIdForThinClientUse();

  // Serializable interface:
  void toData(DataOutput& output) const;
  Serializable* fromData(DataInput& input);
  int32_t classId() const { return 0; }
  int8_t typeId() const { return GeodeTypeIdsImpl::InternalDistributedMember; }
  uint32_t objectSize() const { return 0; }
  int8_t DSFID() const {
    return static_cast<int8_t>(GeodeTypeIdsImpl::FixedIDByte);
  }
  void initObjectVars(const char* hostname, uint8_t* hostAddr,
                      uint32_t hostAddrLen, bool hostAddrLocalMem,
                      uint32_t hostPort, const char* durableClientId,
                      const uint32_t durableClntTimeOut, int32_t dcPort,
                      int32_t vPID, int8_t vmkind, int8_t splitBrainFlag,
                      const char* dsname, const char* uniqueTag,
                      uint32_t vmViewId);

  std::string getDSName() const { return m_dsname; }
  std::string getUniqueTag() const { return m_uniqueTag; }
  uint8_t* getHostAddr() const { return m_hostAddr; }
  uint32_t getHostAddrLen() const { return m_hostAddrLen; }
  uint32_t getHostPort() const { return m_hostPort; }
  virtual std::string getHashKey();
  virtual int16_t compareTo(DSMemberForVersionStampPtr);
  virtual uint32_t hashcode() const {
    uint32_t result = 0;
    char hostInfo[255] = {0};
    uint32_t offset = 0;
    for (uint32_t i = 0; i < getHostAddrLen(); i++) {
      offset += ACE_OS::snprintf(hostInfo + offset, 255 - offset, ":%x",
                                 m_hostAddr[i]);
    }
    CacheableStringPtr tempHashCode = CacheableString::create(hostInfo, offset);
    result = result + tempHashCode->hashcode();
    result = result + m_hostPort;
    return result;
  }

  virtual bool operator==(const CacheableKey& other) const {
    CacheableKey& otherCopy = const_cast<CacheableKey&>(other);
    DSMemberForVersionStamp& temp =
        dynamic_cast<DSMemberForVersionStamp&>(otherCopy);
    DSMemberForVersionStampPtr obj = NULLPTR;
    obj = DSMemberForVersionStampPtr(&temp);

    DSMemberForVersionStampPtr callerPtr = NULLPTR;
    callerPtr = DSMemberForVersionStampPtr(this);
    if (callerPtr->compareTo(obj) == 0) {
      return true;
    } else {
      return false;
    }
  }

  Serializable* readEssentialData(DataInput& input);

 private:
  std::string m_memIDStr;
  std::string m_dsmemIDStr;
  // static data
  static std::string g_dsName;
  static std::string g_randString;
  std::string clientID;

  std::string m_dsname;
  uint32_t m_hostPort;
  uint8_t* m_hostAddr;
  uint32_t m_hostAddrLen;
  std::string m_uniqueTag;
  std::string m_hashKey;
  bool m_hostAddrLocalMem;
  uint32_t m_vmViewId;
  static const uint8_t LONER_DM_TYPE = 13;
  static const int VERSION_MASK;
  static const int8_t TOKEN_ORDINAL;

  void readVersion(int flags, DataInput& input);
  void writeVersion(int16_t ordinal, DataOutput& output);

  void readAdditionalData(DataInput& input);
};
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_CLIENTPROXYMEMBERSHIPID_H_
