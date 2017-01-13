/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "ClientProxyMembershipID.hpp"
#include <time.h>
#include <ace/OS.h>
#include <gfcpp/DistributedSystem.hpp>
#include <gfcpp/GemfireTypeIds.hpp>
#include "GemfireTypeIdsImpl.hpp"
#include <gfcpp/CacheableBuiltins.hpp>
#include "Version.hpp"
#include <string>

#define ADDRSIZE 4
#define DCPORT 12334
#define VMKIND 13
#define ROLEARRLENGTH 0
static int synch_counter = 2;
using namespace gemfire;

namespace {
    static class RandomInitializer {
    public:
        RandomInitializer() {
            // using current time and
            // processor time would be good enough for our purpose
            unsigned long seed = ACE_OS::getpid() + ACE_OS::gettimeofday().msec() + clock();
            seed += ACE_OS::gettimeofday().usec();
            // LOGINFO("PID %ld seed %ld ACE_OS::gettimeofday().usec() = %ld clock =
            // %ld ACE_OS::gettimeofday().msec() = %ld", pid, seed ,
            // ACE_OS::gettimeofday().usec() , clock(),
            // ACE_OS::gettimeofday().msec());
            ACE_OS::srand(seed);
        }
    } oneTimeRandomInitializer;
} // namespace

std::string ClientProxyMembershipID::g_dsName("DSName");
std::string ClientProxyMembershipID::g_randString("GFNative");
#define RAND_STRING_LEN 10
const int ClientProxyMembershipID::VERSION_MASK = 0x8;
const int8_t ClientProxyMembershipID::TOKEN_ORDINAL = -1;

// initialize random string data and DistributedSystem name
void ClientProxyMembershipID::init(const std::string& dsName) {
  if (dsName.size() > 0) {
    const char selectChars[] =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";
    const uint32_t numChars = (sizeof(selectChars) / sizeof(char)) - 1;

    g_dsName = dsName;
    bool randDone = false;
    char randString[RAND_STRING_LEN + 1];
    int pid = ACE_OS::getpid();
    // try /dev/urandom first
    FILE* urandom = ACE_OS::fopen("/dev/urandom", "rb");
    if (urandom) {
      LOGFINE("Opened /dev/urandom for ClientProxyMembershipID random data");
      uint8_t readBytes[RAND_STRING_LEN];
      size_t readLen = ACE_OS::fread(readBytes, RAND_STRING_LEN, 1, urandom);
      if (readLen == 1) {
        for (uint32_t index = 0; index < RAND_STRING_LEN; ++index) {
          randString[index] = selectChars[readBytes[index] % numChars];
        }
        randString[RAND_STRING_LEN] = '\0';
        randDone = true;
      }
      ACE_OS::fclose(urandom);
    }
    if (!randDone) {
      for (uint32_t index = 0; index < RAND_STRING_LEN; ++index) {
        randString[index] = selectChars[ACE_OS::rand() % numChars];
      }
      randString[RAND_STRING_LEN] = '\0';
    }
    char ps[15] = {0};
    ACE_OS::snprintf(ps, 15, "%d", pid);
    g_randString = "GFNative_";
    g_randString.append(randString).append(ps);
    LOGINFO("Using %s as random data for ClientProxyMembershipID",
            g_randString.c_str());
  }
}
const std::string& ClientProxyMembershipID::getRandStringId() {
  return g_randString;
}
/*
// Commenting this function as this is not getting used anywhere.
ClientProxyMembershipID::ClientProxyMembershipID(const char *durableClientId,
                                                 const uint32_t
durableClntTimeOut)
{
  if( durableClientId != NULL && durableClntTimeOut != 0 ) {
    DataOutput  m_memID;
    m_memID.write((int8_t)GemfireTypeIds::CacheableASCIIString);
    m_memID.writeASCII(durableClientId);
    CacheableInt32Ptr int32ptr = CacheableInt32::create(durableClntTimeOut);
    int32ptr->toData(m_memID);
    uint32_t len;
    char* buf = (char*)m_memID.getBuffer(&len);
    m_memIDStr.append(buf, len);
  }
}
*/
ClientProxyMembershipID::ClientProxyMembershipID()
    : m_hostPort(0),
      m_hostAddr(NULL)
      /* adongre  - Coverity II
       * CID 29278: Uninitialized scalar field (UNINIT_CTOR)
       */
      ,
      m_hostAddrLen(0),
      m_hostAddrLocalMem(false),
      m_vmViewId(0) {}

ClientProxyMembershipID::~ClientProxyMembershipID() {
  if (m_hostAddrLocalMem) delete[] m_hostAddr;
}

ClientProxyMembershipID::ClientProxyMembershipID(
    const char* hostname, uint32_t hostAddr, uint32_t hostPort,
    const char* durableClientId, const uint32_t durableClntTimeOut) {
  int32_t vmPID = ACE_OS::getpid();

  initObjectVars(hostname, reinterpret_cast<uint8_t*>(&hostAddr), 4, false,
                 hostPort, durableClientId, durableClntTimeOut, DCPORT, vmPID,
                 VMKIND, 0, g_dsName.c_str(), g_randString.c_str(), 0);
}

// This is only for unit tests and should not be used for any other purpose. See
// testEntriesMapForVersioning.cpp for more details
ClientProxyMembershipID::ClientProxyMembershipID(
    uint8_t* hostAddr, uint32_t hostAddrLen, uint32_t hostPort,
    const char* dsname, const char* uniqueTag, uint32_t vmViewId) {
  int32_t vmPID = ACE_OS::getpid();
  initObjectVars("localhost", hostAddr, hostAddrLen, false, hostPort, "", 0,
                 DCPORT, vmPID, VMKIND, 0, dsname, uniqueTag, vmViewId);
}
void ClientProxyMembershipID::initObjectVars(
    const char* hostname, uint8_t* hostAddr, uint32_t hostAddrLen,
    bool hostAddrLocalMem, uint32_t hostPort, const char* durableClientId,
    const uint32_t durableClntTimeOut, int32_t dcPort, int32_t vPID,
    int8_t vmkind, int8_t splitBrainFlag, const char* dsname,
    const char* uniqueTag, uint32_t vmViewId) {
  DataOutput m_memID;
  if (dsname == NULL) {
    m_dsname = std::string("");
  } else {
    m_dsname = std::string(dsname);
  }
  m_hostPort = hostPort;
  m_hostAddr = hostAddr;
  m_hostAddrLen = hostAddrLen;
  m_hostAddrLocalMem = hostAddrLocalMem;
  if (uniqueTag == NULL) {
    m_uniqueTag = std::string("");
  } else {
    m_uniqueTag = std::string(uniqueTag);
  }

  m_vmViewId = vmViewId;
  m_memID.write(static_cast<int8_t>(GemfireTypeIdsImpl::FixedIDByte));
  m_memID.write(
      static_cast<int8_t>(GemfireTypeIdsImpl::InternalDistributedMember));
  m_memID.writeArrayLen(ADDRSIZE);
  // writing first 4 bytes of the address. This will be same until
  // IPV6 support is added in the client
  uint32_t temp;
  memcpy(&temp, hostAddr, 4);
  m_memID.writeInt(static_cast<int32_t>(temp));
  // m_memID.writeInt((int32_t)hostPort);
  m_memID.writeInt((int32_t)synch_counter);
  m_memID.write(static_cast<int8_t>(GemfireTypeIds::CacheableASCIIString));
  m_memID.writeASCII(hostname);
  m_memID.write(splitBrainFlag);  // splitbrain flags

  m_memID.writeInt(dcPort);

  m_memID.writeInt(vPID);
  m_memID.write(vmkind);
  m_memID.writeArrayLen(ROLEARRLENGTH);
  m_memID.write(static_cast<int8_t>(GemfireTypeIds::CacheableASCIIString));
  m_memID.writeASCII(dsname);
  m_memID.write(static_cast<int8_t>(GemfireTypeIds::CacheableASCIIString));
  m_memID.writeASCII(uniqueTag);

  if (durableClientId != NULL && durableClntTimeOut != 0) {
    m_memID.write(static_cast<int8_t>(GemfireTypeIds::CacheableASCIIString));
    m_memID.writeASCII(durableClientId);
    CacheableInt32Ptr int32ptr = CacheableInt32::create(durableClntTimeOut);
    int32ptr->toData(m_memID);
  }
  writeVersion(Version::getOrdinal(), m_memID);
  uint32_t len;
  char* buf = (char*)m_memID.getBuffer(&len);
  m_memIDStr.append(buf, len);
  /* adongre - Coverity II
   * CID 29206: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
   * "sprintf" can cause a
   * buffer overflow when done incorrectly. Because sprintf() assumes an
   * arbitrarily long string,
   * callers must be careful not to overflow the actual space of the
   * destination.
   * Use snprintf() instead, or correct precision specifiers.
   * Fix : using ACE_OS::snprintf
   */
  char PID[15] = {0};
  char Synch_Counter[15] = {0};
  // ACE_OS::snprintf(PID, 15, "%d",vPID);
  ACE_OS::itoa(vPID, PID, 10);
  // ACE_OS::snprintf(Synch_Counter, 15, "%d",synch_counter);
  ACE_OS::itoa(synch_counter, Synch_Counter, 10);
  clientID.append(hostname);
  clientID.append("(");
  clientID.append(PID);
  clientID.append(":loner):");
  clientID.append(Synch_Counter);
  clientID.append(":");
  clientID.append(getUniqueTag());
  clientID.append(":");
  clientID.append(getDSName());
  // Hash key.

  // int offset = 0;
  for (uint32_t i = 0; i < getHostAddrLen(); i++) {
    char hostInfo[16] = {0};
    // offset += ACE_OS::snprintf(hostInfo + offset , 255 - offset, ":%x",
    // m_hostAddr[i]);
    ACE_OS::itoa(m_hostAddr[i], hostInfo, 16);
    m_hashKey.append(":");
    m_hashKey.append(hostInfo);
  }
  m_hashKey.append(":");
  char hostInfoPort[16] = {0};
  ACE_OS::itoa(getHostPort(), hostInfoPort, 10);
  //  offset += ACE_OS::snprintf(hostInfo + offset, 255 - offset , ":%d",
  //  getHostPort());
  m_hashKey.append(hostInfoPort);
  m_hashKey.append(":");
  m_hashKey.append(getDSName());
  m_hashKey.append(":");
  if (m_uniqueTag.size() != 0) {
    m_hashKey.append(getUniqueTag());
  } else {
    m_hashKey.append(":");
    char viewid[16] = {0};
    ACE_OS::itoa(m_vmViewId, viewid, 10);
    // offset += ACE_OS::snprintf(hostInfo + offset , 255 - offset , ":%d",
    // m_vmViewId);
    m_hashKey.append(viewid);
  }
  LOGDEBUG("GethashKey %s client id: %s ", m_hashKey.c_str(), clientID.c_str());
}
const char* ClientProxyMembershipID::getDSMemberId(uint32_t& mesgLength) const {
  mesgLength = static_cast<int32_t>(m_memIDStr.size());
  return m_memIDStr.c_str();
}
const char* ClientProxyMembershipID::getDSMemberIdForCS43(
    uint32_t& mesgLength) const {
  mesgLength = static_cast<int32_t>(m_dsmemIDStr.size());
  return m_dsmemIDStr.c_str();
}

const std::string& ClientProxyMembershipID::getDSMemberIdForThinClientUse() {
  return clientID;
}

std::string ClientProxyMembershipID::getHashKey() { return m_hashKey; }

void ClientProxyMembershipID::getClientProxyMembershipID() {
  // Implement LonerDistributionManager::generateMemberId() and store result in
  // dsMemberId,dsMemberIdLength
  const char* hex = "0123456789ABCDEF";
  std::string DSMemberId = "";
  ACE_TCHAR host[MAXHOSTNAMELEN];
  std::string hostName = " ";
  char buf[50];
  char dsName[50];
  DistributedSystemPtr dsPtr;
  dsPtr = DistributedSystem::getInstance();

  ACE_OS::hostname(host, sizeof(host) - 1);
  hostName = host;
  pid_t pid;
  pid = ACE_OS::getpid();

  /* adongre
   * CID 28814: Resource leak (RESOURCE_LEAK)
   * Following allocation is not used anywhere
   * commenting the same.
   */

  /*int* random = new int[8];
  for (int i = 0; i < 8; i++) {
  random[i]=ACE_OS::rand()%16;
  } */
  char* hname = host;

  // ACE_OS::sprintf(hname,"%s",hostName);
  uint32_t len = static_cast<uint32_t>(hostName.length());
  DataOutput m_dsmemID;
  m_dsmemID.writeBytesOnly(reinterpret_cast<int8_t*>(hname), len);
  // DSMemberId = DSMemberId.append(host);
  // DSMemberId= DSMemberId.append("(");
  m_dsmemID.write(static_cast<int8_t>('('));
  int lenPid = ACE_OS::snprintf(buf, 50, "%d", pid);
  // DSMemberId.append(buf);
  // m_dsmemID.writeInt((int32_t)pid);
  m_dsmemID.writeBytesOnly(reinterpret_cast<int8_t*>(buf), lenPid);
  // DSMemberId.append("):");
  m_dsmemID.write(static_cast<int8_t>(')'));
  m_dsmemID.write(static_cast<int8_t>(':'));

  char hexBuf[20];
  for (int j = 0; j < 8; j++) {
    //  Hardcoding random number for Thin Client
    // hexBuf[j] = hex[random[j]%16];
    hexBuf[j] = hex[1];
  }
  // DSMemberId = DSMemberId.append(hexBuf);
  m_dsmemID.writeBytesOnly(reinterpret_cast<int8_t*>(hexBuf), 8);
  m_dsmemID.write(static_cast<int8_t>(':'));
  // DSMemberId = DSMemberId.append(":");
  ACE_OS::snprintf(dsName, 50, "%s", dsPtr->getName());
  // DSMemberId.append(dsName);
  uint32_t dsLen = static_cast<uint32_t>(strlen(dsName));
  m_dsmemID.writeBytesOnly(reinterpret_cast<int8_t*>(dsName), dsLen);
  m_dsmemIDStr += (char*)m_dsmemID.getBuffer();
  uint32_t strLen;
  char* strBuf = (char*)m_dsmemID.getBuffer(&strLen);
  m_dsmemIDStr.append(strBuf, strLen);
  // dsMemberIdLength = DSMemberId.length();
  // dsMemberId= (char*)ACE_OS::malloc(dsMemberIdLength+1);
  // ACE_OS::strcpy(dsMemberId,DSMemberId.c_str());
  // return m_dsmemID;
}

void ClientProxyMembershipID::toData(DataOutput& output) const {
  throw IllegalStateException("Member ID toData() not implemented.");
}

Serializable* ClientProxyMembershipID::fromData(DataInput& input) {
  // deserialization for PR FX HA
  uint8_t* hostAddr;
  int32_t len, hostPort, dcport, vPID, durableClntTimeOut;
  CacheableStringPtr hostname, dsName, uniqueTag, durableClientId;
  int8_t splitbrain, vmKind;

  input.readArrayLen(&len);  // inetaddress len
  m_hostAddrLocalMem = true;
  /* adongre  - Coverity II
   * CID 29184: Out-of-bounds access (OVERRUN_DYNAMIC)
   */
  // hostAddr = new uint8_t(len);
  hostAddr = new uint8_t[len];

  input.readBytesOnly(hostAddr, len);  // inetaddress
  input.readInt(&hostPort);            // port
  input.readObject(hostname);          // hostname
  input.read(&splitbrain);             // splitbrain
  input.readInt(&dcport);              // port
  input.readInt(&vPID);                // pid
  input.read(&vmKind);                 // vmkind
  CacheableStringArrayPtr aStringArray(CacheableStringArray::create());
  aStringArray->fromData(input);
  // #925 - currently reading empty string keep watch here,
  // server might remove even sending this string
  // (https://svn.gemstone.com/trac/gemfire/changeset/44566).
  input.readObject(dsName);            // name
  input.readObject(uniqueTag);         // unique tag
  input.readObject(durableClientId);   // durable client id
  input.readInt(&durableClntTimeOut);  // durable client timeout
  int32_t vmViewId = 0;
  readVersion(splitbrain, input);
  if (vmKind != ClientProxyMembershipID::LONER_DM_TYPE) {
    vmViewId = atoi(uniqueTag.ptr()->asChar());
    initObjectVars(hostname->asChar(), hostAddr, len, true, hostPort,
                   durableClientId->asChar(), durableClntTimeOut, dcport, vPID,
                   vmKind, splitbrain, dsName->asChar(), NULL, vmViewId);
  } else {
    // initialize the object
    initObjectVars(hostname->asChar(), hostAddr, len, true, hostPort,
                   durableClientId->asChar(), durableClntTimeOut, dcport, vPID,
                   vmKind, splitbrain, dsName->asChar(), uniqueTag->asChar(),
                   0);
  }

  return this;
}

Serializable* ClientProxyMembershipID::readEssentialData(DataInput& input) {
  uint8_t* hostAddr;
  int32_t len, hostPort, vmViewId = 0;
  CacheableStringPtr hostname, dsName, uniqueTag, vmViewIdstr;
  int8_t vmKind;
  uint8_t flag;

  input.readArrayLen(&len);  // inetaddress len
  m_hostAddrLocalMem = true;
  /* adongre - Coverity II
   * CID 29183: Out-of-bounds access (OVERRUN_DYNAMIC)
   */
  // hostAddr = new uint8_t(len);
  hostAddr = new uint8_t[len];

  input.readBytesOnly(hostAddr, len);  // inetaddress

  input.readInt(&hostPort);  // port
  // TODO: RVV get the host name from

  input.read(&flag);

  input.read(&vmKind);  // vmkind

  if (vmKind == ClientProxyMembershipID::LONER_DM_TYPE) {
    input.readObject(uniqueTag);  // unique tag
  } else {
    input.readObject(vmViewIdstr);
    vmViewId = atoi(vmViewIdstr.ptr()->asChar());
  }

  // #925 - currently reading empty string keep watch here,
  // server might remove even sending this string
  // (https://svn.gemstone.com/trac/gemfire/changeset/44566).
  input.readObject(dsName);  // name

  if (vmKind != ClientProxyMembershipID::LONER_DM_TYPE) {
    // initialize the object with the values read and some dummy values
    initObjectVars("", hostAddr, len, true, hostPort, "", 0, DCPORT, 0, vmKind,
                   0, dsName->asChar(), NULL, vmViewId);
  } else {
    // initialize the object with the values read and some dummy values
    initObjectVars("", hostAddr, len, true, hostPort, "", 0, DCPORT, 0, vmKind,
                   0, dsName->asChar(), uniqueTag->asChar(), vmViewId);
  }

  return this;
}

void ClientProxyMembershipID::increaseSynchCounter() { ++synch_counter; }

// Compares two membershipIds. This is based on the compareTo function
// of InternalDistributedMember class of Java.
// Any change to the java function should be reflected here as well.
int16_t ClientProxyMembershipID::compareTo(DSMemberForVersionStampPtr other) {
  if (other.operator==(this)) {
    return 0;
  }
  if (other.ptr() == NULL) {
    throw ClassCastException(
        "ClientProxyMembershipID.compare(): comparing with a null value");
  }

  ClientProxyMembershipIDPtr otherMember =
      dynCast<ClientProxyMembershipIDPtr>(other);
  uint32_t myPort = getHostPort();
  uint32_t otherPort = otherMember->getHostPort();

  if (myPort < otherPort) return -1;
  if (myPort > otherPort) return 1;

  uint8_t* myAddr = getHostAddr();
  uint8_t* otherAddr = otherMember->getHostAddr();
  // Discard null cases
  if (myAddr == NULL && otherAddr == NULL) {
    if (myPort < otherPort) {
      return -1;
    } else if (myPort > otherPort) {
      return 1;
    } else {
      return 0;
    }
  } else if (myAddr == NULL) {
    return -1;
  } else if (otherAddr == NULL) {
    return 1;
  }
  for (uint32_t i = 0; i < getHostAddrLen(); i++) {
    if (myAddr[i] < otherAddr[i]) return -1;
    if (myAddr[i] > otherAddr[i]) return 1;
  }
  // #925 - currently reading empty string keep watch here,
  // server might remove even sending this string
  // (https://svn.gemstone.com/trac/gemfire/changeset/44566).
  // InternalDistributedMember no longer uses "name" in comparisons.
  // std::string myDSName = getDSName();
  // std::string otherDSName = otherMember->getDSName();

  // if (myDSName.empty()&& otherDSName.empty()) {
  //  // do nothing
  //} else if (myDSName.empty()) {
  //  return -1;
  //}
  // else if (otherDSName.empty()) {
  //  return 1;
  //}
  // else {
  //  int i = myDSName.compare(otherDSName);
  //  if (i != 0) {
  //    return i;
  //  }
  //}
  std::string myUniqueTag = getUniqueTag();
  std::string otherUniqueTag = otherMember->getUniqueTag();
  if (myUniqueTag.empty() && otherUniqueTag.empty()) {
    if (m_vmViewId < otherMember->m_vmViewId) {
      return -1;
    } else if (m_vmViewId > otherMember->m_vmViewId) {
      return 1;
    }  // else they're the same, so continue
  } else if (myUniqueTag.empty()) {
    return -1;
  } else if (otherUniqueTag.empty()) {
    return 1;
  } else {
    int i = myUniqueTag.compare(otherUniqueTag);
    if (i != 0) {
      return i;
    }
  }
  return 0;
}

void ClientProxyMembershipID::readVersion(int flags, DataInput& input) {
  if ((flags & ClientProxyMembershipID::VERSION_MASK) != 0) {
    int8_t ordinal;
    input.read(&ordinal);
    LOGDEBUG("ClientProxyMembershipID::readVersion ordinal = %d ", ordinal);
    if (ordinal != ClientProxyMembershipID::TOKEN_ORDINAL) {
    } else {
      int16_t ordinal;
      input.readInt(&ordinal);
      LOGDEBUG("ClientProxyMembershipID::readVersion ordinal = %d ", ordinal);
    }
  }
}

void ClientProxyMembershipID::writeVersion(int16_t ordinal,
                                           DataOutput& output) {
  if (ordinal <= SCHAR_MAX) {
    output.write(static_cast<int8_t>(ordinal));
    LOGDEBUG("ClientProxyMembershipID::writeVersion ordinal = %d ", ordinal);
  } else {
    output.write(ClientProxyMembershipID::TOKEN_ORDINAL);
    output.writeInt(ordinal);
    LOGDEBUG("ClientProxyMembershipID::writeVersion ordinal = %d ", ordinal);
  }
}
