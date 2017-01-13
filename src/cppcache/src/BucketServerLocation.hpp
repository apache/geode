/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __BUCKET_SERVER_LOCATION__
#define __BUCKET_SERVER_LOCATION__

#include "ServerLocation.hpp"
#include <string>

namespace gemfire {
_GF_PTR_DEF_(BucketServerLocation, BucketServerLocationPtr)

class BucketServerLocation : public ServerLocation {
 private:
  int m_bucketId;
  bool m_isPrimary;
  int8_t m_version;
  CacheableStringArrayPtr m_serverGroups;
  int8_t m_numServerGroups;

 public:
  BucketServerLocation()
      : ServerLocation(),
        m_bucketId(-1),
        m_isPrimary(false),
        m_version(0),
        m_serverGroups(NULLPTR),
        m_numServerGroups((int8_t)0) {}

  BucketServerLocation(std::string host)
      : ServerLocation(host),
        m_bucketId(-1),
        m_isPrimary(false),
        m_version(0),
        m_serverGroups(NULLPTR),
        m_numServerGroups((int8_t)0) {}

  BucketServerLocation(int bucketId, int port, std::string host, bool isPrimary,
                       int8 version)
      : ServerLocation(host, port),
        m_bucketId(bucketId),
        m_isPrimary(isPrimary),
        m_version(version),
        m_serverGroups(NULLPTR),
        m_numServerGroups((int8_t)0) {}

  BucketServerLocation(int bucketId, int port, std::string host, bool isPrimary,
                       int8 version, std::vector<std::string> serverGroups)
      : ServerLocation(host, port),
        m_bucketId(bucketId),
        m_isPrimary(isPrimary),
        m_version(version) {
    int32_t size = (int32_t)serverGroups.size();
    CacheableStringPtr* ptrArr = NULL;
    if (size > 0) {
      ptrArr = new CacheableStringPtr[size];
      for (int i = 0; i < size; i++) {
        ptrArr[i] = CacheableString::create(
            serverGroups[i].c_str(),
            static_cast<int32_t>(serverGroups[i].length()));
      }
    }
    if (size > 0) {
      if (size > 0x7f) {
        // TODO:  should fail here since m_numServerGroups is int8_t?
      }
      m_serverGroups = CacheableStringArray::createNoCopy(ptrArr, size);
      m_numServerGroups = (int8_t)size;
    } else {
      m_serverGroups = NULLPTR;
      m_numServerGroups = (int8_t)0;
    }
  }

  inline int getBucketId() const { return m_bucketId; }

  inline bool isPrimary() const { return m_isPrimary; }

  inline int8 getVersion() const { return m_version; }

  void toData(gemfire::DataOutput& output) const {
    ServerLocation::toData(output);
    output.writeInt(m_bucketId);
    output.writeBoolean(m_isPrimary);
    output.write(m_version);
    output.write((int8_t)m_numServerGroups);
    if (m_numServerGroups > 0) {
      for (int i = 0; i < m_numServerGroups; i++) {
        output.writeNativeString(m_serverGroups[i]->asChar());
      }
    }
  }

  BucketServerLocation* fromData(gemfire::DataInput& input) {
    ServerLocation::fromData(input);
    input.readInt((int32_t*)&m_bucketId);
    input.readBoolean((bool*)&m_isPrimary);
    input.read((int8_t*)&m_version);
    input.read(((int8_t*)&m_numServerGroups));
    CacheableStringPtr* serverGroups = NULL;
    if (m_numServerGroups > 0) {
      serverGroups = new CacheableStringPtr[m_numServerGroups];
      for (int i = 0; i < m_numServerGroups; i++) {
        input.readNativeString(serverGroups[i]);
      }
    }
    if (m_numServerGroups > 0) {
      m_serverGroups =
          CacheableStringArray::createNoCopy(serverGroups, m_numServerGroups);
    }
    return this;
  }

  uint32_t objectSize() const {
    return sizeof(int) + sizeof(bool) + sizeof(int8);
  }

  int8_t typeId() const {
    return 0;  // NOt needed infact
  }

  int8_t DSFID() const {
    return (int8_t)GemfireTypeIdsImpl::FixedIDByte;  // Never used
  }

  int32_t classId() const {
    return 0;  // Never used
  }

  BucketServerLocation& operator=(const BucketServerLocation& rhs) {
    if (this == &rhs) return *this;
    this->m_serverName = rhs.m_serverName;
    this->m_port = rhs.m_port;
    //(ServerLocation&)*this = rhs;
    this->m_bucketId = rhs.m_bucketId;
    this->m_isPrimary = rhs.m_isPrimary;
    this->m_version = rhs.m_version;
    this->m_numServerGroups = rhs.m_numServerGroups;
    this->m_serverGroups = rhs.m_serverGroups;
    return *this;
  }

  BucketServerLocation(
      const BucketServerLocation&
          rhs)  //:ServerLocation(rhs.getServerName(),rhs.getPort())
  {
    this->m_serverName = rhs.m_serverName;
    this->m_port = rhs.m_port;
    this->m_bucketId = rhs.m_bucketId;
    this->m_isPrimary = rhs.m_isPrimary;
    this->m_version = rhs.m_version;
    this->m_numServerGroups = rhs.m_numServerGroups;
    this->m_serverGroups = rhs.m_serverGroups;
  }

  inline CacheableStringArrayPtr getServerGroups() { return m_serverGroups; }
};
}

#endif
