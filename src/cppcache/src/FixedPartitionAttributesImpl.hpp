/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __FIXED_PARTITION_ATTRIBUTES__IMPL__
#define __FIXED_PARTITION_ATTRIBUTES__IMPL__

#include <gfcpp/Serializable.hpp>
#include <gfcpp/DataInput.hpp>
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/CacheableString.hpp>
#include <gfcpp/CacheableBuiltins.hpp>

namespace gemfire {
_GF_PTR_DEF_(FixedPartitionAttributesImpl, FixedPartitionAttributesImplPtr)

class FixedPartitionAttributesImpl : public Serializable {
 private:
  CacheableStringPtr m_partitionName;
  bool m_isPrimary;
  int m_numBuckets;
  int m_startingBucketId;

 public:
  FixedPartitionAttributesImpl()
      : Serializable(),
        m_partitionName(NULLPTR),
        m_isPrimary(false),
        m_numBuckets(1),
        m_startingBucketId(-1) {}

  std::string getPartitionName() {
    if (m_partitionName != NULLPTR) {
      return m_partitionName->asChar();
    }
    return "";
  }

  int getNumBuckets() const { return m_numBuckets; }

  int isPrimary() const { return m_isPrimary; }

  void toData(DataOutput& output) const {
    if (m_partitionName != NULLPTR) {
      output.writeNativeString(m_partitionName->asChar());
    }
    output.writeBoolean(m_isPrimary);
    output.writeInt(m_numBuckets);
    output.writeInt(m_startingBucketId);
  }

  FixedPartitionAttributesImpl* fromData(DataInput& input) {
    input.readNativeString(m_partitionName);
    input.readBoolean((bool*)&m_isPrimary);
    input.readInt((int32_t*)&m_numBuckets);
    input.readInt((int32_t*)&m_startingBucketId);
    return this;
  }

  uint32_t objectSize() const {
    if (m_partitionName != NULLPTR) {
      return (uint32_t)sizeof(int) + (uint32_t)sizeof(int) +
             (uint32_t)sizeof(bool) +
             (uint32_t)(m_partitionName->length()) * (uint32_t)sizeof(char);
    }
    return 0;
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

  FixedPartitionAttributesImpl& operator=(
      const FixedPartitionAttributesImpl& rhs) {
    if (this == &rhs) return *this;
    this->m_partitionName = rhs.m_partitionName;
    this->m_isPrimary = rhs.m_isPrimary;
    this->m_numBuckets = rhs.m_numBuckets;
    this->m_startingBucketId = rhs.m_startingBucketId;
    return *this;
  }

  FixedPartitionAttributesImpl(const FixedPartitionAttributesImpl& rhs) {
    this->m_partitionName = rhs.m_partitionName;
    this->m_isPrimary = rhs.m_isPrimary;
    this->m_numBuckets = rhs.m_numBuckets;
    this->m_startingBucketId = rhs.m_startingBucketId;
  }

  int getStartingBucketID() const { return m_startingBucketId; }

  int getLastBucketID() const { return m_startingBucketId + m_numBuckets - 1; }

  bool hasBucket(int bucketId) {
    return getStartingBucketID() <= bucketId && bucketId <= getLastBucketID();
  }
};
}

#endif
