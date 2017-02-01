#pragma once

#ifndef GEODE_FIXEDPARTITIONATTRIBUTESIMPL_H_
#define GEODE_FIXEDPARTITIONATTRIBUTESIMPL_H_

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

#include <gfcpp/Serializable.hpp>
#include <gfcpp/DataInput.hpp>
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/CacheableString.hpp>
#include <gfcpp/CacheableBuiltins.hpp>

namespace apache {
namespace geode {
namespace client {
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
    input.readBoolean(&m_isPrimary);
    input.readInt((int32_t*)&m_numBuckets);
    input.readInt((int32_t*)&m_startingBucketId);
    return this;
  }

  uint32_t objectSize() const {
    if (m_partitionName != NULLPTR) {
      return static_cast<uint32_t>(sizeof(int)) +
             static_cast<uint32_t>(sizeof(int)) +
             static_cast<uint32_t>(sizeof(bool)) +
             (m_partitionName->length()) * static_cast<uint32_t>(sizeof(char));
    }
    return 0;
  }

  int8_t typeId() const {
    return 0;  // NOt needed infact
  }

  int8_t DSFID() const {
    return static_cast<int8_t>(GeodeTypeIdsImpl::FixedIDByte);  // Never used
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
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_FIXEDPARTITIONATTRIBUTESIMPL_H_
