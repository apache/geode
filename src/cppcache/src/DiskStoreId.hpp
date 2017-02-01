#pragma once

#ifndef GEODE_DISKSTOREID_H_
#define GEODE_DISKSTOREID_H_

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
#include <gfcpp/DataInput.hpp>
#include "DSMemberForVersionStamp.hpp"
#include "GeodeTypeIdsImpl.hpp"

namespace apache {
namespace geode {
namespace client {

class DiskStoreId : public DSMemberForVersionStamp {
 public:
  DiskStoreId() : m_hashCode(""), m_mostSig(0), m_leastSig(0) {}

  /**
  * for internal testing
  */
  DiskStoreId(int64_t mostSig, int64_t leastSig)
      : m_hashCode(""), m_mostSig(mostSig), m_leastSig(leastSig) {}

  DiskStoreId(const DiskStoreId& rhs)
      : m_mostSig(rhs.m_mostSig), m_leastSig(rhs.m_leastSig) {}

  DiskStoreId& operator=(const DiskStoreId& rhs) {
    if (this == &rhs) return *this;
    this->m_leastSig = rhs.m_leastSig;
    this->m_mostSig = rhs.m_mostSig;
    return *this;
  }

  virtual void toData(DataOutput& output) const {
    throw IllegalStateException("DiskStoreId::toData not implemented");
  }
  virtual Serializable* fromData(DataInput& input) {
    input.readInt(&m_mostSig);
    input.readInt(&m_leastSig);
    return this;
  }
  virtual int32_t classId() const { return 0; }

  virtual int8_t typeId() const {
    return static_cast<int8_t>(GeodeTypeIdsImpl::DiskStoreId);
  }

  virtual int16_t compareTo(DSMemberForVersionStampPtr tagID) {
    int64_t result = m_mostSig - ((DiskStoreId*)tagID.ptr())->m_mostSig;
    if (result == 0) {
      result = m_leastSig - ((DiskStoreId*)tagID.ptr())->m_leastSig;
    }
    if (result < 0) {
      return -1;
    } else if (result > 0) {
      return 1;
    } else {
      return 0;
    }
  }
  static Serializable* createDeserializable() { return new DiskStoreId(); }
  std::string getHashKey();

  virtual uint32_t hashcode() const {
    static uint32_t prime = 31;
    uint32_t result = 1;
    result =
        prime * result + static_cast<uint32_t>(m_leastSig ^ (m_leastSig >> 32));
    result =
        prime * result + static_cast<uint32_t>(m_mostSig ^ (m_mostSig >> 32));
    return result;
  }

  virtual bool operator==(const CacheableKey& other) const {
    CacheableKey& otherCopy = const_cast<CacheableKey&>(other);
    DSMemberForVersionStamp& temp =
        dynamic_cast<DSMemberForVersionStamp&>(otherCopy);
    DSMemberForVersionStampPtr otherObjPtr = NULLPTR;
    otherObjPtr = DSMemberForVersionStampPtr(&temp);

    DSMemberForVersionStampPtr callerPtr = NULLPTR;
    callerPtr = DSMemberForVersionStampPtr(this);
    if (callerPtr->compareTo(otherObjPtr) == 0) {
      return true;
    } else {
      return false;
    }
  }

 private:
  std::string m_hashCode;
  int64_t m_mostSig;
  int64_t m_leastSig;
};
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_DISKSTOREID_H_
