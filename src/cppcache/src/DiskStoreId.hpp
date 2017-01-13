/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __DiskStoreId_HPP__
#define __DiskStoreId_HPP__

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/DataInput.hpp>
#include "DSMemberForVersionStamp.hpp"
#include "GemfireTypeIdsImpl.hpp"

namespace gemfire {

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
    return (int8_t)GemfireTypeIdsImpl::DiskStoreId;
  }

  virtual int16_t compareTo(DSMemberForVersionStampPtr tagID) {
    int64_t result = m_mostSig - ((DiskStoreId*)tagID.ptr())->m_mostSig;
    if (result == 0) {
      result = m_leastSig - ((DiskStoreId*)tagID.ptr())->m_leastSig;
    }
    if (result < 0)
      return -1;
    else if (result > 0)
      return 1;
    else
      return 0;
  }
  static Serializable* createDeserializable() { return new DiskStoreId(); }
  std::string getHashKey();

  virtual uint32_t hashcode() const {
    static uint32_t prime = 31;
    uint32_t result = 1;
    result = prime * result + (uint32_t)(m_leastSig ^ (m_leastSig >> 32));
    result = prime * result + (uint32_t)(m_mostSig ^ (m_mostSig >> 32));
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
    if (callerPtr->compareTo(otherObjPtr) == 0)
      return true;
    else
      return false;
  }

 private:
  std::string m_hashCode;
  int64_t m_mostSig;
  int64_t m_leastSig;
};
}

#endif  // __DiskStoreId_HPP__
