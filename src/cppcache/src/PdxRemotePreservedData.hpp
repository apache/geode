/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * PdxType.hpp
 *
 *  Created on: Dec 09, 2011
 *      Author: npatel
 */

#ifndef _GEMFIRE_IMPL_PDXREMOTEPRESERVEDDATA_HPP_
#define _GEMFIRE_IMPL_PDXREMOTEPRESERVEDDATA_HPP_

#include <gfcpp/PdxUnreadFields.hpp>
#include <vector>

namespace gemfire {
class PdxRemotePreservedData;
typedef SharedPtr<PdxRemotePreservedData> PdxRemotePreservedDataPtr;

class PdxRemotePreservedData : public PdxUnreadFields {
 private:
  std::vector<std::vector<int8_t> > m_preservedData;
  int32 m_typeId;
  int32 m_mergedTypeId;
  int32 m_currentIndex;
  SerializablePtr /*Object^*/ m_owner;
  long m_expiryTakId;

 public:
  PdxRemotePreservedData()
      : /* adongre  - Coverity II
         * CID 29283: Uninitialized scalar field (UNINIT_CTOR)
         */
        m_typeId(0),
        m_mergedTypeId(0),
        m_currentIndex(0),
        m_expiryTakId(0) {}

  virtual ~PdxRemotePreservedData() {
    /*for(int i=0;i<numberOfFields;i++)
            delete[] m_preservedData[i];

    delete[] m_preservedData;*/
  }
  PdxRemotePreservedData(int32 typeId, int32 mergedTypeId,
                         int32 numberOfFields, /*Object^*/
                         SerializablePtr owner) {
    m_typeId = typeId;
    m_mergedTypeId = mergedTypeId;
    m_currentIndex = 0;
    m_owner = owner;
    m_expiryTakId = 0;
  }

  void initialize(int32 typeId, int32 mergedTypeId,
                  int32 numberOfFields, /*Object^*/
                  SerializablePtr owner) {
    m_typeId = typeId;
    m_mergedTypeId = mergedTypeId;
    m_currentIndex = 0;
    m_owner = owner;
    m_expiryTakId = 0;
  }

  inline int32 getMergedTypeId() { return m_mergedTypeId; }

  inline void setPreservedDataExpiryTaskId(long expId) {
    m_expiryTakId = expId;
  }

  inline long getPreservedDataExpiryTaskId() { return m_expiryTakId; }

  SerializablePtr getOwner() { return m_owner; }

  void setOwner(SerializablePtr val) { m_owner = val; }

  inline std::vector<int8_t> getPreservedData(int32 idx) {
    return m_preservedData[idx];
  }

  inline void setPreservedData(std::vector<int8_t> inputVector) {
    m_preservedData.push_back(inputVector);
  }

  virtual bool equals(SerializablePtr otherObject) {
    if (otherObject == NULLPTR) return false;

    if (m_owner == NULLPTR) return false;

    return m_owner == otherObject;
  }

  virtual int GetHashCode() {
    if (m_owner != NULLPTR) {
      // TODO
      return 1;  // m_owner->GetHashCode();
    }
    return 0;
  }
};
}
#endif
