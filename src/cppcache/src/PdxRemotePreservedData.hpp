#pragma once

#ifndef GEODE_PDXREMOTEPRESERVEDDATA_H_
#define GEODE_PDXREMOTEPRESERVEDDATA_H_

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

#include <gfcpp/PdxUnreadFields.hpp>
#include <vector>

namespace apache {
namespace geode {
namespace client {
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
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_PDXREMOTEPRESERVEDDATA_H_
