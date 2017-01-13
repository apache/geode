/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/CacheableObjectArray.hpp>
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/DataInput.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include <gfcpp/GemfireTypeIds.hpp>
#include <GemfireTypeIdsImpl.hpp>

namespace gemfire {

void CacheableObjectArray::toData(DataOutput& output) const {
  int32_t len = size();
  output.writeArrayLen(len);
  output.write(static_cast<int8_t>(GemfireTypeIdsImpl::Class));
  output.write(static_cast<int8_t>(GemfireTypeIds::CacheableASCIIString));
  output.writeASCII("java.lang.Object");
  for (Iterator iter = begin(); iter != end(); ++iter) {
    output.writeObject(*iter);
  }
}

Serializable* CacheableObjectArray::fromData(DataInput& input) {
  int32_t len;
  input.readArrayLen(&len);
  if (len >= 0) {
    int8_t header;
    input.read(&header);  // ignore CLASS typeid
    input.read(&header);  // ignore string typeid
    uint16_t classLen;
    input.readInt(&classLen);
    input.advanceCursor(classLen);
    CacheablePtr obj;
    for (int32_t index = 0; index < len; index++) {
      input.readObject(obj);
      push_back(obj);
    }
  }
  return this;
}

int32_t CacheableObjectArray::classId() const { return 0; }

int8_t CacheableObjectArray::typeId() const {
  return GemfireTypeIds::CacheableObjectArray;
}

uint32_t CacheableObjectArray::objectSize() const {
  uint32_t size = sizeof(CacheableObjectArray);
  for (Iterator iter = begin(); iter != end(); ++iter) {
    size += (*iter)->objectSize();
  }
  return size;
}
}  // namespace gemfire
