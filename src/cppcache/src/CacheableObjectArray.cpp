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
#include <gfcpp/CacheableObjectArray.hpp>
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/DataInput.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include <gfcpp/GeodeTypeIds.hpp>
#include <GeodeTypeIdsImpl.hpp>

namespace apache {
namespace geode {
namespace client {

void CacheableObjectArray::toData(DataOutput& output) const {
  int32_t len = size();
  output.writeArrayLen(len);
  output.write(static_cast<int8_t>(GeodeTypeIdsImpl::Class));
  output.write(static_cast<int8_t>(GeodeTypeIds::CacheableASCIIString));
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
  return GeodeTypeIds::CacheableObjectArray;
}

uint32_t CacheableObjectArray::objectSize() const {
  uint32_t size = sizeof(CacheableObjectArray);
  for (Iterator iter = begin(); iter != end(); ++iter) {
    size += (*iter)->objectSize();
  }
  return size;
}
}  // namespace client
}  // namespace geode
}  // namespace apache
