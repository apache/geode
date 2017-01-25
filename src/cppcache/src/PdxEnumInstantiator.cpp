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
/*
* PdxEnumInstantiator.cpp
*/

#include "PdxEnumInstantiator.hpp"
#include "GeodeTypeIdsImpl.hpp"
#include <gfcpp/CacheableString.hpp>
#include <gfcpp/DataInput.hpp>

namespace apache {
namespace geode {
namespace client {

PdxEnumInstantiator::PdxEnumInstantiator() {}

PdxEnumInstantiator::~PdxEnumInstantiator() {}

int8_t PdxEnumInstantiator::typeId() const {
  return static_cast<int8_t>(GeodeTypeIds::CacheableEnum);
}

void PdxEnumInstantiator::toData(DataOutput& output) const {
  throw UnsupportedOperationException(
      "operation PdxEnumInstantiator::toData() is not supported ");
}

Serializable* PdxEnumInstantiator::fromData(DataInput& input) {
  m_enumObject = CacheableEnum::create(" ", " ", 0);
  m_enumObject->fromData(input);
  return m_enumObject.ptr();
}

CacheableStringPtr PdxEnumInstantiator::toString() const {
  return CacheableString::create("PdxEnumInstantiator");
}
}  // namespace client
}  // namespace geode
}  // namespace apache
