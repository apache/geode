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
#include <gfcpp/CacheableFileName.hpp>
#include <gfcpp/GeodeTypeIds.hpp>
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/DataInput.hpp>

#include <ace/ACE.h>
#include <ace/OS.h>

namespace apache {
namespace geode {
namespace client {

void CacheableFileName::toData(DataOutput& output) const {
  output.write(m_type);
  CacheableString::toData(output);
}

Serializable* CacheableFileName::fromData(DataInput& input) {
  input.read(&m_type);
  (void)CacheableString::fromData(input);
  return this;
}

int32_t CacheableFileName::classId() const { return 0; }

int8_t CacheableFileName::typeId() const {
  return GeodeTypeIds::CacheableFileName;
}

uint32_t CacheableFileName::hashcode() const {
#ifndef _WIN32
  if (m_hashcode == 0) {
    m_hashcode = CacheableString::hashcode() ^ 1234321;
  }
  return m_hashcode;
#endif
  if (m_hashcode == 0) {
    int localHashcode = 0;
    if (CacheableString::isCString()) {
      const char* data = CacheableString::asChar();
      for (uint32_t i = 0; i < CacheableString::length(); i++) {
        localHashcode = 31 * localHashcode + ACE_OS::ace_tolower(data[i]);
      }
    } else {
      const wchar_t* data = CacheableString::asWChar();
      for (uint32_t i = 0; i < CacheableString::length(); i++) {
        localHashcode = 31 * localHashcode + ACE_OS::ace_tolower(data[i]);
      }
    }
    m_hashcode = localHashcode ^ 1234321;
  }
  return m_hashcode;
}
}  // namespace client
}  // namespace geode
}  // namespace apache
