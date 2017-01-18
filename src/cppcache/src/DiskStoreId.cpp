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

#include "DiskStoreId.hpp"
#include <ace/OS.h>

namespace apache {
namespace geode {
namespace client {
std::string DiskStoreId::getHashKey() {
  if (m_hashCode.size() == 0) {
    char hashCode[128] = {0};
    /* adongre  - Coverity II
    * CID 29207: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
    * "sprintf" can cause a
    * buffer overflow when done incorrectly. Because sprintf() assumes an
    * arbitrarily long string,
    * callers must be careful not to overflow the actual space of the
    * destination.
    * Use snprintf() instead, or correct precision specifiers.
    * Fix : using ACE_OS::snprintf
    */
    ACE_OS::snprintf(hashCode, 128, "%" PRIx64 "_%" PRIx64, m_mostSig,
                     m_leastSig);
    m_hashCode.append(hashCode);
  }
  return m_hashCode;
}
}  // namespace client
}  // namespace geode
}  // namespace apache
