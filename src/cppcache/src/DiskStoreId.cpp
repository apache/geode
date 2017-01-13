/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "DiskStoreId.hpp"
#include <ace/OS.h>

namespace gemfire {
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
}  // namespace gemfire
