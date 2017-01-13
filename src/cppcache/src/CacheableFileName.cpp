/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/CacheableFileName.hpp>
#include <gfcpp/GemfireTypeIds.hpp>
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/DataInput.hpp>

#include <ace/ACE.h>
#include <ace/OS.h>

namespace gemfire {

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
  return GemfireTypeIds::CacheableFileName;
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
}  // namespace gemfire
