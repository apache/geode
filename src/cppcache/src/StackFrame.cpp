/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <ace/OS.h>
#include <gfcpp/gfcpp_globals.hpp>
#include "StackFrame.hpp"
#include <gfcpp/gf_base.hpp>

namespace gemfire {
char* StackFrame::asString() {
  if (m_string[0] == 0) {
    char* tmp_module = m_module;
    int32_t modlen = static_cast<int32_t>(strlen(tmp_module));
    while (modlen > 0) {
      if ((tmp_module[modlen] == '\\') || (tmp_module[modlen] == '/')) {
        modlen++;
        break;
      }
      modlen--;
    }
    ACE_OS::snprintf(m_string, 1024, "%s at %s in %s", m_symbol, m_offset,
                     tmp_module + modlen);
  }
  return m_string;
}
}  // namespace gemfire
