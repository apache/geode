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
