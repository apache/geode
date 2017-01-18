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
#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/CacheableBuiltins.hpp>

#include <ace/OS.h>
extern "C" {
#include <stdarg.h>
}

namespace apache {
namespace geode {
namespace client {

int gf_sprintf(char* buffer, const char* fmt, ...) {
  va_list args;
  int result;

  va_start(args, fmt);
  result = ACE_OS::vsprintf(buffer, fmt, args);
  va_end(args);
  return result;
}

int gf_snprintf(char* buffer, int32_t maxLength, const char* fmt, ...) {
  va_list args;
  int result;

  va_start(args, fmt);
  result = ACE_OS::vsnprintf(buffer, maxLength, fmt, args);
  va_end(args);
  return result;
}

#define _GF_CACHEABLE_KEY_DEF_(k, s) \
  const char tName_##k[] = #k;       \
  const char tStr_##k[] = s;

_GF_CACHEABLE_KEY_DEF_(CacheableBoolean, "%" PRIi8);
_GF_CACHEABLE_KEY_DEF_(CacheableByte, "%" PRIi8);
_GF_CACHEABLE_KEY_DEF_(CacheableDouble, "%lf");
_GF_CACHEABLE_KEY_DEF_(CacheableFloat, "%f");
_GF_CACHEABLE_KEY_DEF_(CacheableInt16, "%" PRIi16);
_GF_CACHEABLE_KEY_DEF_(CacheableInt32, "%" PRIi32);
_GF_CACHEABLE_KEY_DEF_(CacheableInt64, "%" PRIi64);
_GF_CACHEABLE_KEY_DEF_(CacheableWideChar, "%lc");
}  // namespace client
}  // namespace geode
}  // namespace apache
