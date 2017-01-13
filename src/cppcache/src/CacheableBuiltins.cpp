/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/CacheableBuiltins.hpp>

#include <ace/OS.h>
extern "C" {
#include <stdarg.h>
}

namespace gemfire {

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
}  // namespace gemfire
