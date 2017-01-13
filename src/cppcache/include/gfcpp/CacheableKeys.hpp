#ifndef _GEMFIRE_CACHEABLEKEYS_HPP_
#define _GEMFIRE_CACHEABLEKEYS_HPP_

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gfcpp_globals.hpp"

namespace gemfire {
namespace serializer {

template <typename TObj>
inline bool equals(const TObj& x, const TObj& y) {
  return (x == y);
}

inline uint32_t hashcode(const bool value) {
  if (value)
    return 1231;
  else
    return 1237;
}

inline uint32_t hashcode(const uint8_t value) { return (uint32_t)value; }

inline uint32_t hashcode(const int8_t value) { return (uint32_t)value; }

inline uint32_t hashcode(const uint16_t value) { return (uint32_t)value; }

inline uint32_t hashcode(const int16_t value) { return (uint32_t)value; }

inline uint32_t hashcode(const uint32_t value) { return value; }

inline uint32_t hashcode(const int32_t value) { return (uint32_t)value; }

inline uint32_t hashcode(const uint64_t value) {
  uint32_t hash = (uint32_t)value;
  hash = hash ^ (uint32_t)(value >> 32);
  return hash;
}

inline uint32_t hashcode(const int64_t value) {
  uint32_t hash = (uint32_t)value;
  hash = hash ^ (uint32_t)(value >> 32);
  return hash;
}

inline uint32_t hashcode(const float value) {
  union float_uint32_t {
    float f;
    uint32_t u;
  } v;
  v.f = value;
  return v.u;
}

inline uint32_t hashcode(const double value) {
  union double_uint64_t {
    double d;
    uint64_t u;
  } v;
  v.d = value;
  return hashcode(v.u);
}
}
}

#endif  // _GEMFIRE_CACHEABLEKEYS_HPP_
