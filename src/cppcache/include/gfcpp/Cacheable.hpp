#ifndef __GEMFIRE_CACHEABLE_H__
#define __GEMFIRE_CACHEABLE_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

/**
 * @file
 */

#include "gfcpp_globals.hpp"
#include "Serializable.hpp"

namespace gemfire {

typedef SerializablePtr CacheablePtr;
typedef Serializable Cacheable;

template <typename TVALUE>
inline CacheablePtr createValue(const SharedPtr<TVALUE>& value);

template <typename TVALUE>
inline CacheablePtr createValue(const TVALUE* value);

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_CACHEABLE_H__
