#ifndef __GEMFIRE_CACHEABLE_I__
#define __GEMFIRE_CACHEABLE_I__
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

#include "Cacheable.hpp"

namespace gemfire {

template< class PRIM >
inline CacheablePtr Cacheable::create( const PRIM value )
{
  return createValue( value );
}

template <typename TVALUE>
inline CacheablePtr createValue( const SharedPtr< TVALUE >& value )
{
  return CacheablePtr( value );
}

template <typename TVALUE>
inline CacheablePtr createValue( const TVALUE* value )
{
  return createValueArr( value );
}

} //namespace gemfire

#endif //ifndef __GEMFIRE_CACHEABLE_I__
