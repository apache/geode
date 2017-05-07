#ifndef __GEMFIRE_CACHEABLEKEY_I__
#define __GEMFIRE_CACHEABLEKEY_I__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "CacheableKey.hpp"

/**
 * @file
 */

namespace gemfire
{

template< class PRIM >
inline CacheableKeyPtr CacheableKey::create( const PRIM value )
{
  return createKey( value );
}

template <class TKEY>
inline CacheableKeyPtr createKey( const SharedPtr< TKEY >& value )
{
  return CacheableKeyPtr( value );
}

template <typename TKEY>
inline CacheableKeyPtr createKey( const TKEY* value )
{
  return createKeyArr( value );
}

}

#endif
