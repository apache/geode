/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "gf_includes.hpp"
#include "CacheStatistics.hpp"


namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      uint32_t CacheStatistics::LastModifiedTime::get( )
      {
        return NativePtr->getLastModifiedTime( );
      }

      uint32_t CacheStatistics::LastAccessedTime::get( )
      {
        return NativePtr->getLastAccessedTime( );
      }

    }
  }
}
 } //namespace 
