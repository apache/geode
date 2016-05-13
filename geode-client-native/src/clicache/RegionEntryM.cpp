/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "RegionEntryM.hpp"
#include "RegionM.hpp"
#include "CacheStatisticsM.hpp"
#include "impl/SafeConvert.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      GemStone::GemFire::Cache::ICacheableKey^ RegionEntry::Key::get( )
      {
        gemfire::CacheableKeyPtr& nativeptr( NativePtr->getKey( ) );

        return SafeUMKeyConvert( nativeptr.ptr( ) );
      }

      IGFSerializable^ RegionEntry::Value::get( )
      {
        gemfire::CacheablePtr& nativeptr( NativePtr->getValue( ) );

        return SafeUMSerializableConvert( nativeptr.ptr( ) );
      }

      GemStone::GemFire::Cache::Region^ RegionEntry::Region::get( )
      {
        gemfire::RegionPtr rptr;

        NativePtr->getRegion( rptr );
        return GemStone::GemFire::Cache::Region::Create( rptr.ptr( ) );
      }

      CacheStatistics^ RegionEntry::Statistics::get( )
      {
        gemfire::CacheStatisticsPtr nativeptr;

        NativePtr->getStatistics( nativeptr );
        return CacheStatistics::Create( nativeptr.ptr( ) );
      }

      bool RegionEntry::IsDestroyed::get( )
      {
        return NativePtr->isDestroyed( );
      }

    }
  }
}
