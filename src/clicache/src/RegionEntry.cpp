/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "gf_includes.hpp"
#include "RegionEntry.hpp"
#include "Region.hpp"
#include "CacheStatistics.hpp"
#include "impl/SafeConvert.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      generic<class TKey, class TValue>
      TKey RegionEntry<TKey, TValue>::Key::get( )
      {
        gemfire::CacheableKeyPtr& nativeptr( NativePtr->getKey( ) );
        
        return Serializable::GetManagedValueGeneric<TKey>( nativeptr );
      }

      generic<class TKey, class TValue>
      TValue RegionEntry<TKey, TValue>::Value::get( )
      {
        gemfire::CacheablePtr& nativeptr( NativePtr->getValue( ) );

        return Serializable::GetManagedValueGeneric<TValue>( nativeptr );
      }

      generic<class TKey, class TValue>
      IRegion<TKey, TValue>^ RegionEntry<TKey, TValue>::Region::get( )
      {
        gemfire::RegionPtr rptr;

        NativePtr->getRegion( rptr );
        return GemStone::GemFire::Cache::Generic::Region<TKey, TValue>::Create( rptr.ptr( ) );
      }

      generic<class TKey, class TValue>
      GemStone::GemFire::Cache::Generic::CacheStatistics^ RegionEntry<TKey, TValue>::Statistics::get( )
      {
        gemfire::CacheStatisticsPtr nativeptr;

        NativePtr->getStatistics( nativeptr );
        return GemStone::GemFire::Cache::Generic::CacheStatistics::Create( nativeptr.ptr( ) );
      }

      generic<class TKey, class TValue>
      bool RegionEntry<TKey, TValue>::IsDestroyed::get( )
      {
        return NativePtr->isDestroyed( );
      }

    }
  }
}
 } //namespace 
