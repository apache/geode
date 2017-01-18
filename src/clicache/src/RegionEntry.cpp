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
        apache::geode::client::CacheableKeyPtr& nativeptr( NativePtr->getKey( ) );
        
        return Serializable::GetManagedValueGeneric<TKey>( nativeptr );
      }

      generic<class TKey, class TValue>
      TValue RegionEntry<TKey, TValue>::Value::get( )
      {
        apache::geode::client::CacheablePtr& nativeptr( NativePtr->getValue( ) );

        return Serializable::GetManagedValueGeneric<TValue>( nativeptr );
      }

      generic<class TKey, class TValue>
      IRegion<TKey, TValue>^ RegionEntry<TKey, TValue>::Region::get( )
      {
        apache::geode::client::RegionPtr rptr;

        NativePtr->getRegion( rptr );
        return GemStone::GemFire::Cache::Generic::Region<TKey, TValue>::Create( rptr.ptr( ) );
      }

      generic<class TKey, class TValue>
      GemStone::GemFire::Cache::Generic::CacheStatistics^ RegionEntry<TKey, TValue>::Statistics::get( )
      {
        apache::geode::client::CacheStatisticsPtr nativeptr;

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
