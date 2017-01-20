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
#include "EntryEvent.hpp"
#include "Region.hpp"
#include "impl/SafeConvert.hpp"

using namespace System;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {
namespace Generic
    {

      generic<class TKey, class TValue>
      EntryEvent<TKey, TValue>::EntryEvent(IRegion<TKey, TValue>^ region,
        TKey key, TValue oldValue,
        TValue newValue, Object^ aCallbackArgument,
        bool remoteOrigin)
        : UMWrap( )
      {
        //TODO:: from where this gets called
        /*apache::geode::client::RegionPtr regionptr( GetNativePtr<apache::geode::client::Region>( region ) );
        apache::geode::client::CacheableKeyPtr keyptr( SafeMKeyConvert( key ) );
        apache::geode::client::CacheablePtr oldptr( SafeMSerializableConvert( oldValue ) );
        apache::geode::client::CacheablePtr newptr( SafeMSerializableConvert( newValue ) );
        apache::geode::client::UserDataPtr callbackptr(SafeMSerializableConvert(
            aCallbackArgument));

        SetPtr(new apache::geode::client::EntryEvent(regionptr, keyptr,
          oldptr, newptr, callbackptr, remoteOrigin), true);*/
      }

      generic<class TKey, class TValue>
      IRegion<TKey, TValue>^ EntryEvent<TKey, TValue>::Region::get( )
      {
        apache::geode::client::RegionPtr& regionptr( NativePtr->getRegion( ) );
        return Generic::Region<TKey, TValue>::Create( regionptr.ptr( ) );
      }

      generic<class TKey, class TValue>
      TKey EntryEvent<TKey, TValue>::Key::get( )
      {
        apache::geode::client::CacheableKeyPtr& keyptr( NativePtr->getKey( ) );
        return Serializable::GetManagedValueGeneric<TKey>( keyptr );
      }

      generic<class TKey, class TValue>
      TValue EntryEvent<TKey, TValue>::OldValue::get( )
      {
        apache::geode::client::CacheablePtr& valptr( NativePtr->getOldValue( ) );
        return Serializable::GetManagedValueGeneric<TValue>( valptr );
      }

      generic<class TKey, class TValue>
      TValue EntryEvent<TKey, TValue>::NewValue::get( )
      {
        apache::geode::client::CacheablePtr& valptr( NativePtr->getNewValue( ) );
        return Serializable::GetManagedValueGeneric<TValue>( valptr );
      }

      generic<class TKey, class TValue>
      Object^ EntryEvent<TKey, TValue>::CallbackArgument::get()
      {
        apache::geode::client::UserDataPtr& valptr(NativePtr->getCallbackArgument());
        return Serializable::GetManagedValueGeneric<Object^>( valptr );
      }

      generic<class TKey, class TValue>
      bool EntryEvent<TKey, TValue>::RemoteOrigin::get( )
      {
        return NativePtr->remoteOrigin( );
      }

    }
  }
}
 } //namespace 
