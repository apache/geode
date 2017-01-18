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
#include "RegionEvent.hpp"
#include "Region.hpp"
#include "IGFSerializable.hpp"
#include "impl/SafeConvert.hpp"
using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      generic<class TKey, class TValue>
      RegionEvent<TKey, TValue>::RegionEvent(Generic::IRegion<TKey, TValue>^ region,
        Object^ aCallbackArgument, bool remoteOrigin)
        : UMWrap( )
      {
        //TODO:: do we neeed this
        /*if ( region == nullptr ) {
          throw gcnew IllegalArgumentException( "RegionEvent.ctor(): "
            "null region passed" );
        }

        apache::geode::client::UserDataPtr callbackptr(SafeMSerializableConvert(
            aCallbackArgument));

        SetPtr(new apache::geode::client::RegionEvent(apache::geode::client::RegionPtr(region->_NativePtr),
          callbackptr, remoteOrigin), true);*/
      }

      generic<class TKey, class TValue>
      IRegion<TKey, TValue>^ RegionEvent<TKey, TValue>::Region::get( )
      {
        apache::geode::client::RegionPtr& regionptr( NativePtr->getRegion( ) );

        return Generic::Region<TKey, TValue>::Create( regionptr.ptr( ) );
      }

      generic<class TKey, class TValue>
      Object^ RegionEvent<TKey, TValue>::CallbackArgument::get()
      {
        apache::geode::client::UserDataPtr& valptr(NativePtr->getCallbackArgument());
        return Serializable::GetManagedValueGeneric<Object^>( valptr );
      }

      generic<class TKey, class TValue>
      bool RegionEvent<TKey, TValue>::RemoteOrigin::get( )
      {
        return NativePtr->remoteOrigin( );
      }

    }
  }
}
 } //namespace 
