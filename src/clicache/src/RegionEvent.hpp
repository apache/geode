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

#pragma once

#include "gf_defs.hpp"
#include <gfcpp/RegionEvent.hpp>
//#include "impl/NativeWrapper.hpp"
#include "IGFSerializable.hpp"
#include "IRegion.hpp"
#include "Region.hpp"

using namespace System;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {
namespace Generic
    {

      //ref class Region;

      /// <summary>
      /// This class encapsulates events that occur for a region.
      /// </summary>
      generic<class TKey, class TValue>
      public ref class RegionEvent sealed
        : public Generic::Internal::UMWrap<apache::geode::client::RegionEvent>
      {
      public:

        /// <summary>
        /// Constructor to create a <c>RegionEvent</c> for a given region.
        /// </summary>
        /// <exception cref="IllegalArgumentException">
        /// if region is null
        /// </exception>
        RegionEvent(IRegion<TKey, TValue>^ region, Object^ aCallbackArgument,
          bool remoteOrigin);

        /// <summary>
        /// Return the region this event occurred in.
        /// </summary>
        property IRegion<TKey, TValue>^ Region
        {
          IRegion<TKey, TValue>^ get( );
        }

        /// <summary>
        /// Returns the callbackArgument passed to the method that generated
        /// this event. See the <see cref="Region" /> interface methods
        /// that take a callbackArgument parameter.
        /// </summary>
        property Object^ CallbackArgument
        {
          Object^ get();
        }

        /// <summary>
        /// Returns true if the event originated in a remote process.
        /// </summary>
        property bool RemoteOrigin
        {
          bool get( );
        }


      internal:

        /// <summary>
        /// Internal constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline Apache::Geode::Client::Generic::RegionEvent<TKey, TValue>( const apache::geode::client::RegionEvent* nativeptr )
          : Apache::Geode::Client::Generic::Internal::UMWrap<apache::geode::client::RegionEvent>(
            const_cast<apache::geode::client::RegionEvent*>( nativeptr ), false ) { }
      };

    }
  }
}
} //namespace 
