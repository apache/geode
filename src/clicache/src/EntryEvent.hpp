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
#include <gfcpp/EntryEvent.hpp>
#include "impl/NativeWrapper.hpp"
#include "IRegion.hpp"
//#include "Region.hpp"

using namespace System;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {
namespace Generic
      {
        interface class IGFSerializable;

     // ref class Region;
      //interface class ICacheableKey;

      /// <summary>
      /// This class encapsulates events that occur for an entry in a region.
      /// </summary>
      generic<class TKey, class TValue>
      public ref class EntryEvent sealed
        : public Internal::UMWrap<apache::geode::client::EntryEvent>
      {
      public:

        /// <summary>
        /// Constructor to create an <c>EntryEvent</c> for the given region.
        /// </summary>
        EntryEvent(IRegion<TKey, TValue>^ region, TKey key, TValue oldValue,
          TValue newValue, Object^ aCallbackArgument,
          bool remoteOrigin );

        /// <summary>
        /// Return the region this event occurred in.
        /// </summary>
        property IRegion<TKey, TValue>^ Region
        {
          IRegion<TKey, TValue>^ get( );
        }

        /// <summary>
        /// Returns the key this event describes.
        /// </summary>
        property TKey Key
        {
          TKey get( );
        }

        /// <summary>
        /// Returns 'null' if there was no value in the cache. If the prior state
        ///  of the entry was invalid, or non-existent/destroyed, then the old
        /// value will be 'null'.
        /// </summary>
        property TValue OldValue
        {
          TValue get( );
        }

        /// <summary>
        /// Return the updated value from this event. If the event is a destroy
        /// or invalidate operation, then the new value will be NULL.
        /// </summary>
        property TValue NewValue
        {
          TValue get( );
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
        /// If the event originated in a remote process, returns true.
        /// </summary>
        property bool RemoteOrigin
        {
          bool get( );
        }


      internal:

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline EntryEvent<TKey, TValue>( const apache::geode::client::EntryEvent* nativeptr )
          : Internal::UMWrap<apache::geode::client::EntryEvent>(
            const_cast<apache::geode::client::EntryEvent*>( nativeptr ), false ) { }
      };

    }
  }
}
 } //namespace 
