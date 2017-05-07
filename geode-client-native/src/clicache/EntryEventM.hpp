/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include "cppcache/EntryEvent.hpp"
#include "impl/NativeWrapper.hpp"

#include "ICacheableKey.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      ref class Region;
      interface class IGFSerializable;
      //interface class ICacheableKey;

      /// <summary>
      /// This class encapsulates events that occur for an entry in a region.
      /// </summary>
      [Obsolete("Use classes and APIs from the GemStone.GemFire.Cache.Generic namespace")]
      public ref class EntryEvent sealed
        : public Internal::UMWrap<gemfire::EntryEvent>
      {
      public:

        /// <summary>
        /// Constructor to create an <c>EntryEvent</c> for the given region.
        /// </summary>
        EntryEvent( Region^ region, ICacheableKey^ key, IGFSerializable^ oldValue,
          IGFSerializable^ newValue, IGFSerializable^ aCallbackArgument,
          bool remoteOrigin );

        /// <summary>
        /// Return the region this event occurred in.
        /// </summary>
        property Region^ Region
        {
          GemStone::GemFire::Cache::Region^ get( );
        }

        /// <summary>
        /// Returns the key this event describes.
        /// </summary>
        property ICacheableKey^ Key
        {
          ICacheableKey^ get( );
        }

        /// <summary>
        /// Returns 'null' if there was no value in the cache. If the prior state
        ///  of the entry was invalid, or non-existent/destroyed, then the old
        /// value will be 'null'.
        /// </summary>
        property IGFSerializable^ OldValue
        {
          IGFSerializable^ get( );
        }

        /// <summary>
        /// Return the updated value from this event. If the event is a destroy
        /// or invalidate operation, then the new value will be NULL.
        /// </summary>
        property IGFSerializable^ NewValue
        {
          IGFSerializable^ get( );
        }

        /// <summary>
        /// Returns the callbackArgument passed to the method that generated
        /// this event. See the <see cref="Region" /> interface methods
        /// that take a callbackArgument parameter.
        /// </summary>
        property IGFSerializable^ CallbackArgument
        {
          IGFSerializable^ get();
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
        inline EntryEvent( const gemfire::EntryEvent* nativeptr )
          : UMWrap( const_cast<gemfire::EntryEvent*>( nativeptr ), false ) { }
      };

    }
  }
}
