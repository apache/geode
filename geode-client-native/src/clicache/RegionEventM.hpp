/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include "cppcache/RegionEvent.hpp"
#include "impl/NativeWrapper.hpp"
#include "IGFSerializable.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      ref class Region;

      /// <summary>
      /// This class encapsulates events that occur for a region.
      /// </summary>
      [Obsolete("Use classes and APIs from the GemStone.GemFire.Cache.Generic namespace")]
      public ref class RegionEvent sealed
        : public Internal::UMWrap<gemfire::RegionEvent>
      {
      public:

        /// <summary>
        /// Constructor to create a <c>RegionEvent</c> for a given region.
        /// </summary>
        /// <exception cref="IllegalArgumentException">
        /// if region is null
        /// </exception>
        RegionEvent(Region^ region, IGFSerializable^ aCallbackArgument,
          bool remoteOrigin);

        /// <summary>
        /// Return the region this event occurred in.
        /// </summary>
        property Region^ Region
        {
          GemStone::GemFire::Cache::Region^ get( );
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
        inline RegionEvent( const gemfire::RegionEvent* nativeptr )
          : UMWrap( const_cast<gemfire::RegionEvent*>( nativeptr ), false ) { }
      };

    }
  }
}
