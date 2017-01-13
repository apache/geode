/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include <gfcpp/RegionEvent.hpp>
//#include "impl/NativeWrapper.hpp"
#include "IGFSerializable.hpp"
#include "IRegion.hpp"
#include "Region.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      //ref class Region;

      /// <summary>
      /// This class encapsulates events that occur for a region.
      /// </summary>
      generic<class TKey, class TValue>
      public ref class RegionEvent sealed
        : public Generic::Internal::UMWrap<gemfire::RegionEvent>
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
        inline GemStone::GemFire::Cache::Generic::RegionEvent<TKey, TValue>( const gemfire::RegionEvent* nativeptr )
          : GemStone::GemFire::Cache::Generic::Internal::UMWrap<gemfire::RegionEvent>(
            const_cast<gemfire::RegionEvent*>( nativeptr ), false ) { }
      };

    }
  }
}
} //namespace 
