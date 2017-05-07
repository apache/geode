/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */



#pragma once

#include "../../gf_defs.hpp"
#include <cppcache/CacheAttributes.hpp>
#include "impl/NativeWrapperN.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      /// <summary>
      /// Defines attributes for configuring a cache.
      /// </summary>
      /// <remarks>
      /// Currently the following attributes are defined:
      /// <c>redundancyLevel</c>: Redundancy for HA client queues.
      /// <c>endpoints</c>: Cache level endpoints list.
      /// To create an instance of this interface, use
      /// <see cref="CacheAttributesFactory.CreateCacheAttributes" />.
      ///
      /// For compatibility rules and default values, see
      /// <see cref="CacheAttributesFactory" />.
      ///
      /// Note that the <c>CacheAttributes</c> are not distributed with
      /// the region.
      /// </remarks>
      /// <seealso cref="CacheAttributesFactory" />
      public ref class CacheAttributes sealed
        : public Internal::SBWrap<gemfire::CacheAttributes>
      {
      public:

        /// <summary>
        /// Gets redundancy level for regions in the cache.
        /// </summary>
        property int32_t RedundancyLevel
        {
          int32_t get( );
        }

        /// <summary>
        /// Gets cache level endpoints list.
        /// </summary>
        property String^ Endpoints
        {
          String^ get( );
        }


      internal:

        /// <summary>
        /// Internal factory function to wrap a native object pointer inside
        /// this managed class with null pointer check.
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        /// <returns>
        /// The managed wrapper object; null if the native pointer is null.
        /// </returns>
        inline static CacheAttributes^ Create(
          gemfire::CacheAttributes* nativeptr )
        {
          return ( nativeptr != nullptr ?
            gcnew CacheAttributes( nativeptr ) : nullptr );
        }


      private:

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline CacheAttributes( gemfire::CacheAttributes* nativeptr )
          : SBWrap( nativeptr ) { }
      };

    }
  }
}
 } //namespace 

