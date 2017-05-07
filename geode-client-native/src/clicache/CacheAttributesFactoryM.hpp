/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include "cppcache/CacheAttributesFactory.hpp"
#include "impl/NativeWrapper.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      ref class CacheAttributes;

      /// <summary>
      /// Creates instances of <c>CacheAttributes</c>.
      /// </summary>
      /// <seealso cref="CacheAttributes" />
      [Obsolete("Use classes and APIs from the GemStone.GemFire.Cache.Generic namespace")]
      public ref class CacheAttributesFactory sealed
        : public Internal::UMWrap<gemfire::CacheAttributesFactory>
      {
      public:

        /// <summary>
        /// Creates a new instance of <c>CacheAttributesFactory</c> ready
        /// to create a <c>CacheAttributes</c> with default settings.
        /// </summary>
        inline CacheAttributesFactory( )
          : UMWrap( new gemfire::CacheAttributesFactory( ), true )
        { }

        // ATTRIBUTES

        /// <summary>
        /// Sets redundancy level to use for regions in the cache.
        /// </summary>
        [Obsolete("This method is obsolete since 3.5; use PoolFactory.SetSubscriptionRedundancy instead.")]
        void SetRedundancyLevel( int32_t redundancyLevel );

        /// <summary>
        /// Sets endpoints list to be used at the cache-level.
        /// </summary>
        [Obsolete("This method is obsolete since 3.5; use PoolFactory.AddServer or PoolFactory.AddLocator instead.")]
        void SetEndpoints( String^ endpoints );

        // FACTORY METHOD

        /// <summary>
        /// Creates a <c>CacheAttributes</c> with the current settings.
        /// </summary>
        /// <returns>The newly created <c>CacheAttributes</c></returns>
        /// <exception cref="IllegalStateException">
        /// if the current settings violate the <a href="compability.html">
        /// compatibility</a> rules.
        /// </exception>
        CacheAttributes^ CreateCacheAttributes( );
      };

    }
  }
}
