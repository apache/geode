/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include "IRegionService.hpp"
#include "DistributedSystemM.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      /// <summary>
      /// GemFireCache represents the singleton cache that must be created
      /// in order to connect to Gemfire server.
      /// </summary>
      /// <remarks>
      /// Caches are obtained from Crest methods on the
      /// <see cref="CacheFactory.Create"/> class.
      /// <para>
      /// When a cache is created a <see cref="DistributedSystem" />
      /// must be specified.
      /// </para><para>
      /// When a cache will no longer be used, call <see cref="Cache.Close" />.
      /// Once it <see cref="Cache.IsClosed" /> any attempt to use it
      /// will cause a <c>CacheClosedException</c> to be thrown.
      /// </para><para>
      /// A cache can have multiple root regions, each with a different name.
      /// </para>
      /// </remarks>
      [Obsolete("Use classes and APIs from the GemStone.GemFire.Cache.Generic namespace")]
      public interface class IGemFireCache : IRegionService
      {
      public:        
        
        /// <summary>
        /// Returns the name of this cache.
        /// </summary>
        /// <remarks>
        /// This method does not throw
        /// <c>CacheClosedException</c> if the cache is closed.
        /// </remarks>
        /// <returns>the string name of this cache</returns>
        property String^ Name
        {
          String^ get( );
        }

        /// <summary>
        /// Initializes the cache from an XML file.
        /// </summary>
        /// <param name="cacheXml">pathname of a <c>cache.xml</c> file</param>
        void InitializeDeclarativeCache( String^ cacheXml );

        /// <summary>
        /// Returns the distributed system used to
        /// <see cref="CacheFactory.Create" /> this cache.
        /// </summary>
        /// <remarks>
        /// This method does not throw
        /// <c>CacheClosedException</c> if the cache is closed.
        /// </remarks>
        property DistributedSystem^ DistributedSystem
        {
          GemStone::GemFire::Cache::DistributedSystem^ get( );
        } 
      };
    }
  }
}
