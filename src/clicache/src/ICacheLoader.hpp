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
#include <gfcpp/CacheLoader.hpp>
#include "IRegion.hpp"
//#include "Region.hpp"
//#include "ICacheableKey.hpp"

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      //interface class ICacheableKey;

      /// <summary>
      /// CacheLoader
      /// </summary>
      /// <remarks>
      /// CacheLoader
      /// </remarks>
      public ref class CacheLoader STATICCLASS
      {
      };

      /// <summary>
      /// A data-loading application plug-in that can be installed on a region.
      /// </summary>
      /// <remarks>
      /// Loaders facilitate loading of data into the cache from a third-party data source. 
      /// When an application does a
      /// lookup for a key in a region and it does not exist, Geode checks to
      /// see if any loaders are available for the region in the system and
      /// invokes them to get the value for the key into the cache.
      /// <para>
      /// A cache loader is defined in the <see cref="RegionAttributes" />.
      /// </para>
      /// When <see cref="Region.Get" /> is called for a region
      /// entry that has a null value, the <see cref="ICacheLoader.Load" />
      /// method of the region's cache loader is invoked.  The <c>Load</c> method
      /// creates the value for the desired key by performing an operation such
      /// as a database query. 
      /// </remarks>
      /// <seealso cref="AttributesFactory.SetCacheLoader" />
      /// <seealso cref="RegionAttributes.CacheLoader" />
      /// <seealso cref="ICacheListener" />
      /// <seealso cref="ICacheWriter" />
      generic<class TKey, class TValue>
      public interface class ICacheLoader
      {
      public:

        /// <summary>
        /// Loads a value. Application writers should implement this
        /// method to customize the loading of a value.
        /// </summary>
        /// <remarks>
        /// This method is called
        /// by the caching service when the requested value is not in the cache.
        /// Any exception thrown by this method is propagated back to and thrown
        /// by the invocation of <see cref="Region.Get" /> that triggered this load.
        /// </remarks>
        /// <param name="region">a Region for which this is called.</param>
        /// <param name="key">the key for the cacheable</param>
        /// <param name="callbackArgument">
        /// </param>
        /// <returns>
        /// the value supplied for this key, or null if no value can be
        /// supplied. 
        /// If every available loader returns
        /// a null value, <see cref="Region.Get" /> will return null.
        /// </returns>
        /// <seealso cref="Region.Get" />
        TValue Load(IRegion<TKey, TValue>^ region, TKey key,
                    Object^ callbackArgument);

        /// <summary>
        /// Called when the region containing this callback is destroyed, when
        /// the cache is closed.
        /// </summary>
        /// <remarks>
        /// Implementations should clean up any external resources, such as
        /// database connections. Any runtime exceptions this method throws will be logged.
        /// <para>
        /// It is possible for this method to be called multiple times on a single
        /// callback instance, so implementations must be tolerant of this.
        /// </para>
        /// </remarks>
        /// <seealso cref="Cache.Close" />
        /// <seealso cref="Region.DestroyRegion" />
        void Close(IRegion<TKey, TValue>^ region);
      };
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache

