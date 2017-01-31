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
#include "CqEvent.hpp"
using namespace System;
namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      generic<class TKey, class TResult>
      ref class CqEvent;

      /// <summary>
      /// An application plug-in that can be installed on a region.
      /// Listener change notifications are invoked <c>after</c>
      /// the change has occured.
      /// </summary>
      /// <remarks>
      /// Listeners receive notifications when entries in a region change or changes occur to the
      /// region attributes themselves.
      /// <para>
      /// A cache listener is defined in the <see cref="RegionAttributes" />.
      /// </para>
      /// The methods on a <c>ICacheListener</c>
      /// are invoked asynchronously. Multiple events can cause concurrent invocation
      /// of <c>ICacheListener</c> methods.  If event A occurs before event B,
      /// there is no guarantee that their corresponding <c>ICacheListener</c>
      /// method invocations will occur in the same order.  Any exceptions thrown by
      /// the listener are caught by Geode and logged. 
      ///
      /// Listeners are user callbacks that
      /// are invoked by Geode. It is important to ensure that minimal work is done in the
      /// listener before returning control back to Geode. For example, a listener
      /// implementation may choose to hand off the event to a thread pool that then processes
      /// the event on its thread rather than the listener thread
      /// </remarks>
      /// <seealso cref="AttributesFactory.SetCacheListener" />
      /// <seealso cref="RegionAttributes.CacheListener" />
      /// <seealso cref="ICacheLoader" />
      /// <seealso cref="ICacheWriter" />
      //generic<class TKey, class TValue>
      generic<class TKey, class TResult>
      public interface class ICqListener
      {
      public:

        /// <summary>
        /// Handles the event of a new key being added to a region.
        /// </summary>
        /// <remarks>
        /// The entry did not previously exist in this region in the local cache
        /// (even with a null value).
        /// <para>
        /// This function does not throw any exception.
        /// </para>
        /// </remarks>
        /// <param name="ev">
        /// Denotes the event object associated with the entry creation.
        /// </param>
        /// <seealso cref="Region.Create" />
        /// <seealso cref="Region.Put" />
        /// <seealso cref="Region.Get" />
        void OnEvent(CqEvent<TKey, TResult>^ ev);

        /// <summary>
        /// Handles the event of an entry's value being modified in a region.
        /// </summary>
        /// <remarks>
        /// This entry previously existed in this region in the local cache,
        /// but its previous value may have been null.
        /// </remarks>
        /// <param name="ev">
        /// EntryEvent denotes the event object associated with updating the entry.
        /// </param>
        /// <seealso cref="Region.Put" />
        //generic<class TKey, class TValue>
        void OnError(CqEvent<TKey, TResult>^ ev);


        /// <summary>
        /// Called when the region containing this callback is destroyed, when
        /// the cache is closed.
        /// </summary>
        /// <remarks>
        /// Implementations should clean up any external resources,
        /// such as database connections. Any runtime exceptions this method
        /// throws will be logged.
        /// <para>
        /// It is possible for this method to be called multiple times on a single
        /// callback instance, so implementations must be tolerant of this.
        /// </para>
        /// </remarks>
        /// <seealso cref="Cache.Close" />
        /// <seealso cref="Region.DestroyRegion" />
        void Close();
      };
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache

