/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"


namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      interface class CqListener;

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
      /// the listener are caught by GemFire and logged. 
      ///
      /// Listeners are user callbacks that
      /// are invoked by GemFire. It is important to ensure that minimal work is done in the
      /// listener before returning control back to GemFire. For example, a listener
      /// implementation may choose to hand off the event to a thread pool that then processes
      /// the event on its thread rather than the listener thread
      /// </remarks>
      /// <seealso cref="AttributesFactory.SetCacheListener" />
      /// <seealso cref="RegionAttributes.CacheListener" />
      /// <seealso cref="ICacheLoader" />
      /// <seealso cref="ICacheWriter" />
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
        void OnEvent( CqEvent^ ev );

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
        void OnError( CqEvent^ ev );


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

    }
  }
}
 } //namespace 
