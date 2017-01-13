/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include "IRegion.hpp"
//#include "Region.hpp"

#include "EntryEvent.hpp"
#include "RegionEvent.hpp"

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      /// <summary>
      /// An application plug-in that can be installed on a region.
      /// </summary>
      /// <remarks>
      /// Listeners are change notifications that are invoked
      /// AFTER the change has occured for region update operations on a client.
      /// Listeners also receive notifications when entries in a region are modified.
      /// Multiple events can cause concurrent invocation
      /// of <c>ICacheListener</c> methods.  If event A occurs before event B,
      /// there is no guarantee that their corresponding <c>ICacheListener</c>
      /// method invocations will occur in the same order. Any exceptions thrown by
      /// the listener are caught by GemFire and logged. If the exception is due to
      /// listener invocation on the same thread where a region operation has been
      /// performed, then a <c>CacheListenerException</c> is thrown back to
      /// the application. If the exception is for a notification received from
      /// server then that is logged and the notification thread moves on to
      /// receiving other notifications.
      /// <para>
      /// A cache listener is defined in the <see cref="RegionAttributes" />.
      /// </para>
      ///
      /// There are two cases in which listeners are invoked. The first is when a
      /// region modification operation (e.g. put, create, destroy, invalidate)
      /// is performed. For this case it is important to ensure that minimal work is
      /// done in the listener before returning control back to Gemfire since the
      /// operation will block till the listener has not completed. For example,
      /// a listener implementation may choose to hand off the event to a thread pool
      /// that then processes the event on its thread rather than the listener thread.
      /// The second is when notifications are received from java server as a result
      /// of region register interest calls (<c>Region.RegisterKeys</c> etc),
      /// or invalidate notifications when notify-by-subscription is false on the
      /// server. In this case the methods of <c>ICacheListener</c> are invoked
      /// asynchronously (i.e. does not block the thread that receives notification
      /// messages). Additionally for the latter case of notifications from server,
      /// listener is always invoked whether or not local operation is successful
      /// e.g. if a destroy notification is received and key does not exist in the
      /// region, the listener will still be invoked. This is different from the
      /// first case where listeners are invoked only when the region update
      /// operation succeeds.
      /// </remarks>
      /// <seealso cref="AttributesFactory.SetCacheListener" />
      /// <seealso cref="RegionAttributes.CacheListener" />
      /// <seealso cref="ICacheLoader" />
      /// <seealso cref="ICacheWriter" />
      /// <seealso cref="CacheListenerException" />
      generic<class TKey, class TValue>
      public interface class ICacheListener
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
        void AfterCreate( EntryEvent<TKey, TValue>^ ev );

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
        void AfterUpdate( EntryEvent<TKey, TValue>^ ev );

        /// <summary>
        /// Handles the event of an entry's value being invalidated.
        /// </summary>
        /// <param name="ev">
        /// EntryEvent denotes the event object associated with the entry invalidation.
        /// </param>
        void AfterInvalidate( EntryEvent<TKey, TValue>^ ev );

        /// <summary>
        /// Handles the event of an entry being destroyed.
        /// </summary>
        /// <param name="ev">
        /// EntryEvent denotes the event object associated with the entry destruction.
        /// </param>
        /// <seealso cref="Region.Destroy" />
        void AfterDestroy( EntryEvent<TKey, TValue>^ ev );

        /// <summary>
        /// Handles the event of a region being cleared.
        /// </summary>
        void AfterRegionClear( RegionEvent<TKey, TValue>^ ev );

        /// <summary>
        /// Handles the event of a region being invalidated.
        /// </summary>
        /// <remarks>
        /// Events are not invoked for each individual value that is invalidated
        /// as a result of the region being invalidated. Each subregion, however,
        /// gets its own <c>RegionInvalidated</c> event invoked on its listener.
        /// </remarks>
        /// <param name="ev">
        /// RegionEvent denotes the event object associated with the region invalidation.
        /// </param>
        /// <seealso cref="Region.InvalidateRegion" />
        void AfterRegionInvalidate( RegionEvent<TKey, TValue>^ ev );

        /// <summary>
        /// Handles the event of a region being destroyed.
        /// </summary>
        /// <remarks>
        /// Events are not invoked for each individual entry that is destroyed
        /// as a result of the region being destroyed. Each subregion, however,
        /// gets its own <c>AfterRegionDestroyed</c> event invoked on its listener.
        /// </remarks>
        /// <param name="ev">
        /// RegionEvent denotes the event object associated with the region destruction.
        /// </param>
        /// <seealso cref="Region.DestroyRegion" />
        void AfterRegionDestroy( RegionEvent<TKey, TValue>^ ev );

        /// <summary>
        /// Handles the event of a region going live.
        /// </summary>
        /// <remarks>
        /// Each subregion gets its own <c>AfterRegionLive</c> event invoked on its listener.
        /// </remarks>
        /// <param name="ev">
        /// RegionEvent denotes the event object associated with the region going live.
        /// </param>
        /// <seealso cref="Cache.ReadyForEvents" />
        void AfterRegionLive( RegionEvent<TKey, TValue>^ ev );

        /// <summary>
        /// Called when the region containing this callback is destroyed, when
        /// the cache is closed.
        /// </summary>
        /// <remarks>
        /// Implementations should clean up any external resources,
        /// such as database connections. Any runtime exceptions this method
        /// throws will be logged.
        /// </remarks>
        /// <param>
        /// It is possible for this method to be called multiple times on a single
        /// callback instance, so implementations must be tolerant of this.
        /// </param>
        /// <seealso cref="Cache.Close" />
        /// <seealso cref="Region.DestroyRegion" />
        void Close( IRegion<TKey, TValue>^ region );

        ///<summary>
        ///Called when all the endpoints associated with region are down.
        ///This will be called when all the endpoints are down for the first time.
        ///If endpoints come up and again go down it will be called again.
        ///This will also be called when all endpoints are down and region is attached to the pool.
        ///</summary>
        ///<remarks>
        ///</remark>
        ///<param>
        ///region Region^ denotes the assosiated region.
        ///</param>
        void AfterRegionDisconnected( IRegion<TKey, TValue>^ region );
      };

    }
  }
}
 } //namespace 
