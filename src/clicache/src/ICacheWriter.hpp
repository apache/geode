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
      /// Defines methods that are called <b>before</b> entry modification,
      /// such as writing the value to a database.
      /// </summary>
      /// <remarks>
      /// <para>
      /// A distributed region will typically have a single cache writer.
      /// If the application is designed such that all or most updates to
      /// a region occur on a node, the cache writer for the region should
      /// be installed at that node. 
      /// </para><para>
      /// A cache writer is defined in the <see cref="RegionAttributes" />.
      /// </para><para>
      /// Cache writer invocations are initiated by the node where the entry or
      /// region modification occurs. 
      /// </para><para>
      /// Before a region is updated via a put, create, or destroy operation,
      /// GemFire will call an <c>ICacheWriter</c> that is installed anywhere in any
      /// participating cache for that region, preferring a local <c>ICacheWriter</c>
      /// if there is one. Usually there will be only one <c>ICacheWriter</c> in
      /// the distributed system. If there are multiple <c>ICacheWriter</c>s
      /// available in the distributed system, the GemFire
      /// implementation always prefers one that is stored locally, or else picks one
      /// arbitrarily. In any case, only one <c>ICacheWriter</c> will be invoked.
      /// </para><para>
      /// The typical use for a <c>ICacheWriter</c> is to update a database.
      /// Application writers should implement these methods to execute
      /// application-specific behavior before the cache is modified.
      /// </para>
      /// <para>
      /// Note that cache writer callbacks are synchronous callbacks and have the ability
      /// to veto the cache update. Since cache writer invocations require communications
      /// over the network, (especially if they are not co-located on the nodes where the
      /// change occurs) the use of cache writers presents a performance penalty.
      /// </para><para>
      /// The <c>ICacheWriter</c> is capable of aborting the update to the cache by throwing
      /// a <c>CacheWriterException</c>. This exception or any runtime exception
      /// thrown by the <c>ICacheWriter</c> will abort the operation, and the
      /// exception will be propagated to the initiator of the operation, regardless
      /// of whether the initiator is in the same process as the <c>ICacheWriter</c>.
      /// </para>
      /// </remarks>
      /// <seealso cref="AttributesFactory.SetCacheWriter" />
      /// <seealso cref="RegionAttributes.CacheWriter" />
      /// <seealso cref="ICacheLoader" />
      /// <seealso cref="ICacheListener" />
      generic <class TKey, class TValue>
      public interface class ICacheWriter
      {
      public:

        /// <summary>
        /// Called before an entry is updated. The entry update is initiated by a
        /// <c>Put</c> or a <c>Get</c> that causes the loader to update an existing entry.
        /// </summary>
        /// <remarks>
        /// The entry previously existed in the cache where the operation was
        /// initiated, although the old value may have been null. The entry being
        /// updated may or may not exist in the local cache where the CacheWriter is
        /// installed.
        /// </remarks>
        /// <param name="ev">
        /// event object associated with updating the entry
        /// </param>
        /// <seealso cref="Region.Put" />
        /// <seealso cref="Region.Get" />
        bool BeforeUpdate( EntryEvent<TKey, TValue>^ ev );

        /// <summary>
        /// Called before an entry is created. Entry creation is initiated by a
        /// <c>Create</c>, a <c>Put</c>, or a <c>Get</c>.
        /// </summary>
        /// <remarks>
        /// The <c>CacheWriter</c> can determine whether this value comes from a
        /// <c>Get</c> or not from <c>Load</c>. The entry being
        /// created may already exist in the local cache where this <c>CacheWriter</c>
        /// is installed, but it does not yet exist in the cache where the operation was initiated.
        /// </remarks>
        /// <param name="ev">
        /// event object associated with creating the entry
        /// </param>
        /// <seealso cref="Region.Create" />
        /// <seealso cref="Region.Put" />
        /// <seealso cref="Region.Get" />
        bool BeforeCreate( EntryEvent<TKey, TValue>^ ev );

        /// <summary>
        /// Called before an entry is destroyed.
        /// </summary>
        /// <remarks>
        /// The entry being destroyed may or may
        /// not exist in the local cache where the CacheWriter is installed. This method
        /// is <em>not</em> called as a result of expiration or
        /// <see cref="Region.LocalDestroyRegion" />.
        /// </remarks>
        /// <param name="ev">
        /// event object associated with destroying the entry
        /// </param>
        /// <seealso cref="Region.Destroy" />
        bool BeforeDestroy( EntryEvent<TKey, TValue>^ ev );

        /// <summary>
        /// Called before this region is cleared.
        /// </summary>
        bool BeforeRegionClear( RegionEvent<TKey, TValue>^ ev );

        /// <summary>
        /// Called before this region is destroyed.
        /// </summary>
        /// <param name="ev">
        /// event object associated with destroying the region
        /// </param>
        /// <seealso cref="Region.DestroyRegion" />
        bool BeforeRegionDestroy( RegionEvent<TKey, TValue>^ ev );

        /// <summary>
        /// Called when the region containing this callback is destroyed, when
        /// the cache is closed.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Implementations should clean up any external
        /// resources, such as database connections. Any runtime exceptions this method
        /// throws will be logged.
        /// </para><para>
        /// It is possible for this method to be called multiple times on a single
        /// callback instance, so implementations must be tolerant of this.
        /// </para>
        /// </remarks>
        /// <param name="region">region to close</param>
        /// <seealso cref="Cache.Close" />
        /// <seealso cref="Region.DestroyRegion" />
        void Close( IRegion<TKey, TValue>^ region );
      };

    }
  }
}
 } //namespace 
