/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "../gf_defs.hpp"
#include <vcclr.h>
#include <cppcache/CacheListener.hpp>
#include "../ICacheListener.hpp"

/*
namespace GemStone {

  namespace GemFire {

    namespace Cache {

      interface class ICacheListener;
    }
  }
}
*/

namespace gemfire {

  /// <summary>
  /// Wraps the managed <see cref="GemStone.GemFire.Cache.ICacheListener" />
  /// object and implements the native <c>gemfire::CacheListener</c> interface.
  /// </summary>
  class ManagedCacheListener
    : public CacheListener
  {
  public:

    /// <summary>
    /// Constructor to initialize with the provided managed object.
    /// </summary>
    /// <param name="managedptr">
    /// The managed object.
    /// </param>
    inline ManagedCacheListener(
      GemStone::GemFire::Cache::ICacheListener^ managedptr )
      : m_managedptr( managedptr ) { }

    /// <summary>
    /// Static function to create a <c>ManagedCacheListener</c> using given
    /// managed assembly path and given factory function.
    /// </summary>
    /// <param name="assemblyPath">
    /// The path of the managed assembly that contains the <c>ICacheListener</c>
    /// factory function.
    /// </param>
    /// <param name="factoryFunctionName">
    /// The name of the factory function of the managed class for creating
    /// an object that implements <c>ICacheListener</c>.
    /// This should be a static function of the format
    /// {Namespace}.{Class Name}.{Method Name}.
    /// </param>
    /// <exception cref="IllegalArgumentException">
    /// If the managed library cannot be loaded or the factory function fails.
    /// </exception>
    static CacheListener* create( const char* assemblyPath,
      const char* factoryFunctionName );

    /// <summary>
    /// Destructor -- does nothing.
    /// </summary>
    virtual ~ManagedCacheListener( ) { }

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
    /// <seealso cref="GemStone.GemFire.Cache.Region.Create" />
    /// <seealso cref="GemStone.GemFire.Cache.Region.Put" />
    /// <seealso cref="GemStone.GemFire.Cache.Region.Get" />
    virtual void afterCreate( const EntryEvent& ev );

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
    /// <seealso cref="GemStone.GemFire.Cache.Region.Put" />
    virtual void afterUpdate( const EntryEvent& ev );

    /// <summary>
    /// Handles the event of an entry's value being invalidated.
    /// </summary>
    /// <param name="ev">
    /// EntryEvent denotes the event object associated with the entry invalidation.
    /// </param>
    virtual void afterInvalidate( const EntryEvent& ev );

    /// <summary>
    /// Handles the event of an entry being destroyed.
    /// </summary>
    /// <param name="ev">
    /// EntryEvent denotes the event object associated with the entry destruction.
    /// </param>
    /// <seealso cref="GemStone.GemFire.Cache.Region.Destroy" />
    virtual void afterDestroy( const EntryEvent& ev );

    /// <summary>
    /// Handles the event of a region being cleared.
    /// </summary>
    virtual void afterRegionClear( const RegionEvent& ev );

    /// <summary>
    /// Handles the event of a region being invalidated.
    /// </summary>
    /// <remarks>
    /// Events are not invoked for each individual value that is invalidated
    /// as a result of the region being invalidated. Each subregion, however,
    /// gets its own <c>regionInvalidated</c> event invoked on its listener.
    /// </remarks>
    /// <param name="ev">
    /// RegionEvent denotes the event object associated with the region invalidation.
    /// </param>
    /// <seealso cref="GemStone.GemFire.Cache.Region.InvalidateRegion" />
    virtual void afterRegionInvalidate( const RegionEvent& ev );

    /// <summary>
    /// Handles the event of a region being destroyed.
    /// </summary>
    /// <remarks>
    /// Events are not invoked for each individual entry that is destroyed
    /// as a result of the region being destroyed. Each subregion, however,
    /// gets its own <c>afterRegionDestroyed</c> event invoked on its listener.
    /// </remarks>
    /// <param name="ev">
    /// RegionEvent denotes the event object associated with the region destruction.
    /// </param>
    /// <seealso cref="GemStone.GemFire.Cache.Region.DestroyRegion" />
    virtual void afterRegionDestroy( const RegionEvent& ev );

    /// <summary>
    /// Handles the event of a region being live.
    /// </summary>
    /// <remarks>
    /// Each subregion gets its own <c>afterRegionLive</c> event invoked on its listener.
    /// </remarks>
    /// <param name="ev">
    /// RegionEvent denotes the event object associated with the region going live.
    /// </param>
    /// <seealso cref="GemStone.GemFire.Cache.Cache.ReadyForEvents" />
    virtual void afterRegionLive( const RegionEvent& ev );

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
    /// <seealso cref="GemStone.GemFire.Cache.Cache.Close" />
    /// <seealso cref="GemStone.GemFire.Cache.Region.DestroyRegion" />
    virtual void close( const RegionPtr& region );

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
    virtual void afterRegionDisconnected( const RegionPtr& region  );

    /// <summary>
    /// Returns the wrapped managed object reference.
    /// </summary>
    inline GemStone::GemFire::Cache::ICacheListener^ ptr( ) const
    {
      return m_managedptr;
    }


  private:

    /// <summary>
    /// Using gcroot to hold the managed delegate pointer (since it cannot be stored directly).
    /// Note: not using auto_gcroot since it will result in 'Dispose' of the ICacheListener
    /// to be called which is not what is desired when this object is destroyed. Normally this
    /// managed object may be created by the user and will be handled automatically by the GC.
    /// </summary>
    gcroot<GemStone::GemFire::Cache::ICacheListener^> m_managedptr;
  };

}
