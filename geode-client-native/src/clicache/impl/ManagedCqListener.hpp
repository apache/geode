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
#include <cppcache/CqListener.hpp>
#include "../ICqListener.hpp"

namespace gemfire {

  /// <summary>
  /// Wraps the managed <see cref="GemStone.GemFire.Cache.ICacheListener" />
  /// object and implements the native <c>gemfire::CacheListener</c> interface.
  /// </summary>
  class ManagedCqListener
    : public CqListener
  {
  public:

    /// <summary>
    /// Constructor to initialize with the provided managed object.
    /// </summary>
    /// <param name="managedptr">
    /// The managed object.
    /// </param>
    inline ManagedCqListener(
      GemStone::GemFire::Cache::ICqListener^ managedptr )
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
    static CqListener* create( const char* assemblyPath,
      const char* factoryFunctionName );

    /// <summary>
    /// Destructor -- does nothing.
    /// </summary>
    virtual ~ManagedCqListener( ) { }

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
    virtual void onEvent( const CqEvent& ev );

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
    virtual void onError( const CqEvent& ev );

    /// <summary>
    /// Handles the event of an entry's value being invalidated.
    /// </summary>
    /// EntryEvent denotes the event object associated with the entry invalidation.
    /// </param>
    virtual void close();
    /// <summary>
    /// Returns the wrapped managed object reference.
    /// </summary>
    inline GemStone::GemFire::Cache::ICqListener^ ptr( ) const
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
    gcroot<GemStone::GemFire::Cache::ICqListener^> m_managedptr;
    //GemStone::GemFire::Cache::ICqListener^ m_managedptr;
  };

}
