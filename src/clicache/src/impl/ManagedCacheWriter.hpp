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
#include <gfcpp/CacheWriter.hpp>
#include "../ICacheWriter.hpp"

using namespace System;
//using namespace gemfire;
namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      interface class ICacheWriter;
    }
  }
}

namespace gemfire
{

  /// <summary>
  /// Wraps the managed <see cref="GemStone.GemFire.Cache.ICacheWriter" />
  /// object and implements the native <c>gemfire::CacheWriter</c> interface.
  /// </summary>
  class ManagedCacheWriterGeneric
    : public CacheWriter
  {
  public:

    /// <summary>
    /// Constructor to initialize with the provided managed object.
    /// </summary>
    /// <param name="userptr">
    /// The managed object.
    /// </param>
    inline ManagedCacheWriterGeneric( Object^ userptr ) : m_userptr( userptr ) { }

    /// <summary>
    /// Static function to create a <c>ManagedCacheWriter</c> using given
    /// managed assembly path and given factory function.
    /// </summary>
    /// <param name="assemblyPath">
    /// The path of the managed assembly that contains the <c>ICacheWriter</c>
    /// factory function.
    /// </param>
    /// <param name="factoryFunctionName">
    /// The name of the factory function of the managed class for creating
    /// an object that implements <c>ICacheWriter</c>.
    /// This should be a static function of the format
    /// {Namespace}.{Class Name}.{Method Name}.
    /// </param>
    /// <exception cref="IllegalArgumentException">
    /// If the managed library cannot be loaded or the factory function fails.
    /// </exception>
    static CacheWriter* create( const char* assemblyPath,
      const char* factoryFunctionName );

    virtual ~ManagedCacheWriterGeneric( ) { }

    /// <summary>
    /// Called before an entry is updated. The entry update is initiated by a
    /// <c>put</c> or a <c>get</c> that causes the loader to update an existing entry.
    /// </summary>
    /// <remarks>
    /// The entry previously existed in the cache where the operation was
    /// initiated, although the old value may have been null. The entry being
    /// updated may or may not exist in the local cache where the CacheWriter is
    /// installed.
    /// </remarks>
    /// <param name="ev">
    /// EntryEvent denotes the event object associated with updating the entry
    /// </param>
    /// <seealso cref="GemStone.GemFire.Cache.Region.Put" />
    /// <seealso cref="GemStone.GemFire.Cache.Region.Get" />
    bool beforeUpdate( const EntryEvent& ev );

    /// <summary>
    /// Called before an entry is created. Entry creation is initiated by a
    /// <c>create</c>, a <c>put</c>, or a <c>get</c>.
    /// </summary>
    /// <remarks>
    /// The <c>CacheWriter</c> can determine whether this value comes from a
    /// <c>get</c> or not from <c>load</c>. The entry being created may already
    /// exist in the local cache where this <c>CacheWriter</c> is installed,
    /// but it does not yet exist in the cache where the operation was initiated.
    /// </remarks>
    /// <param name="ev">
    /// EntryEvent denotes the event object associated with creating the entry
    /// </param>
    /// <seealso cref="GemStone.GemFire.Cache.Region.Create" />
    /// <seealso cref="GemStone.GemFire.Cache.Region.Put" />
    /// <seealso cref="GemStone.GemFire.Cache.Region.Get" />
    bool beforeCreate( const EntryEvent& ev );

    /// <summary>
    /// Called before an entry is destroyed.
    /// </summary>
    /// <remarks>
    /// The entry being destroyed may or may
    /// not exist in the local cache where the CacheWriter is installed. This method
    /// is <em>not</em> called as a result of expiration or
    /// <see cref="GemStone.GemFire.Cache.Region.LocalDestroyRegion" />.
    /// </remarks>
    /// <param name="ev">
    /// EntryEvent denotes the event object associated with destroying the entry
    /// </param>
    /// <seealso cref="GemStone.GemFire.Cache.Region.Destroy" />
    bool beforeDestroy( const EntryEvent& ev );

    /// <summary>
    /// called before this region is cleared
    /// </summary>
    bool beforeRegionClear( const RegionEvent& ev );

    /// <summary>
    /// called before this region is destroyed
    /// </summary>
    /// <param name="ev">
    /// RegionEvent denotes the event object associated with destroying the region
    /// </param>
    /// <seealso cref="GemStone.GemFire.Cache.Region.DestroyRegion" />
    bool beforeRegionDestroy( const RegionEvent& ev );

    /// <summary>
    /// Called when the region containing this callback is destroyed, when
    /// the cache is closed.
    /// </summary>
    /// <remarks>
    /// Implementations should clean up any external
    /// resources, such as database connections. Any runtime exceptions this method
    /// throws will be logged.
    /// <para>
    /// It is possible for this method to be called multiple times on a single
    /// callback instance, so implementations must be tolerant of this.
    /// </para>
    /// </remarks>
    /// <seealso cref="GemStone.GemFire.Cache.Cache.Close" />
    /// <seealso cref="GemStone.GemFire.Cache.Region.DestroyRegion" />
    void close( const RegionPtr& rp );

    /// <summary>
    /// Returns the wrapped managed object reference.
    /// </summary>
    inline GemStone::GemFire::Cache::Generic::ICacheWriter<Object^, Object^>^ ptr( ) const
    {
      return m_managedptr;
    }

    inline void setptr( GemStone::GemFire::Cache::Generic::ICacheWriter<Object^, Object^>^ managedptr )
    {
      m_managedptr = managedptr;
    }

    inline Object^ userptr( ) const
    {
      return m_userptr;
    }


  private:

    /// <summary>
    /// Using gcroot to hold the managed delegate pointer (since it cannot be stored directly).
    /// Note: not using auto_gcroot since it will result in 'Dispose' of the ICacheWriter
    /// to be called which is not what is desired when this object is destroyed. Normally this
    /// managed object may be created by the user and will be handled automatically by the GC.
    /// </summary>
    gcroot<GemStone::GemFire::Cache::Generic::ICacheWriter<Object^, Object^>^> m_managedptr;

    gcroot<Object^> m_userptr;
  };

}
