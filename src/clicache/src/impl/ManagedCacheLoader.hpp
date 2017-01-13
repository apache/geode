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
#include <gfcpp/CacheLoader.hpp>

#include "../ICacheLoader.hpp"
#include "CacheLoader.hpp"

namespace gemfire
{

  /// <summary>
  /// Wraps the managed <see cref="GemStone.GemFire.Cache.ICacheLoader" />
  /// object and implements the native <c>gemfire::CacheLoader</c> interface.
  /// </summary>
  class ManagedCacheLoaderGeneric
    : public gemfire::CacheLoader
  {
  public:

    /// <summary>
    /// Constructor to initialize with the provided managed object.
    /// </summary>
    /// <param name="userptr">
    /// The managed object.
    /// </param>
    inline ManagedCacheLoaderGeneric(
      /*Generic::ICacheLoader<Object^, Object^>^ managedptr,*/ Object^ userptr )
      : /*m_managedptr( managedptr ),*/ m_userptr(userptr) { }

    /// <summary>
    /// Static function to create a <c>ManagedCacheLoader</c> using given
    /// managed assembly path and given factory function.
    /// </summary>
    /// <param name="assemblyPath">
    /// The path of the managed assembly that contains the <c>ICacheLoader</c>
    /// factory function.
    /// </param>
    /// <param name="factoryFunctionName">
    /// The name of the factory function of the managed class for creating
    /// an object that implements <c>ICacheLoader</c>.
    /// This should be a static function of the format
    /// {Namespace}.{Class Name}.{Method Name}.
    /// </param>
    /// <exception cref="IllegalArgumentException">
    /// If the managed library cannot be loaded or the factory function fails.
    /// </exception>
    static gemfire::CacheLoader* create( const char* assemblyPath,
      const char* factoryFunctionName );

    virtual ~ManagedCacheLoaderGeneric( ) { }

    /// <summary>
    /// Loads a value. Application writers should implement this
    /// method to customize the loading of a value.
    /// </summary>
    /// <remarks>
    /// This method is called by the caching service when the requested
    /// value is not in the cache. Any exception thrown by this method
    /// is propagated back to and thrown by the invocation of
    /// <see cref="GemStone.GemFire.Cache.Region.Get" /> that triggered this load.
    /// </remarks>
    /// <param name="region">a Region Pointer for which this is called.</param>
    /// <param name="key">the key for the cacheable</param>
    /// <param name="aCallbackArgument">any related user data, or null</param>
    /// <returns>
    /// the value supplied for this key, or null if no value can be
    /// supplied. 
    /// If every available loader returns
    /// a null value, <see cref="GemStone.GemFire.Cache.Region.Get" />
    /// will return null.
    /// </returns>
    /// <seealso cref="GemStone.GemFire.Cache.Region.Get" />
    virtual CacheablePtr load(const RegionPtr& region,
      const CacheableKeyPtr& key, const UserDataPtr& aCallbackArgument);

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
    /// <param name="region">the region pointer</param>
    /// <seealso cref="GemStone.GemFire.Cache.Cache.Close" />
    /// <seealso cref="GemStone.GemFire.Cache.Region.DestroyRegion" />
    virtual void close( const RegionPtr& region );

    /*
    /// <summary>
    /// Returns the wrapped managed object reference.
    /// </summary>
    inline GemStone::GemFire::Cache::ICacheLoader^ ptr( ) const
    {
      return m_managedptr;
    }
    */

    inline void setptr(GemStone::GemFire::Cache::Generic::ICacheLoaderProxy^ managedptr)
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
    /// Note: not using auto_gcroot since it will result in 'Dispose' of the ICacheLoader
    /// to be called which is not what is desired when this object is destroyed. Normally this
    /// managed object may be created by the user and will be handled automatically by the GC.
    /// </summary>
    gcroot<GemStone::GemFire::Cache::Generic::ICacheLoaderProxy^> m_managedptr;

    gcroot<Object^> m_userptr;

    // Disable the copy and assignment constructors
    ManagedCacheLoaderGeneric( const ManagedCacheLoaderGeneric& );
    ManagedCacheLoaderGeneric& operator = ( const ManagedCacheLoaderGeneric& );
  };

}
