/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include <gfcpp/AttributesFactory.hpp>
//#include "impl/NativeWrapper.hpp"
#include "ExpirationAction.hpp"
#include "DiskPolicyType.hpp"
//#include "ScopeType.hpp"
#include <gfcpp/RegionFactory.hpp>
#include "RegionShortcut.hpp"

#include "ICacheLoader.hpp"
#include "ICacheWriter.hpp"
#include "ICacheListener.hpp"
#include "IPartitionResolver.hpp"
#include "IFixedPartitionResolver.hpp"
#include "IPersistenceManager.hpp"

#include "IRegion.hpp"
#include "Properties.hpp"
#include "Region.hpp"

using namespace System;
using namespace System::Collections::Generic;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Generic
      {
			/// <summary>
      /// This interface provides for the configuration and creation of instances of Region.
      /// </summary>
      public ref class RegionFactory sealed
				: public Internal::SBWrap<gemfire::RegionFactory>
      {
      public:
        /// <summary>
        /// Sets the library path for the library that will be invoked for the loader of the region.
        /// </summary>
        /// <param name="libPath">
        /// library pathname containing the factory function.
        /// </param>
        /// <param name="factoryFunctionName">
        /// Name of factory function that creates a <c>CacheLoader</c>
        /// for a native library, or the name of the method in the form
        /// {Namespace}.{Class Name}.{Method Name} that creates an
        /// <c>ICacheLoader</c> for a managed library.
        /// </param>
        /// <returns>the instance of RegionFactory</returns>
        RegionFactory^ SetCacheLoader( String^ libPath, String^ factoryFunctionName );

        /// <summary>
        /// Sets the library path for the library that will be invoked for the writer of the region.
        /// </summary>
        /// <param name="libPath">
        /// library pathname containing the factory function.
        /// </param>
        /// <param name="factoryFunctionName">
        /// Name of factory function that creates a <c>CacheWriter</c>
        /// for a native library, or the name of the method in the form
        /// {Namespace}.{Class Name}.{Method Name} that creates an
        /// <c>ICacheWriter</c> for a managed library.
        /// </param>
        /// <returns>the instance of RegionFactory</returns>
        RegionFactory^ SetCacheWriter( String^ libPath, String^ factoryFunctionName );

        /// <summary>
        /// Sets the library path for the library that will be invoked for the listener of the region.
        /// </summary>
        /// <param name="libPath">
        /// library pathname containing the factory function.
        /// </param>
        /// <param name="factoryFunctionName">
        /// Name of factory function that creates a <c>CacheListener</c>
        /// for a native library, or the name of the method in the form
        /// {Namespace}.{Class Name}.{Method Name} that creates an
        /// <c>ICacheListener</c> for a managed library.
        /// </param>
        /// <returns>the instance of RegionFactory</returns>
        RegionFactory^ SetCacheListener( String^ libPath, String^ factoryFunctionName );


        /// <summary>
        /// Sets the library path for the library that will be invoked for the partition resolver of the region.
        /// </summary>
        /// <param name="libPath">
        /// library pathname containing the factory function.
        /// </param>
        /// <param name="factoryFunctionName">
        /// Name of factory function that creates a <c>PartitionResolver</c>
        /// for a native library, or the name of the method in the form
        /// {Namespace}.{Class Name}.{Method Name} that creates an
        /// <c>IPartitionResolver</c> for a managed library.
        /// </param>
        /// <returns>the instance of RegionFactory</returns>
        RegionFactory^ SetPartitionResolver( String^ libPath, String^ factoryFunctionName );
        
        /// <summary>
        /// Sets the idleTimeout expiration attributes for region entries for the next
        /// <c>RegionAttributes</c> created.
        /// </summary>
        /// <param name="action">
        /// The expiration action for which to set the timeout.
        /// </param>
        /// <param name="idleTimeout">
        /// the idleTimeout in seconds for entries in this region.
        /// </param>
        /// <returns>the instance of RegionFactory</returns>
        RegionFactory^ SetEntryIdleTimeout( ExpirationAction action, uint32_t idleTimeout );

        /// <summary>
        /// Sets the timeToLive expiration attributes for region entries for the next
        /// <c>RegionAttributes</c> created.
        /// </summary>
        /// <param name="action">
        /// The expiration action for which to set the timeout.
        /// </param>
        /// <param name="timeToLive">
        /// the timeToLive in seconds for entries in this region.
        /// </param>
        /// <returns>the instance of RegionFactory</returns>
        RegionFactory^ SetEntryTimeToLive( ExpirationAction action, uint32_t timeToLive );

        /// <summary>
        /// Sets the idleTimeout expiration attributes for the region itself for the
        /// next <c>RegionAttributes</c> created.
        /// </summary>
        /// <param name="action">
        /// The expiration action for which to set the timeout.
        /// </param>
        /// <param name="idleTimeout">
        /// the idleTimeout in seconds for the region as a whole.
        /// </param>
        /// <returns>the instance of RegionFactory</returns>
        RegionFactory^ SetRegionIdleTimeout( ExpirationAction action, uint32_t idleTimeout );

        /// <summary>
        /// Sets the timeToLive expiration attributes for the region itself for the
        /// next <c>RegionAttributes</c> created.
        /// </summary>
        /// <param name="action">
        /// The expiration action for which to set the timeout.
        /// </param>
        /// <param name="timeToLive">
        /// the timeToLive in seconds for the region as a whole.
        /// </param>
        /// <returns>the instance of RegionFactory</returns>
        RegionFactory^ SetRegionTimeToLive( ExpirationAction action, uint32_t timeToLive );


        // PERSISTENCE
        
        /// <summary>
        /// Sets the persistence manager for the <c>RegionAttributes</c> being created.
        /// </summary>
        /// <param name="persistenceManager">
        /// a user-defined persistence manager, or null for no persistence manager
        /// </param>
        /// <returns>the instance of RegionFactory</returns>
        generic <class TKey, class TValue>
        RegionFactory^ SetPersistenceManager( Generic::IPersistenceManager<TKey, TValue>^ persistenceManager );
        
        /// <summary>
        /// Sets the persistence manager for the <c>RegionAttributes</c> being created.
        /// </summary>
        /// <param name="persistenceManager">
        /// a user-defined persistence manager, or null for no persistence manager
        /// </param>
        /// <param name="config">
        /// The configuration properties to use for the PersistenceManager.
        /// </param>
        /// <returns>the instance of RegionFactory</returns>
        generic <class TKey, class TValue>
        RegionFactory^ SetPersistenceManager( Generic::IPersistenceManager<TKey, TValue>^ persistenceManager, 
            Properties<String^, String^>^ config);

        /// <summary>
        /// Sets the library path for the library that will be invoked for the persistence of the region.
        /// If the region is being created from a client on a server, or on a server directly, then
        /// This must be used to set the PersistenceManager.
        /// </summary>
        /// <param name="libPath">The path of the PersistenceManager shared library.</param>
        /// <param name="factoryFunctionName">
        /// The name of the factory function to create an instance of PersistenceManager object.
        /// </param>
        /// <returns>the instance of RegionFactory</returns>
        RegionFactory^ SetPersistenceManager( String^ libPath, String^ factoryFunctionName );

        /// <summary>
        /// Sets the library path for the library that will be invoked for the persistence of the region.
        /// If the region is being created from a client on a server, or on a server directly, then
        /// This must be used to set the PersistenceManager.
        /// </summary>
        /// <param name="libPath">The path of the PersistenceManager shared library.</param>
        /// <param name="factoryFunctionName">
        /// The name of the factory function to create an instance of PersistenceManager object.
        /// </param>
        /// <param name="config">
        /// The configuration properties to use for the PersistenceManager.
        /// </param>
        /// <returns>the instance of RegionFactory</returns>
        RegionFactory^ SetPersistenceManager( String^ libPath, String^ factoryFunctionName,
          /*Dictionary<Object^, Object^>*/Properties<String^, String^>^ config );

        /// <summary>
        /// Set the pool name for a Thin Client region.
        /// </summary>
        /// <remarks>
        /// The pool with the name specified must be already created.
        /// </remarks>
        /// <param name="poolName">
        /// The name of the pool to attach to this region.
        /// </param>
        /// <returns>the instance of RegionFactory</returns>
        RegionFactory^ SetPoolName( String^ poolName );

        // MAP ATTRIBUTES

        /// <summary>
        /// Sets the entry initial capacity for the <c>RegionAttributes</c>
        /// being created. This value is used in initializing the map that
        /// holds the entries.
        /// </summary>
        /// <param name="initialCapacity">the initial capacity of the entry map</param>
        /// <exception cref="IllegalArgumentException">
        /// if initialCapacity is nonpositive
        /// </exception>
        /// <returns>the instance of RegionFactory</returns>
        RegionFactory^ SetInitialCapacity( int32_t initialCapacity );

        /// <summary>
        /// Sets the entry load factor for the next <c>RegionAttributes</c>
        /// created. This value is
        /// used in initializing the map that holds the entries.
        /// </summary>
        /// <param name="loadFactor">the load factor of the entry map</param>
        /// <exception cref="IllegalArgumentException">
        /// if loadFactor is nonpositive
        /// </exception>
        /// <returns>the instance of RegionFactory</returns>
        RegionFactory^ SetLoadFactor( Single loadFactor );

        /// <summary>
        /// Sets the concurrency level of the next <c>RegionAttributes</c>
        /// created. This value is used in initializing the map that holds the entries.
        /// </summary>
        /// <param name="concurrencyLevel">
        /// the concurrency level of the entry map
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// if concurrencyLevel is nonpositive
        /// </exception>
        /// <returns>the instance of RegionFactory</returns>
        RegionFactory^ SetConcurrencyLevel( int32_t concurrencyLevel );

        /// <summary>
        /// Sets a limit on the number of entries that will be held in the cache.
        /// If a new entry is added while at the limit, the cache will evict the
        /// least recently used entry.
        /// </summary>
        /// <param name="entriesLimit">
        /// The limit of the number of entries before eviction starts.
        /// Defaults to 0, meaning no LRU actions will used.
        /// </param>
        /// <returns>the instance of RegionFactory</returns>
        RegionFactory^ SetLruEntriesLimit( uint32_t entriesLimit );

        /// <summary>
        /// Sets the disk policy type for the next <c>RegionAttributes</c> created.
        /// </summary>
        /// <param name="diskPolicy">
        /// the disk policy to use for the region
        /// </param>
        /// <returns>the instance of RegionFactory</returns>
        RegionFactory^ SetDiskPolicy( DiskPolicyType diskPolicy );

        /// <summary>
        /// Set caching enabled flag for this region.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If set to false, then no data is stored in the local process,
        /// but events and distributions will still occur, and the region
        /// can still be used to put and remove, etc...
        /// </para><para>
        /// The default if not set is 'true', 'false' is illegal for regions
        /// of <c>ScopeType.Local</c> scope. 
        /// </para>
        /// </remarks>
        /// <param name="cachingEnabled">
        /// if true, cache data for this region in this process.
        /// </param>
        /// <returns>the instance of RegionFactory</returns>
        RegionFactory^ SetCachingEnabled( bool cachingEnabled );
        /// <summary>
        /// Set cloning enabled flag for this region.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If set to false, then there is no cloning will take place in case of delta.
        /// Delta will be applied on the old value which will change old value in-place.
        /// </para><para>
        /// The default if not set is 'false'
        /// of <c>ScopeType.Local</c> scope. 
        /// </para>
        /// </remarks>
        /// <param name="cloningEnabled">
        /// if true, clone old value before applying delta so that in-place change would not occour..
        /// </param>
        /// <returns>the instance of RegionFactory</returns>
        RegionFactory^ SetCloningEnabled( bool cloningEnabled );

        /// <summary>
        /// Sets concurrency checks enabled flag for this region.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If set to false, then the version checks will not occur.
        /// </para><para>
        /// The default if not set is 'true'
        /// </para>
        /// </remarks>
        /// <param name="concurrencyChecksEnabled">
        /// if true, version checks for region entries will occur.
        /// </param>
        /// <returns>the instance of RegionFactory</returns>
        RegionFactory^ SetConcurrencyChecksEnabled( bool concurrencyChecksEnabled );
        // NEW GENERIC APIs:

        /// <summary>
        /// Creates a region with the given name. 
        /// The region is just created locally. It is not created on the server
        /// to which this client is connected with.
        /// </summary>
        /// <remarks>
        /// If Pool attached with Region is in multiusersecure mode then don't use return instance of region as no credential are attached with this instance.
        /// Get instance of region from <see cref="Cache.CreateAuthenticatedView" to do the operation on Cache. 
        /// </remarks>
        /// <param name="name">the name of the region to create</param>
        /// <returns>new region</returns>
        /// <exception cref="RegionExistsException">
        /// if a region with the same name is already in this cache
        /// </exception>
        /// <exception cref="CacheClosedException">
        /// if the cache is closed
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if the memory allocation failed
        /// </exception>
        /// <exception cref="RegionCreationFailedException">
        /// if the call fails due to incomplete mirror initialization
        /// </exception>
        /// <exception cref="InitFailedException">
        /// if the optional PersistenceManager fails to initialize
        /// </exception>
        /// <exception cref="UnknownException">otherwise</exception>
        generic <class TKey, class TValue>
        IRegion<TKey, TValue>^ Create(String^ regionName);

        /// <summary>
        /// Sets the cache loader for the <c>RegionAttributes</c> being created.
        /// </summary>
        /// <param name="cacheLoader">
        /// a user-defined cache loader, or null for no cache loader
        /// </param>
        /// <returns>the instance of RegionFactory</returns>
        generic <class TKey, class TValue>
        RegionFactory^ SetCacheLoader( ICacheLoader<TKey, TValue>^ cacheLoader );

        /// <summary>
        /// Sets the cache writer for the <c>RegionAttributes</c> being created.
        /// </summary>
        /// <param name="cacheWriter">
        /// user-defined cache writer, or null for no cache writer
        /// </param>
        /// <returns>the instance of RegionFactory</returns>
        generic <class TKey, class TValue>
        RegionFactory^ SetCacheWriter( ICacheWriter<TKey, TValue>^ cacheWriter );

        /// <summary>
        /// Sets the CacheListener for the <c>RegionAttributes</c> being created.
        /// </summary>
        /// <param name="cacheListener">
        /// user-defined cache listener, or null for no cache listener
        /// </param>
        /// <returns>the instance of RegionFactory</returns>
        generic <class TKey, class TValue>
        RegionFactory^ SetCacheListener( ICacheListener<TKey, TValue>^ cacheListener );

        /// <summary>
        /// Sets the PartitionResolver for the <c>RegionAttributes</c> being created.
        /// </summary>
        /// <param name="partitionresolver">
        /// user-defined partition resolver, or null for no partition resolver
        /// </param>
        /// <returns>the instance of RegionFactory</returns>
        generic <class TKey, class TValue>
        RegionFactory^ SetPartitionResolver( IPartitionResolver<TKey, TValue>^ partitionresolver );


    internal:

      /// <summary>
      /// Internal factory function to wrap a native object pointer inside
      /// this managed class with null pointer check.
      /// </summary>
      /// <param name="nativeptr">The native object pointer</param>
      /// <returns>
      /// The managed wrapper object; null if the native pointer is null.
      /// </returns>
      inline static RegionFactory^ Create( gemfire::RegionFactory* nativeptr )
      {
        return ( nativeptr != nullptr ?
          gcnew RegionFactory( nativeptr ) : nullptr );
      }

	  private:

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
      inline RegionFactory( gemfire::RegionFactory* nativeptr )
				: Internal::SBWrap<gemfire::RegionFactory>( nativeptr ) { }
      };
      } // end namespace Generic
    }
  }
}

