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

#include "ICacheLoader.hpp"
#include "ICacheWriter.hpp"
#include "ICacheListener.hpp"
#include "IPartitionResolver.hpp"
#include "IFixedPartitionResolver.hpp"
#include "IPersistenceManager.hpp"
#include "RegionAttributes.hpp"
#include "RegionAttributes.hpp"


using namespace System;
using namespace System::Collections::Generic;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache {
    
    namespace Generic 
    {

      //ref class RegionAttributes;
      //interface class ICacheLoader;
      //interface class ICacheWriter;
      //interface class ICacheListener;
      //interface class IPartitionResolver;
      //interface class IFixedPartitionResolver;


      /// <summary>
      /// Factory class to create instances of <see cref="RegionAttributes" />.
      /// </summary>
      /// <remarks>
      /// An <see cref="AttributesFactory" />
      /// instance maintains state for creating <see cref="RegionAttributes" /> instances.
      /// The setter methods are used to change the settings that will be used for
      /// creating the next attributes instance with the <see cref="CreateRegionAttributes" />
      /// method. If you create a factory with the default constructor, then the
      /// factory is set up to create attributes with all default settings. You can
      /// also create a factory by providing a preset <see cref="RegionAttributes" />.
      /// <para>
      /// Once a <see cref="RegionAttributes" /> is created, it can only be modified
      /// after it has been used to create a <see cref="Region" />, and then only by
      /// using an <see cref="AttributesMutator" /> obtained from the region.
      /// </para><para>
      /// <h3>Attributes</h3>
      /// <h4>Callbacks</h4>
      /// <dl>
      /// <dt><see cref="ICacheLoader" /> [<em>default:</em> null]</dt>
      ///     <dd>User-implemented plug-in for loading data on cache misses.<br />
      ///        see <see cref="SetCacheLoader" />,
      ///            <see cref="RegionAttributes.CacheLoader" /></dd>
      ///
      /// <dt><see cref="ICacheWriter" /> [<em>default:</em> null]</dt>
      ///     <dd>User-implemented plug-in for intercepting cache modifications, e.g.
      ///         for writing to an external data source.<br />
      ///         see <see cref="SetCacheWriter" />,
      ///             <see cref="RegionAttributes.CacheWriter" /></dd>
      ///
      /// <dt><see cref="ICacheListener" /> [<em>default:</em> null]</dt>
      ///     <dd>User-implemented plug-in for receiving and handling cache-related events.<br />
      ///         see <see cref="SetCacheListener" />,
      ///             <see cref="RegionAttributes.CacheListener" /></dd>
      ///
      /// <dt><see cref="IPartitionResolver" /> [<em>default:</em> null]</dt>
      ///     <dd>User-implemented plug-in for custom partitioning.<br />
      ///         see <see cref="SetPartitionResolver" />,
      ///             <see cref="RegionAttributes.PartitionResolver" /></dd>
      /// </dl>
      /// <h4>Expiration</h4>
      /// <dl>
      /// <dt>RegionTimeToLive [<em>default:</em> no expiration]</dt>
      ///     <dd>Expiration configuration for the entire region based on the
      ///     lastModifiedTime ( <see cref="CacheStatistics.LastModifiedTime" /> ).<br />
      ///         see <see cref="SetRegionTimeToLive" />,
      ///             <see cref="RegionAttributes.RegionTimeToLive" />,
      ///             <see cref="AttributesMutator.SetRegionTimeToLive" /></dd>
      ///
      /// <dt>RegionIdleTimeout [<em>default:</em> no expiration]</dt>
      ///     <dd>Expiration configuration for the entire region based on the
      ///         lastAccessedTime ( <see cref="CacheStatistics.LastAccessedTime" /> ).<br />
      ///         see <see cref="SetRegionIdleTimeout" />,
      ///             <see cref="RegionAttributes.RegionIdleTimeout" />,
      ///             <see cref="AttributesMutator.SetRegionIdleTimeout" /></dd>
      ///
      /// <dt>EntryTimeToLive [<em>default:</em> no expiration]</dt>
      ///     <dd>Expiration configuration for individual entries based on the
      ///     lastModifiedTime ( <see cref="CacheStatistics.LastModifiedTime" /> ).<br />
      ///         see <see cref="SetEntryTimeToLive" />,
      ///             <see cref="RegionAttributes.EntryTimeToLive" />,
      ///             <see cref="AttributesMutator.SetEntryTimeToLive" /></dd>
      ///
      /// <dt>EntryIdleTimeout [<em>default:</em> no expiration]</dt>
      ///     <dd>Expiration configuration for individual entries based on the
      ///         lastAccessedTime ( <see cref="CacheStatistics.LastAccessedTime" /> ).<br />
      ///         see <see cref="SetEntryIdleTimeout" />,
      ///             <see cref="RegionAttributes.EntryIdleTimeout" />,
      ///             <see cref="AttributesMutator.SetEntryIdleTimeout" /></dd>
      /// </dl>
      /// <h4>Storage</h4>
      /// <dl>
      /// <dt>InitialCapacity [<em>default:</em> <tt>16</tt>]</dt>
      ///     <dd>The initial capacity of the map used for storing the entries.<br />
      ///         see <see cref="SetInitialCapacity" />,
      ///             <see cref="RegionAttributes.InitialCapacity" /></dd>
      ///
      /// <dt>LoadFactor [<em>default:</em> <tt>0.75</tt>]</dt>
      ///     <dd>The load factor of the map used for storing the entries.<br />
      ///         see <see cref="SetLoadFactor" />,
      ///             <see cref="RegionAttributes.LoadFactor" /></dd>
      ///
      /// <dt>ConcurrencyLevel [<em>default:</em> <tt>16</tt>]</dt>
      ///     <dd>The allowed concurrency among updates to values in the region
      ///         is guided by the <tt>concurrencyLevel</tt>, which is used as a hint
      ///         for internal sizing. The actual concurrency will vary.
      ///         Ideally, you should choose a value to accommodate as many
      ///         threads as will ever concurrently modify values in the region. Using a
      ///         significantly higher value than you need can waste space and time,
      ///         and a significantly lower value can lead to thread contention. But
      ///         overestimates and underestimates within an order of magnitude do
      ///         not usually have much noticeable impact. A value of one is
      ///         appropriate when it is known that only one thread will modify
      ///         and all others will only read.<br />
      ///         see <see cref="SetConcurrencyLevel" />,
      ///             <see cref="RegionAttributes.ConcurrencyLevel" /></dd>
      ///
      /// </dl>
      /// </para>
      /// </remarks>
      /// <seealso cref="RegionAttributes" />
      /// <seealso cref="AttributesMutator" />
      /// <seealso cref="Region.CreateSubRegion" />
      generic<class TKey, class TValue>
      public ref class AttributesFactory sealed
        : public Internal::UMWrap<gemfire::AttributesFactory>
      {
      public:

        /// <summary>
        /// Creates a new <c>AttributesFactory</c> ready to create
        /// a <c>RegionAttributes</c> with default settings.
        /// </summary>
        inline AttributesFactory<TKey, TValue>( )
          : UMWrap( new gemfire::AttributesFactory( ), true ) { }

        /// <summary>
        /// Creates a new instance of <c>AttributesFactory</c> ready to create
        /// a <c>RegionAttributes</c> with the same settings as those in the
        /// specified <c>RegionAttributes</c>.
        /// </summary>
        /// <param name="regionAttributes">
        /// attributes used to initialize this AttributesFactory
        /// </param>
        AttributesFactory<TKey, TValue>(RegionAttributes<TKey, TValue>^ regionAttributes);

        // CALLBACKS

        /// <summary>
        /// Sets the cache loader for the <c>RegionAttributes</c> being created.
        /// </summary>
        /// <param name="cacheLoader">
        /// a user-defined cache loader, or null for no cache loader
        /// </param>
        //generic<class TKey, class TValue>
        void SetCacheLoader( ICacheLoader<TKey, TValue>^ cacheLoader );

        /// <summary>
        /// Sets the cache writer for the <c>RegionAttributes</c> being created.
        /// </summary>
        /// <param name="cacheWriter">
        /// user-defined cache writer, or null for no cache writer
        /// </param>
        //generic<class TKey, class TValue>
        void SetCacheWriter( ICacheWriter<TKey, TValue>^ cacheWriter );

        /// <summary>
        /// Sets the CacheListener for the <c>RegionAttributes</c> being created.
        /// </summary>
        /// <param name="cacheListener">
        /// user-defined cache listener, or null for no cache listener
        /// </param>
        //generic<class TKey, class TValue>
        void SetCacheListener( ICacheListener<TKey, TValue>^ cacheListener );

        /// <summary>
        /// Sets the PartitionResolver for the <c>RegionAttributes</c> being created.
        /// </summary>
        /// <param name="partitionresolver">
        /// user-defined partition resolver, or null for no partition resolver
        /// </param>
        //generic<class TKey, class TValue>
        void SetPartitionResolver( IPartitionResolver<TKey, TValue>^ partitionresolver );

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
        //generic<class TKey, class TValue>
        void SetCacheLoader( String^ libPath, String^ factoryFunctionName );

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
        //generic<class TKey, class TValue>
        void SetCacheWriter( String^ libPath, String^ factoryFunctionName );

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
        //generic<class TKey, class TValue>
        void SetCacheListener( String^ libPath, String^ factoryFunctionName );


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
        //generic<class TKey, class TValue>
        void SetPartitionResolver( String^ libPath, String^ factoryFunctionName );


        // EXPIRATION ATTRIBUTES

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
        void SetEntryIdleTimeout( ExpirationAction action, uint32_t idleTimeout );

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
        void SetEntryTimeToLive( ExpirationAction action, uint32_t timeToLive );

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
        void SetRegionIdleTimeout( ExpirationAction action, uint32_t idleTimeout );

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
        void SetRegionTimeToLive( ExpirationAction action, uint32_t timeToLive );


        // PERSISTENCE

        /// <summary>
        /// Sets the PersistenceManager object that will be invoked for the persistence of the region.
        /// </summary>
        /// <param name="persistenceManager">
        /// Persistence Manager object
        /// </param>
        //generic<class TKey, class TValue>
        void SetPersistenceManager(IPersistenceManager<TKey, TValue>^ persistenceManager);

        /// <summary>
        /// Sets the PersistenceManager object that will be invoked for the persistence of the region.
        /// </summary>
        /// <param name="persistenceManager">
        /// Persistence Manager object
        /// </param>
        /// <param name="config">
        /// The configuration properties to use for the PersistenceManager.
        /// </param>
        //generic<class TKey, class TValue>
        void SetPersistenceManager(IPersistenceManager<TKey, TValue>^ persistenceManager, Properties<String^, String^>^ config);
        

        /// <summary>
        /// Sets the library path for the library that will be invoked for the persistence of the region.
        /// If the region is being created from a client on a server, or on a server directly, then
        /// This must be used to set the PersistenceManager.
        /// </summary>
        /// <param name="libPath">The path of the PersistenceManager shared library.</param>
        /// <param name="factoryFunctionName">
        /// The name of the factory function to create an instance of PersistenceManager object.
        /// </param>
        void SetPersistenceManager( String^ libPath, String^ factoryFunctionName );

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
        void SetPersistenceManager( String^ libPath, String^ factoryFunctionName,
          /*Dictionary<Object^, Object^>*/Properties<String^, String^>^ config );


        // STORAGE ATTRIBUTES

        /// <summary>
        /// Set the pool name for a Thin Client region.
        /// </summary>
        /// <remarks>
        /// The pool with the name specified must be already created.
        /// </remarks>
        /// <param name="poolName">
        /// The name of the pool to attach to this region.
        /// </param>
        void SetPoolName( String^ poolName );

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
        void SetInitialCapacity( int32_t initialCapacity );

        /// <summary>
        /// Sets the entry load factor for the next <c>RegionAttributes</c>
        /// created. This value is
        /// used in initializing the map that holds the entries.
        /// </summary>
        /// <param name="loadFactor">the load factor of the entry map</param>
        /// <exception cref="IllegalArgumentException">
        /// if loadFactor is nonpositive
        /// </exception>
        void SetLoadFactor( Single loadFactor );

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
        void SetConcurrencyLevel( int32_t concurrencyLevel );

        /// <summary>
        /// Sets a limit on the number of entries that will be held in the cache.
        /// If a new entry is added while at the limit, the cache will evict the
        /// least recently used entry.
        /// </summary>
        /// <param name="entriesLimit">
        /// The limit of the number of entries before eviction starts.
        /// Defaults to 0, meaning no LRU actions will used.
        /// </param>
        void SetLruEntriesLimit( uint32_t entriesLimit );

        /// <summary>
        /// Sets the disk policy type for the next <c>RegionAttributes</c> created.
        /// </summary>
        /// <param name="diskPolicy">
        /// the disk policy to use for the region
        /// </param>
        void SetDiskPolicy( DiskPolicyType diskPolicy );

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
        void SetCachingEnabled( bool cachingEnabled );
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
        void SetCloningEnabled( bool cloningEnabled );

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
        void SetConcurrencyChecksEnabled( bool concurrencyChecksEnabled );
        // FACTORY METHOD

        /// <summary>
        /// Creates a <c>RegionAttributes</c> with the current settings.
        /// </summary>
        /// <returns>the newly created <c>RegionAttributes</c></returns>
        /// <exception cref="IllegalStateException">
        /// if the current settings violate the
        /// compatibility rules.
        /// </exception>
        RegionAttributes<TKey, TValue>^ CreateRegionAttributes( );
      };

    }
  }
}
 } //namespace 
