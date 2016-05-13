/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include "cppcache/Cache.hpp"
#include "impl/NativeWrapper.hpp"
#include "CacheableHashMapM.hpp"
#include "LogM.hpp"
#include "ExceptionTypesM.hpp"

using namespace System;
using namespace System::Collections::Generic;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      ref class Cache;
      ref class RegionEntry;
      ref class RegionAttributes;
      ref class AttributesMutator;
      ref class CacheStatistics;
      interface class IGFSerializable;
      ref class Serializable;
      ref class CacheableKey;
      interface class ISelectResults;
      interface class IRegionService;

      /// <summary>
      /// Encapsulates a concrete region of cached data.
      /// </summary>
      /// <remarks>
      /// This class manages subregions and cached data. Each region
      /// can contain multiple subregions and entries for data.
      /// Regions provide a hierachical name space
      /// within the cache. Also, a region can be used to group cached
      /// objects for management purposes.
      ///
      /// Entries managed by the region are key-value pairs. A set of region attributes
      /// is associated with the region when it is created.
      ///
      /// The Region interface basically contains two set of APIs: Region management
      /// APIs and (potentially) distributed operations on entries. Non-distributed
      /// operations on entries  are provided by <c>RegionEntry</c>.
      ///
      /// Each <c>Cache</c> defines regions called the root regions.
      /// User applications can use the root regions to create subregions
      /// for isolated name spaces and object grouping.
      ///
      /// A region's name can be any string, except that it must not contain
      /// the region name separator, a forward slash (/).
      ///
      /// <c>Regions</c>  can be referenced by a relative path name from any region
      /// higher in the hierarchy in <see cref="Region.GetSubRegion" />. You can get the relative
      /// path from the root region with <see cref="Region.FullPath" />. The name separator
      /// is used to concatenate all the region names together from the root, starting
      /// with the root's subregions.
      /// </remarks>
      /// <see cref="RegionAttributes" />
      [Obsolete("Use classes and APIs from the GemStone.GemFire.Cache.Generic namespace")]
      public ref class Region
        : public Internal::SBWrap<gemfire::Region>
      {
      public:

        /// <summary>
        /// Gets the region name.
        /// </summary>
        /// <returns>
        /// region's name
        /// </returns>
        property String^ Name
        {
          String^ get( );
        }

        /// <summary>
        /// Gets the region's full path, which can be used to get this region object
        /// with <see cref="Cache.GetRegion" />.
        /// </summary>
        /// <returns>
        /// region's pathname
        /// </returns>
        property String^ FullPath
        {
          String^ get( );
        }

        /// <summary>
        /// Gets the parent region.
        /// </summary>
        /// <returns>
        /// region's parent, if any, or null if this is a root region
        /// </returns>
        /// <exception cref="RegionDestroyedException">
        /// if the region has been destroyed
        /// </exception>
        property Region^ ParentRegion
        {
          Region^ get( );
        }

        /// <summary>
        /// Returns the attributes for this region, which can be used to create a new
        /// region with <see cref="Cache.CreateRegion" />.
        /// </summary>
        /// <returns>
        /// region's attributes
        /// </returns>
        property RegionAttributes^ Attributes
        {
          RegionAttributes^ get( );
        }

        /// <summary>
        /// Return a mutator object for changing a subset of the
        /// region attributes.
        /// </summary>
        /// <returns>
        /// attribute mutator
        /// </returns>
        /// <exception cref="RegionDestroyedException">
        /// if the region has been destroyed
        /// </exception>
        AttributesMutator^ GetAttributesMutator( );

        /// <summary>
        /// Returns the statistics for this region.
        /// </summary>
        /// <returns>the <c>CacheStatistics</c> for this region</returns>
        /// <exception cref="StatisticsDisabledException">
        /// if statistics have been disabled for this region
        /// </exception>
        property CacheStatistics^ Statistics
        {
          CacheStatistics^ get( );
        }

        /// <summary>
        /// Invalidates this region.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The invalidation will cascade to all the subregions and cached
        /// entries. The region
        /// and the entries in it will still exist.
        /// </para>
        /// <para>
        /// This operation is not distributed for native clients
        /// </para>
        /// <para>
        /// To remove all the
        /// entries and the region, use <see cref="DestroyRegion" />.
        /// </para><para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="callback">
        /// user-defined parameter to pass to callback events triggered by this method
        /// </param>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if this region has been destroyed
        /// </exception>
        /// <seealso cref="LocalInvalidateRegion" />
        /// <seealso cref="DestroyRegion" />
        /// <seealso cref="ICacheListener.AfterRegionInvalidate" />
        void InvalidateRegion( IGFSerializable^ callback );

        /// <summary>
        /// Invalidates this region.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The invalidation will cascade to all the subregions and cached
        /// entries. The region
        /// and the entries in it will still exist.
        /// </para>
        /// <para>
        /// This operation is not distributed for native clients
        /// </para>
        /// <para>
        /// To remove all the
        /// entries and the region, use <see cref="DestroyRegion" />.
        /// </para><para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception; if this occurs some
        /// subregions may have already been successfully invalidated
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if this region has been destroyed
        /// </exception>
        /// <seealso cref="LocalInvalidateRegion" />
        /// <seealso cref="DestroyRegion" />
        /// <seealso cref="ICacheListener.AfterRegionInvalidate" />
        inline void InvalidateRegion( )
        {
          InvalidateRegion( nullptr );
        }

        /// <summary>
        /// Invalidates this region without distributing to other caches.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The invalidation will cascade to all the local subregions and cached
        /// entries. The region
        /// and the entries in it will still exist.
        /// </para><para>
        /// To remove all the
        /// entries and the region, use <see cref="LocalDestroyRegion" />.
        /// </para><para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="callback">
        /// a user-defined parameter to pass to callback events triggered by this method
        /// </param>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception; if this occurs some
        /// subregions may have already been successfully invalidated
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if the region is no longer valid
        /// </exception>
        /// <seealso cref="InvalidateRegion" />
        /// <seealso cref="LocalDestroyRegion" />
        /// <seealso cref="ICacheListener.AfterRegionInvalidate" />
        void LocalInvalidateRegion( IGFSerializable^ callback );

        /// <summary>
        /// Invalidates this region without distributing to other caches.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The invalidation will cascade to all the local subregions and cached
        /// entries. The region
        /// and the entries in it will still exist.
        /// </para><para>
        /// To remove all the
        /// entries and the region, use <see cref="LocalDestroyRegion" />.
        /// </para><para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception; if this occurs some
        /// subregions may have already been successfully invalidated
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if the region is no longer valid
        /// </exception>
        /// <seealso cref="InvalidateRegion" />
        /// <seealso cref="LocalDestroyRegion" />
        /// <seealso cref="ICacheListener.AfterRegionInvalidate" />
        inline void LocalInvalidateRegion( )
        {
          LocalInvalidateRegion( nullptr );
        }

        /// <summary>
        /// Destroys the whole distributed region and provides a user-defined parameter
        /// object to any <c>ICacheWriter</c> invoked in the process.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Destroy cascades to all entries and subregions. After the destroy,
        /// this region object can not be used any more. Any attempt to use
        /// this region object will get a <c>RegionDestroyedException</c>
        /// The region destroy not only destroys the local region but also destroys the
        /// server region.
        /// </para><para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="callback">
        /// a user-defined parameter to pass to callback events triggered by this call
        /// </param>
        /// <exception cref="CacheWriterException">
        /// if a CacheWriter aborts the operation; if this occurs some
        /// subregions may have already been successfully destroyed.
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception; if this occurs some
        /// subregions may have already been successfully invalidated
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <seealso cref="InvalidateRegion" />
        void DestroyRegion( IGFSerializable^ callback );

        /// <summary>
        /// Destroys the whole distributed region and provides a user-defined parameter
        /// object to any <c>ICacheWriter</c> invoked in the process.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Destroy cascades to all entries and subregions. After the destroy,
        /// this region object can not be used any more. Any attempt to use
        /// this region object will get a <c>RegionDestroyedException</c>
        /// The region destroy not only destroys the local region but also destroys the
        /// server region.
        /// </para><para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <exception cref="CacheWriterException">
        /// if a CacheWriter aborts the operation; if this occurs some
        /// subregions may have already been successfully destroyed.
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception; if this occurs some
        /// subregions may have already been successfully invalidated
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <seealso cref="InvalidateRegion" />
        inline void DestroyRegion( )
        {
          DestroyRegion( nullptr );
        }

        /// <summary>
        /// Destroys the whole local region and provides a user-defined parameter
        /// object to any <c>ICacheWriter</c> invoked in the process.
        /// The region destroy is not distributed to other caches.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Destroy cascades to all entries and subregions. After the destroy,
        /// any attempt to use
        /// this region object will get a <c>RegionDestroyedException</c>.
        /// </para><para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="callback">
        /// a user-defined parameter to pass to callback events triggered by this call
        /// </param>
        /// <exception cref="CacheWriterException">
        /// if a CacheWriter aborts the operation; if this occurs some
        /// subregions may have already been successfully destroyed.
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception; if this occurs some
        /// subregions may have already been successfully invalidated
        /// </exception>
        /// <seealso cref="DestroyRegion" />
        /// <seealso cref="LocalInvalidateRegion" />
        void LocalDestroyRegion( IGFSerializable^ callback );

        /// <summary>
        /// Destroys the whole local region and provides a user-defined parameter
        /// object to any <c>ICacheWriter</c> invoked in the process.
        /// The region destroy is not distributed to other caches.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Destroy cascades to all entries and subregions. After the destroy,
        /// any attempt to use
        /// this region object will get a <c>RegionDestroyedException</c>.
        /// </para><para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <exception cref="CacheWriterException">
        /// if a CacheWriter aborts the operation; if this occurs some
        /// subregions may have already been successfully destroyed.
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception; if this occurs some
        /// subregions may have already been successfully invalidated
        /// </exception>
        /// <seealso cref="DestroyRegion" />
        /// <seealso cref="LocalInvalidateRegion" />
        inline void LocalDestroyRegion( ) { LocalDestroyRegion( nullptr ); }

        /// <summary>
        /// Returns the subregion identified by the path, null if no such subregion.
        /// </summary>
        /// <param name="path">path</param>
        /// <returns>subregion, or null if none</returns>
        /// <seealso cref="FullPath" />
        /// <seealso cref="SubRegions" />
        /// <seealso cref="ParentRegion" />
        Region^ GetSubRegion( String^ path );

        /// <summary>
        /// Creates a subregion with the given name and attributes.
        /// </summary>
        /// <param name="subRegionName">new subregion name</param>
        /// <param name="attributes">subregion attributes</param>
        /// <returns>new subregion</returns>
        /// <seealso cref="CreateServerSubRegion" />
        Region^ CreateSubRegion( String^ subRegionName, RegionAttributes^ attributes );

        /// <summary>
        /// Returns the subregions of this region.
        /// </summary>
        /// <param name="recursive">if true, also return all nested subregions</param>
        /// <returns>array of regions</returns>
        /// <exception cref="RegionDestroyedException">
        /// this region has already been destroyed
        /// </exception>
        array<Region^>^ SubRegions( bool recursive );

        /// <summary>
        /// Return the meta-object RegionEntry for the given key.
        /// </summary>
        /// <param name="key">key to use</param>
        /// <returns>region entry object</returns>
        /// <exception cref="IllegalArgumentException">key is null</exception>
        /// <exception cref="RegionDestroyedException">
        /// region has been destroyed
        /// </exception>
        RegionEntry^ GetEntry( GemStone::GemFire::Cache::ICacheableKey^ key );

        /// <summary>
        /// Return the meta-object RegionEntry for the given key.
        /// </summary>
        /// <param name="key">key to use</param>
        /// <returns>region entry object</returns>
        /// <exception cref="IllegalArgumentException">key is null</exception>
        /// <exception cref="RegionDestroyedException">
        /// region has been destroyed
        /// </exception>
        RegionEntry^ GetEntry( CacheableKey^ key );

        /// <summary>
        /// Returns the value for the given key, passing the callback argument
        /// to any cache loaders or that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If the value is not present locally then it is requested from the java server.
        /// If even that is unsuccessful then a local CacheLoader will be invoked if there is one.
        /// </para>
        /// <para>
        /// The value returned by get is not copied, so multi-threaded applications
        /// should not modify the value directly, but should use the update methods.
        /// </para><para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" />
        /// <see cref="CacheStatistics.HitCount" />, <see cref="CacheStatistics.MissCount" />,
        /// and <see cref="CacheStatistics.LastModifiedTime" /> (if a new value is loaded)
        /// for this region and the entry.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// key whose associated value is to be returned -- the key
        /// object must implement the Equals and GetHashCode methods.
        /// </param>
        /// <param name="callback">
        /// An argument passed into the CacheLoader if loader is used.
        /// Has to be Serializable (i.e. implement <c>IGFSerializable</c>);
        /// can be null.
        /// </param>
        /// <returns>
        /// value, or null if the value is not found and can't be loaded
        /// </returns>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="CacheLoaderException">
        /// if CacheLoader throws an exception
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="MessageException">
        /// If the message received from server could not be handled. This will
        /// be the case when an unregistered typeId is received in the reply or
        /// reply is not well formed. More information can be found in the log.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if this region has been destroyed
        /// </exception>
        /// <seealso cref="Put" />
        IGFSerializable^ Get( GemStone::GemFire::Cache::ICacheableKey^ key,
          IGFSerializable^ callback );

        /// <summary>
        /// Returns the value for the given key, passing the callback argument
        /// to any cache loaders or that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If the value is not present locally then it is requested from the java server.
        /// If even that is unsuccessful then a local CacheLoader will be invoked if there is one.
        /// </para>
        /// <para>
        /// The value returned by get is not copied, so multi-threaded applications
        /// should not modify the value directly, but should use the update methods.
        /// </para><para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" />
        /// <see cref="CacheStatistics.HitCount" />, <see cref="CacheStatistics.MissCount" />,
        /// and <see cref="CacheStatistics.LastModifiedTime" /> (if a new value is loaded)
        /// for this region and the entry.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// key whose associated value is to be returned -- the key
        /// object must implement the Equals and GetHashCode methods.
        /// </param>
        /// <param name="callback">
        /// An argument passed into the CacheLoader if loader is used.
        /// Has to be Serializable (i.e. implement <c>IGFSerializable</c>);
        /// can be null.
        /// </param>
        /// <returns>
        /// value, or null if the value is not found and can't be loaded
        /// </returns>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="CacheLoaderException">
        /// if CacheLoader throws an exception
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="MessageException">
        /// If the message received from server could not be handled. This will
        /// be the case when an unregistered typeId is received in the reply or
        /// reply is not well formed. More information can be found in the log.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if this region has been destroyed
        /// </exception>
        /// <seealso cref="Put" />
        IGFSerializable^ Get( CacheableKey^ key,
          IGFSerializable^ callback );

        /// <summary>
        /// Returns the value for the given key, passing the callback argument
        /// to any cache loaders or that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If the value is not present locally then it is requested from the java server.
        /// If even that is unsuccessful then a local CacheLoader will be invoked if there is one.
        /// </para>
        /// <para>
        /// The value returned by get is not copied, so multi-threaded applications
        /// should not modify the value directly, but should use the update methods.
        /// </para><para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" />
        /// <see cref="CacheStatistics.HitCount" />, <see cref="CacheStatistics.MissCount" />,
        /// and <see cref="CacheStatistics.LastModifiedTime" /> (if a new value is loaded)
        /// for this region and the entry.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// key whose associated value is to be returned -- the key
        /// object must implement the Equals and GetHashCode methods.
        /// </param>
        /// <returns>
        /// value, or null if the value is not found and can't be loaded
        /// </returns>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="CacheLoaderException">
        /// if CacheLoader throws an exception
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="MessageException">
        /// If the message received from server could not be handled. This will
        /// be the case when an unregistered typeId is received in the reply or
        /// reply is not well formed. More information can be found in the log.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if this region has been destroyed
        /// </exception>
        /// <seealso cref="Put" />
        inline IGFSerializable^ Get( GemStone::GemFire::Cache::ICacheableKey^ key )
        {
          return Get( key, nullptr );
        }
        /// <summary>
	/// check to see if the key is present on the server
        /// </summary>

        Boolean  ContainsKeyOnServer( CacheableKey^ key );


        /// <summary>
        /// Returns the value for the given key, passing the callback argument
        /// to any cache loaders or that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If the value is not present locally then it is requested from the java server.
        /// If even that is unsuccessful then a local CacheLoader will be invoked if there is one.
        /// </para>
        /// <para>
        /// The value returned by get is not copied, so multi-threaded applications
        /// should not modify the value directly, but should use the update methods.
        /// </para><para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" />
        /// <see cref="CacheStatistics.HitCount" />, <see cref="CacheStatistics.MissCount" />,
        /// and <see cref="CacheStatistics.LastModifiedTime" /> (if a new value is loaded)
        /// for this region and the entry.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// key whose associated value is to be returned -- the key
        /// object must implement the Equals and GetHashCode methods.
        /// </param>
        /// <returns>
        /// value, or null if the value is not found and can't be loaded
        /// </returns>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="CacheLoaderException">
        /// if CacheLoader throws an exception
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="MessageException">
        /// If the message received from server could not be handled. This will
        /// be the case when an unregistered typeId is received in the reply or
        /// reply is not well formed. More information can be found in the log.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if this region has been destroyed
        /// </exception>
        /// <seealso cref="Put" />
        inline IGFSerializable^ Get( CacheableKey^ key )
        {
          return Get( key, nullptr );
        }

        /// <summary>
        /// Puts a new value into an entry in this region with the specified key,
        /// passing the callback argument to any cache writers and cache listeners
        /// that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If there is already an entry associated with the specified key in
        /// this region, the entry's previous value is overwritten.
        /// The new put value is propogated to the java server to which it is connected with.
        /// Put is intended for very simple caching situations. In general
        /// it is better to create a <c>ICacheLoader</c> object and allow the
        /// cache to manage the creation and loading of objects.
        /// </para><para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region and the entry.
        /// </para><para>
        /// If remote server put fails throwing back a <c>CacheServerException</c>
        /// or security exception, then local put is tried to rollback. However,
        /// if the entry has overflowed/evicted/expired then the rollback is
        /// aborted since it may be due to a more recent notification or update
        /// by another thread.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// a key object associated with the value to be put into this region.
        /// </param>
        /// <param name="value">the value to be put into this region</param>
        /// <param name="callback">
        /// argument that is passed to the callback functions
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if  there is not enough memory for the value
        /// </exception>
        /// <seealso cref="Get" />
        /// <seealso cref="Create" />
        void Put( GemStone::GemFire::Cache::ICacheableKey^ key, IGFSerializable^ value,
          IGFSerializable^ callback );


        /// <summary>
        /// Puts a new value into an entry in this region with the specified key,
        /// passing the callback argument to any cache writers and cache listeners
        /// that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If there is already an entry associated with the specified key in
        /// this region, the entry's previous value is overwritten.
        /// The new put value is propogated to the java server to which it is connected with.
        /// Put is intended for very simple caching situations. In general
        /// it is better to create a <c>ICacheLoader</c> object and allow the
        /// cache to manage the creation and loading of objects.
        /// </para><para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region and the entry.
        /// </para><para>
        /// If remote server put fails throwing back a <c>CacheServerException</c>
        /// or security exception, then local put is tried to rollback. However,
        /// if the entry has overflowed/evicted/expired then the rollback is
        /// aborted since it may be due to a more recent notification or update
        /// by another thread.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// a key object associated with the value to be put into this region.
        /// </param>
        /// <param name="value">the value to be put into this region</param>
        /// <param name="callback">
        /// argument that is passed to the callback functions
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if  there is not enough memory for the value
        /// </exception>
        /// <seealso cref="Get" />
        /// <seealso cref="Create" />
        void Put( CacheableKey^ key, IGFSerializable^ value,
          IGFSerializable^ callback );


        /// <summary>
        /// Puts a new value into an entry in this region with the specified key,
        /// passing the callback argument to any cache writers and cache listeners
        /// that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If there is already an entry associated with the specified key in
        /// this region, the entry's previous value is overwritten.
        /// The new put value is propogated to the java server to which it is connected with.
        /// Put is intended for very simple caching situations. In general
        /// it is better to create a <c>ICacheLoader</c> object and allow the
        /// cache to manage the creation and loading of objects.
        /// </para><para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region and the entry.
        /// </para><para>
        /// If remote server put fails throwing back a <c>CacheServerException</c>
        /// or security exception, then local put is tried to rollback. However,
        /// if the entry has overflowed/evicted/expired then the rollback is
        /// aborted since it may be due to a more recent notification or update
        /// by another thread.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// a key object associated with the value to be put into this region.
        /// </param>
        /// <param name="value">the value to be put into this region</param>
        /// <param name="callback">
        /// argument that is passed to the callback functions
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if  there is not enough memory for the value
        /// </exception>
        /// <seealso cref="Get" />
        /// <seealso cref="Create" />
        void Put( GemStone::GemFire::Cache::ICacheableKey^ key, GemStone::GemFire::Cache::Serializable^ value,
          IGFSerializable^ callback );


        /// <summary>
        /// Puts a new value into an entry in this region with the specified key,
        /// passing the callback argument to any cache writers and cache listeners
        /// that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If there is already an entry associated with the specified key in
        /// this region, the entry's previous value is overwritten.
        /// The new put value is propogated to the java server to which it is connected with.
        /// Put is intended for very simple caching situations. In general
        /// it is better to create a <c>ICacheLoader</c> object and allow the
        /// cache to manage the creation and loading of objects.
        /// </para><para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region and the entry.
        /// </para><para>
        /// If remote server put fails throwing back a <c>CacheServerException</c>
        /// or security exception, then local put is tried to rollback. However,
        /// if the entry has overflowed/evicted/expired then the rollback is
        /// aborted since it may be due to a more recent notification or update
        /// by another thread.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// a key object associated with the value to be put into this region.
        /// </param>
        /// <param name="value">the value to be put into this region</param>
        /// <param name="callback">
        /// argument that is passed to the callback functions
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if  there is not enough memory for the value
        /// </exception>
        /// <seealso cref="Get" />
        /// <seealso cref="Create" />
        void Put( CacheableKey^ key, GemStone::GemFire::Cache::Serializable^ value,
          IGFSerializable^ callback );


        /// <summary>
        /// Puts a new value into an entry in this region with the specified key.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If there is already an entry associated with the specified key in
        /// this region, the entry's previous value is overwritten.
        /// The new put value is propogated to the java server to which it is connected with.
        /// Put is intended for very simple caching situations. In general
        /// it is better to create a <c>ICacheLoader</c> object and allow the
        /// cache to manage the creation and loading of objects.
        /// </para><para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region and the entry.
        /// </para><para>
        /// If remote server put fails throwing back a <c>CacheServerException</c>
        /// or security exception, then local put is tried to rollback. However,
        /// if the entry has overflowed/evicted/expired then the rollback is
        /// aborted since it may be due to a more recent notification or update
        /// by another thread.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// a key object associated with the value to be put into this region.
        /// </param>
        /// <param name="value">the value to be put into this region</param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if  there is not enough memory for the value
        /// </exception>
        /// <seealso cref="Get" />
        /// <seealso cref="Create" />
        inline void Put( GemStone::GemFire::Cache::ICacheableKey^ key, IGFSerializable^ value )
        {
          Put( key, value, nullptr );
        }


        /// <summary>
        /// Puts a new value into an entry in this region with the specified key.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If there is already an entry associated with the specified key in
        /// this region, the entry's previous value is overwritten.
        /// The new put value is propogated to the java server to which it is connected with.
        /// Put is intended for very simple caching situations. In general
        /// it is better to create a <c>ICacheLoader</c> object and allow the
        /// cache to manage the creation and loading of objects.
        /// </para><para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region and the entry.
        /// </para><para>
        /// If remote server put fails throwing back a <c>CacheServerException</c>
        /// or security exception, then local put is tried to rollback. However,
        /// if the entry has overflowed/evicted/expired then the rollback is
        /// aborted since it may be due to a more recent notification or update
        /// by another thread.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// a key object associated with the value to be put into this region.
        /// </param>
        /// <param name="value">the value to be put into this region</param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if  there is not enough memory for the value
        /// </exception>
        /// <seealso cref="Get" />
        /// <seealso cref="Create" />
        inline void Put( CacheableKey^ key, IGFSerializable^ value )
        {
          Put( key, value, nullptr );
        }


        /// <summary>
        /// Puts a new value into an entry in this region with the specified key.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If there is already an entry associated with the specified key in
        /// this region, the entry's previous value is overwritten.
        /// The new put value is propogated to the java server to which it is connected with.
        /// Put is intended for very simple caching situations. In general
        /// it is better to create a <c>ICacheLoader</c> object and allow the
        /// cache to manage the creation and loading of objects.
        /// </para><para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region and the entry.
        /// </para><para>
        /// If remote server put fails throwing back a <c>CacheServerException</c>
        /// or security exception, then local put is tried to rollback. However,
        /// if the entry has overflowed/evicted/expired then the rollback is
        /// aborted since it may be due to a more recent notification or update
        /// by another thread.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// a key object associated with the value to be put into this region.
        /// </param>
        /// <param name="value">the value to be put into this region</param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if  there is not enough memory for the value
        /// </exception>
        /// <seealso cref="Get" />
        /// <seealso cref="Create" />
        inline void Put( GemStone::GemFire::Cache::ICacheableKey^ key, GemStone::GemFire::Cache::Serializable^ value )
        {
          Put( key, value, nullptr );
        }


        /// <summary>
        /// Puts a new value into an entry in this region with the specified key.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If there is already an entry associated with the specified key in
        /// this region, the entry's previous value is overwritten.
        /// The new put value is propogated to the java server to which it is connected with.
        /// Put is intended for very simple caching situations. In general
        /// it is better to create a <c>ICacheLoader</c> object and allow the
        /// cache to manage the creation and loading of objects.
        /// </para><para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region and the entry.
        /// </para><para>
        /// If remote server put fails throwing back a <c>CacheServerException</c>
        /// or security exception, then local put is tried to rollback. However,
        /// if the entry has overflowed/evicted/expired then the rollback is
        /// aborted since it may be due to a more recent notification or update
        /// by another thread.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// a key object associated with the value to be put into this region.
        /// </param>
        /// <param name="value">the value to be put into this region</param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if  there is not enough memory for the value
        /// </exception>
        /// <seealso cref="Get" />
        /// <seealso cref="Create" />
        inline void Put( CacheableKey^ key, GemStone::GemFire::Cache::Serializable^ value )
        {
          Put( key, value, nullptr );
        }

        /// <summary>
        /// Puts a map of entries in this region.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If there is already an entry associated with any key in the map in
        /// this region, the entry's previous value is overwritten.
        /// The new values are propogated to the java server to which it is connected with.
        /// PutAll is intended for speed up large amount of put operation into
        /// the same region.
        /// </para>
        /// </remarks>
        /// <param name="map">
        /// A hashmap contains entries, i.e. (key, value) pairs. Value should
        /// not be null in any of the enties.
        /// </param>
        /// <param name="timeout">The time (in seconds) to wait for the PutAll
        /// response. It should be less than or equal to 2^31/1000 i.e. 2147483.
        /// Optional.
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// If timeout is more than 2^31/1000 i.e. 2147483.
        /// </exception>
        /// <exception cref="NullPointerException">
        /// if any value in the map is null
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if  there is not enough memory for the value
        /// </exception>
        /// <seealso cref="Put" />
        void PutAll(CacheableHashMap^ map, uint32_t timeout);

        /// <summary>
        /// Puts a map of entries in this region.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If there is already an entry associated with any key in the map in
        /// this region, the entry's previous value is overwritten.
        /// The new values are propogated to the java server to which it is connected with.
        /// PutAll is intended for speed up large amount of put operation into
        /// the same region.
        /// </para>
        /// </remarks>
        /// <param name="map">
        /// A hashmap contains entries, i.e. (key, value) pairs. Value should
        /// not be null in any of the enties.
        /// </param>
        /// <exception cref="NullPointerException">
        /// if any value in the map is null
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if  there is not enough memory for the value
        /// </exception>
        /// <seealso cref="Put" />
        void PutAll(CacheableHashMap^ map);

        /// <summary>
        /// Puts a new value into an entry in this region with the specified
        /// key in the local cache only, passing the callback argument to any
        /// cache writers and cache listeners that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If there is already an entry associated with the specified key in
        /// this region, the entry's previous value is overwritten.
        /// </para><para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region and the entry.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// a key object associated with the value to be put into this region.
        /// </param>
        /// <param name="value">the value to be put into this region</param>
        /// <param name="callbackArg">
        /// argument that is passed to the callback functions
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if  there is not enough memory for the value
        /// </exception>
        /// <seealso cref="Get" />
        /// <seealso cref="Create" />
        void LocalPut(GemStone::GemFire::Cache::ICacheableKey^ key, IGFSerializable^ value,
          IGFSerializable^ callbackArg);

        /// <summary>
        /// Puts a new value into an entry in this region with the specified
        /// key in the local cache only, passing the callback argument to any
        /// cache writers and cache listeners that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If there is already an entry associated with the specified key in
        /// this region, the entry's previous value is overwritten.
        /// </para><para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region and the entry.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// a key object associated with the value to be put into this region.
        /// </param>
        /// <param name="value">the value to be put into this region</param>
        /// <param name="callbackArg">
        /// argument that is passed to the callback functions
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if  there is not enough memory for the value
        /// </exception>
        /// <seealso cref="Get" />
        /// <seealso cref="Create" />
        void LocalPut(CacheableKey^ key, IGFSerializable^ value,
          IGFSerializable^ callbackArg);

        /// <summary>
        /// Puts a new value into an entry in this region with the specified
        /// key in the local cache only, passing the callback argument to any
        /// cache writers and cache listeners that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If there is already an entry associated with the specified key in
        /// this region, the entry's previous value is overwritten.
        /// </para><para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region and the entry.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// a key object associated with the value to be put into this region.
        /// </param>
        /// <param name="value">the value to be put into this region</param>
        /// <param name="callbackArg">
        /// argument that is passed to the callback functions
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if  there is not enough memory for the value
        /// </exception>
        /// <seealso cref="Get" />
        /// <seealso cref="Create" />
        void LocalPut(GemStone::GemFire::Cache::ICacheableKey^ key, GemStone::GemFire::Cache::Serializable^ value,
          IGFSerializable^ callbackArg);

        /// <summary>
        /// Puts a new value into an entry in this region with the specified
        /// key in the local cache only, passing the callback argument to any
        /// cache writers and cache listeners that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If there is already an entry associated with the specified key in
        /// this region, the entry's previous value is overwritten.
        /// </para><para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region and the entry.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// a key object associated with the value to be put into this region.
        /// </param>
        /// <param name="value">the value to be put into this region</param>
        /// <param name="callbackArg">
        /// argument that is passed to the callback functions
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if  there is not enough memory for the value
        /// </exception>
        /// <seealso cref="Get" />
        /// <seealso cref="Create" />
        void LocalPut(CacheableKey^ key, GemStone::GemFire::Cache::Serializable^ value,
          IGFSerializable^ callbackArg);

        /// <summary>
        /// Puts a new value into an entry in this region with the specified key
        /// in the local cache only.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If there is already an entry associated with the specified key in
        /// this region, the entry's previous value is overwritten.
        /// </para><para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region and the entry.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// a key object associated with the value to be put into this region.
        /// </param>
        /// <param name="value">the value to be put into this region</param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if  there is not enough memory for the value
        /// </exception>
        /// <seealso cref="Get" />
        /// <seealso cref="Create" />
        inline void LocalPut(GemStone::GemFire::Cache::ICacheableKey^ key, IGFSerializable^ value)
        {
          LocalPut(key, value, nullptr);
        }

        /// <summary>
        /// Puts a new value into an entry in this region with the specified key
        /// in the local cache only.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If there is already an entry associated with the specified key in
        /// this region, the entry's previous value is overwritten.
        /// </para><para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region and the entry.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// a key object associated with the value to be put into this region.
        /// </param>
        /// <param name="value">the value to be put into this region</param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if  there is not enough memory for the value
        /// </exception>
        /// <seealso cref="Get" />
        /// <seealso cref="Create" />
        inline void LocalPut(CacheableKey^ key, IGFSerializable^ value)
        {
          LocalPut(key, value, nullptr);
        }

        /// <summary>
        /// Puts a new value into an entry in this region with the specified key
        /// in the local cache only.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If there is already an entry associated with the specified key in
        /// this region, the entry's previous value is overwritten.
        /// </para><para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region and the entry.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// a key object associated with the value to be put into this region.
        /// </param>
        /// <param name="value">the value to be put into this region</param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if  there is not enough memory for the value
        /// </exception>
        /// <seealso cref="Get" />
        /// <seealso cref="Create" />
        inline void LocalPut(GemStone::GemFire::Cache::ICacheableKey^ key, GemStone::GemFire::Cache::Serializable^ value)
        {
          LocalPut(key, value, nullptr);
        }

        /// <summary>
        /// Puts a new value into an entry in this region with the specified key
        /// in the local cache only.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If there is already an entry associated with the specified key in
        /// this region, the entry's previous value is overwritten.
        /// </para><para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region and the entry.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// a key object associated with the value to be put into this region.
        /// </param>
        /// <param name="value">the value to be put into this region</param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if  there is not enough memory for the value
        /// </exception>
        /// <seealso cref="Get" />
        /// <seealso cref="Create" />
        inline void LocalPut(CacheableKey^ key, GemStone::GemFire::Cache::Serializable^ value)
        {
          LocalPut(key, value, nullptr);
        }

        /// <summary>
        /// Creates a new entry in this region with the specified key and value,
        /// passing the callback argument to any cache writers and cache listeners
        /// that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region
        /// and the entry.
        /// </para>
        /// <para>
        /// The new entry is propogated to the java server to which it is connected with.
        /// </para><para>
        /// If remote server put fails throwing back a <c>CacheServerException</c>
        /// or security exception, then local put is tried to rollback. However,
        /// if the entry has overflowed/evicted/expired then the rollback is
        /// aborted since it may be due to a more recent notification or update
        /// by another thread.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// The key for which to create the entry in this region. The object is
        /// created before the call, and the caller should not deallocate the object.
        /// </param>
        /// <param name="value">
        /// The value for the new entry, which may be null to indicate that the new
        /// entry starts as if it had been locally invalidated.
        /// </param>
        /// <param name="callbackArg">
        /// a custome parameter to pass to the cache writer or cache listener
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to a GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if there is not enough memory for the new entry
        /// </exception>
        /// <exception cref="EntryExistsException">
        /// if an entry with this key already exists
        /// </exception>
        /// <seealso cref="Put" />
        /// <seealso cref="Get" />
        void Create( GemStone::GemFire::Cache::ICacheableKey^ key, IGFSerializable^ value,
          IGFSerializable^ callbackArg );

        /// <summary>
        /// Creates a new entry in this region with the specified key and value,
        /// passing the callback argument to any cache writers and cache listeners
        /// that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region
        /// and the entry.
        /// </para>
        /// <para>
        /// The new entry is propogated to the java server to which it is connected with.
        /// </para><para>
        /// If remote server put fails throwing back a <c>CacheServerException</c>
        /// or security exception, then local put is tried to rollback. However,
        /// if the entry has overflowed/evicted/expired then the rollback is
        /// aborted since it may be due to a more recent notification or update
        /// by another thread.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// The key for which to create the entry in this region. The object is
        /// created before the call, and the caller should not deallocate the object.
        /// </param>
        /// <param name="value">
        /// The value for the new entry, which may be null to indicate that the new
        /// entry starts as if it had been locally invalidated.
        /// </param>
        /// <param name="callbackArg">
        /// a custome parameter to pass to the cache writer or cache listener
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to a GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if there is not enough memory for the new entry
        /// </exception>
        /// <exception cref="EntryExistsException">
        /// if an entry with this key already exists
        /// </exception>
        /// <seealso cref="Put" />
        /// <seealso cref="Get" />
        void Create( CacheableKey^ key, IGFSerializable^ value,
          IGFSerializable^ callbackArg );

        /// <summary>
        /// Creates a new entry in this region with the specified key and value,
        /// passing the callback argument to any cache writers and cache listeners
        /// that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region
        /// and the entry.
        /// </para>
        /// <para>
        /// The new entry is propogated to the java server to which it is connected with.
        /// </para><para>
        /// If remote server put fails throwing back a <c>CacheServerException</c>
        /// or security exception, then local put is tried to rollback. However,
        /// if the entry has overflowed/evicted/expired then the rollback is
        /// aborted since it may be due to a more recent notification or update
        /// by another thread.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// The key for which to create the entry in this region. The object is
        /// created before the call, and the caller should not deallocate the object.
        /// </param>
        /// <param name="value">
        /// The value for the new entry, which may be null to indicate that the new
        /// entry starts as if it had been locally invalidated.
        /// </param>
        /// <param name="callbackArg">
        /// a custome parameter to pass to the cache writer or cache listener
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to a GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if there is not enough memory for the new entry
        /// </exception>
        /// <exception cref="EntryExistsException">
        /// if an entry with this key already exists
        /// </exception>
        /// <seealso cref="Put" />
        /// <seealso cref="Get" />
        void Create( GemStone::GemFire::Cache::ICacheableKey^ key, GemStone::GemFire::Cache::Serializable^ value,
          IGFSerializable^ callbackArg );

        /// <summary>
        /// Creates a new entry in this region with the specified key and value,
        /// passing the callback argument to any cache writers and cache listeners
        /// that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region
        /// and the entry.
        /// </para>
        /// <para>
        /// The new entry is propogated to the java server to which it is connected with.
        /// </para><para>
        /// If remote server put fails throwing back a <c>CacheServerException</c>
        /// or security exception, then local put is tried to rollback. However,
        /// if the entry has overflowed/evicted/expired then the rollback is
        /// aborted since it may be due to a more recent notification or update
        /// by another thread.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// The key for which to create the entry in this region. The object is
        /// created before the call, and the caller should not deallocate the object.
        /// </param>
        /// <param name="value">
        /// The value for the new entry, which may be null to indicate that the new
        /// entry starts as if it had been locally invalidated.
        /// </param>
        /// <param name="callbackArg">
        /// a custome parameter to pass to the cache writer or cache listener
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to a GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if there is not enough memory for the new entry
        /// </exception>
        /// <exception cref="EntryExistsException">
        /// if an entry with this key already exists
        /// </exception>
        /// <seealso cref="Put" />
        /// <seealso cref="Get" />
        void Create( CacheableKey^ key, GemStone::GemFire::Cache::Serializable^ value,
          IGFSerializable^ callbackArg );

        /// <summary>
        /// Creates a new entry in this region with the specified key and value.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region
        /// and the entry.
        /// </para>
        /// <para>
        /// The new entry is propogated to the java server to which it is connected with.
        /// </para><para>
        /// If remote server put fails throwing back a <c>CacheServerException</c>
        /// or security exception, then local put is tried to rollback. However,
        /// if the entry has overflowed/evicted/expired then the rollback is
        /// aborted since it may be due to a more recent notification or update
        /// by another thread.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// The key for which to create the entry in this region. The object is
        /// created before the call, and the caller should not deallocate the object.
        /// </param>
        /// <param name="value">
        /// The value for the new entry, which may be null to indicate that the new
        /// entry starts as if it had been locally invalidated.
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to a GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if there is not enough memory for the new entry
        /// </exception>
        /// <exception cref="EntryExistsException">
        /// if an entry with this key already exists
        /// </exception>
        /// <seealso cref="Put" />
        /// <seealso cref="Get" />
        inline void Create( GemStone::GemFire::Cache::ICacheableKey^ key, IGFSerializable^ value )
        {
          Create( key, value, nullptr );
        }

        /// <summary>
        /// Creates a new entry in this region with the specified key and value.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region
        /// and the entry.
        /// </para>
        /// <para>
        /// The new entry is propogated to the java server to which it is connected with.
        /// </para><para>
        /// If remote server put fails throwing back a <c>CacheServerException</c>
        /// or security exception, then local put is tried to rollback. However,
        /// if the entry has overflowed/evicted/expired then the rollback is
        /// aborted since it may be due to a more recent notification or update
        /// by another thread.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// The key for which to create the entry in this region. The object is
        /// created before the call, and the caller should not deallocate the object.
        /// </param>
        /// <param name="value">
        /// The value for the new entry, which may be null to indicate that the new
        /// entry starts as if it had been locally invalidated.
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to a GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if there is not enough memory for the new entry
        /// </exception>
        /// <exception cref="EntryExistsException">
        /// if an entry with this key already exists
        /// </exception>
        /// <seealso cref="Put" />
        /// <seealso cref="Get" />
        inline void Create( CacheableKey^ key, IGFSerializable^ value )
        {
          Create( key, value, nullptr );
        }

        /// <summary>
        /// Creates a new entry in this region with the specified key and value.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region
        /// and the entry.
        /// </para>
        /// <para>
        /// The new entry is propogated to the java server to which it is connected with.
        /// </para><para>
        /// If remote server put fails throwing back a <c>CacheServerException</c>
        /// or security exception, then local put is tried to rollback. However,
        /// if the entry has overflowed/evicted/expired then the rollback is
        /// aborted since it may be due to a more recent notification or update
        /// by another thread.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// The key for which to create the entry in this region. The object is
        /// created before the call, and the caller should not deallocate the object.
        /// </param>
        /// <param name="value">
        /// The value for the new entry, which may be null to indicate that the new
        /// entry starts as if it had been locally invalidated.
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to a GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if there is not enough memory for the new entry
        /// </exception>
        /// <exception cref="EntryExistsException">
        /// if an entry with this key already exists
        /// </exception>
        /// <seealso cref="Put" />
        /// <seealso cref="Get" />
        inline void Create( GemStone::GemFire::Cache::ICacheableKey^ key, GemStone::GemFire::Cache::Serializable^ value )
        {
          Create( key, value, nullptr );
        }

        /// <summary>
        /// Creates a new entry in this region with the specified key and value.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region
        /// and the entry.
        /// </para>
        /// <para>
        /// The new entry is propogated to the java server to which it is connected with.
        /// </para><para>
        /// If remote server put fails throwing back a <c>CacheServerException</c>
        /// or security exception, then local put is tried to rollback. However,
        /// if the entry has overflowed/evicted/expired then the rollback is
        /// aborted since it may be due to a more recent notification or update
        /// by another thread.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// The key for which to create the entry in this region. The object is
        /// created before the call, and the caller should not deallocate the object.
        /// </param>
        /// <param name="value">
        /// The value for the new entry, which may be null to indicate that the new
        /// entry starts as if it had been locally invalidated.
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to a GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if there is not enough memory for the new entry
        /// </exception>
        /// <exception cref="EntryExistsException">
        /// if an entry with this key already exists
        /// </exception>
        /// <seealso cref="Put" />
        /// <seealso cref="Get" />
        inline void Create( CacheableKey^ key, GemStone::GemFire::Cache::Serializable^ value )
        {
          Create( key, value, nullptr );
        }

        /// <summary>
        /// Creates a new entry in this region with the specified key and value
        /// in the local cache only, passing the callback argument to any
        /// cache writers and cache listeners that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If there is already an entry associated with the specified key in
        /// this region, then an <c>EntryExistsException</c> is thrown.
        /// </para><para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region and the entry.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// the key object for which to create the entry in this region.
        /// </param>
        /// <param name="value">the value to be created in this region</param>
        /// <param name="callbackArg">
        /// argument that is passed to the callback functions
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="EntryExistsException">
        /// if an entry with this key already exists
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if  there is not enough memory for the value
        /// </exception>
        /// <seealso cref="Get" />
        /// <seealso cref="Create" />
        void LocalCreate(GemStone::GemFire::Cache::ICacheableKey^ key, IGFSerializable^ value,
          IGFSerializable^ callbackArg);

        /// <summary>
        /// Creates a new entry in this region with the specified key and value
        /// in the local cache only, passing the callback argument to any
        /// cache writers and cache listeners that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If there is already an entry associated with the specified key in
        /// this region, then an <c>EntryExistsException</c> is thrown.
        /// </para><para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region and the entry.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// the key object for which to create the entry in this region.
        /// </param>
        /// <param name="value">the value to be created in this region</param>
        /// <param name="callbackArg">
        /// argument that is passed to the callback functions
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="EntryExistsException">
        /// if an entry with this key already exists
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if  there is not enough memory for the value
        /// </exception>
        /// <seealso cref="Get" />
        /// <seealso cref="Create" />
        void LocalCreate(GemStone::GemFire::Cache::ICacheableKey^ key, GemStone::GemFire::Cache::Serializable^ value,
          IGFSerializable^ callbackArg);

        /// <summary>
        /// Creates a new entry in this region with the specified key and value
        /// in the local cache only, passing the callback argument to any
        /// cache writers and cache listeners that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If there is already an entry associated with the specified key in
        /// this region, then an <c>EntryExistsException</c> is thrown.
        /// </para><para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region and the entry.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// the key object for which to create the entry in this region.
        /// </param>
        /// <param name="value">the value to be created in this region</param>
        /// <param name="callbackArg">
        /// argument that is passed to the callback functions
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="EntryExistsException">
        /// if an entry with this key already exists
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if  there is not enough memory for the value
        /// </exception>
        /// <seealso cref="Get" />
        /// <seealso cref="Create" />
        void LocalCreate(CacheableKey^ key, IGFSerializable^ value,
          IGFSerializable^ callbackArg);

        /// <summary>
        /// Creates a new entry in this region with the specified key and value
        /// in the local cache only, passing the callback argument to any
        /// cache writers and cache listeners that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If there is already an entry associated with the specified key in
        /// this region, then an <c>EntryExistsException</c> is thrown.
        /// </para><para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region and the entry.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// the key object for which to create the entry in this region.
        /// </param>
        /// <param name="value">the value to be created in this region</param>
        /// <param name="callbackArg">
        /// argument that is passed to the callback functions
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="EntryExistsException">
        /// if an entry with this key already exists
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if  there is not enough memory for the value
        /// </exception>
        /// <seealso cref="Get" />
        /// <seealso cref="Create" />
        void LocalCreate(CacheableKey^ key, GemStone::GemFire::Cache::Serializable^ value,
          IGFSerializable^ callbackArg);

        /// <summary>
        /// Creates a new entry in this region with the specified key and value
        /// in the local cache only.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If there is already an entry associated with the specified key in
        /// this region, then an <c>EntryExistsException</c> is thrown.
        /// </para><para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region and the entry.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// the key object for which to create the entry in this region.
        /// </param>
        /// <param name="value">the value to be created in this region</param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="EntryExistsException">
        /// if an entry with this key already exists
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if  there is not enough memory for the value
        /// </exception>
        /// <seealso cref="Get" />
        /// <seealso cref="Create" />
        inline void LocalCreate(GemStone::GemFire::Cache::ICacheableKey^ key, IGFSerializable^ value)
        {
          LocalCreate(key, value, nullptr);
        }

        /// <summary>
        /// Creates a new entry in this region with the specified key and value
        /// in the local cache only.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If there is already an entry associated with the specified key in
        /// this region, then an <c>EntryExistsException</c> is thrown.
        /// </para><para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region and the entry.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// the key object for which to create the entry in this region.
        /// </param>
        /// <param name="value">the value to be created in this region</param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="EntryExistsException">
        /// if an entry with this key already exists
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if  there is not enough memory for the value
        /// </exception>
        /// <seealso cref="Get" />
        /// <seealso cref="Create" />
        inline void LocalCreate(GemStone::GemFire::Cache::ICacheableKey^ key, GemStone::GemFire::Cache::Serializable^ value)
        {
          LocalCreate(key, value, nullptr);
        }

        /// <summary>
        /// Creates a new entry in this region with the specified key and value
        /// in the local cache only.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If there is already an entry associated with the specified key in
        /// this region, then an <c>EntryExistsException</c> is thrown.
        /// </para><para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region and the entry.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// the key object for which to create the entry in this region.
        /// </param>
        /// <param name="value">the value to be created in this region</param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="EntryExistsException">
        /// if an entry with this key already exists
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if  there is not enough memory for the value
        /// </exception>
        /// <seealso cref="Get" />
        /// <seealso cref="Create" />
        inline void LocalCreate(CacheableKey^ key, IGFSerializable^ value)
        {
          LocalCreate(key, value, nullptr);
        }

        /// <summary>
        /// Creates a new entry in this region with the specified key and value
        /// in the local cache only.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If there is already an entry associated with the specified key in
        /// this region, then an <c>EntryExistsException</c> is thrown.
        /// </para><para>
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
        /// <see cref="CacheStatistics.LastModifiedTime" /> for this region and the entry.
        /// </para>
        /// </remarks>
        /// <param name="key">
        /// the key object for which to create the entry in this region.
        /// </param>
        /// <param name="value">the value to be created in this region</param>
        /// <exception cref="IllegalArgumentException">
        /// if key is null
        /// </exception>
        /// <exception cref="EntryExistsException">
        /// if an entry with this key already exists
        /// </exception>
        /// <exception cref="CacheWriterException">
        /// if CacheWriter aborts the operation
        /// </exception>
        /// <exception cref="CacheListenerException">
        /// if CacheListener throws an exception
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if region has been destroyed
        /// </exception>
        /// <exception cref="OutOfMemoryException">
        /// if  there is not enough memory for the value
        /// </exception>
        /// <seealso cref="Get" />
        /// <seealso cref="Create" />
        inline void LocalCreate(CacheableKey^ key, GemStone::GemFire::Cache::Serializable^ value)
        {
          LocalCreate(key, value, nullptr);
        }

        /// <summary>
        /// Invalidates the entry with the specified key,
        /// passing the callback argument
        /// to any cache
        /// listeners that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Invalidate only removes the value from the entry -- the key is kept intact.
        /// To completely remove the entry, call <see cref="Destroy" />.
        /// </para>
        /// <para>
        /// The invalidate is not propogated to the Gemfire cache server to which it is connected with.
        /// </para>
        /// <para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">key of the value to be invalidated</param>
        /// <param name="callback">
        /// a user-defined parameter to pass to callback events triggered by this method
        /// </param>
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="EntryNotFoundException">
        /// if this entry does not exist in this region locally
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if the region is destroyed
        /// </exception>
        /// <seealso cref="LocalInvalidate" />
        /// <seealso cref="Destroy" />
        /// <seealso cref="ICacheListener.AfterInvalidate" />
        void Invalidate( GemStone::GemFire::Cache::ICacheableKey^ key,
          IGFSerializable^ callback );

        /// <summary>
        /// Invalidates the entry with the specified key,
        /// passing the callback argument
        /// to any cache
        /// listeners that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Invalidate only removes the value from the entry -- the key is kept intact.
        /// To completely remove the entry, call <see cref="Destroy" />.
        /// </para>
        /// <para>
        /// The invalidate is not propogated to the Gemfire cache server to which it is connected with.
        /// </para>
        /// <para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">key of the value to be invalidated</param>
        /// <param name="callback">
        /// a user-defined parameter to pass to callback events triggered by this method
        /// </param>
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="EntryNotFoundException">
        /// if this entry does not exist in this region locally
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if the region is destroyed
        /// </exception>
        /// <seealso cref="LocalInvalidate" />
        /// <seealso cref="Destroy" />
        /// <seealso cref="ICacheListener.AfterInvalidate" />
        void Invalidate( CacheableKey^ key,
          IGFSerializable^ callback );

        /// <summary>
        /// Invalidates the entry with the specified key, passing the callback
        /// argument to any cache listeners that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Invalidate only removes the value from the entry -- the key is kept intact.
        /// To completely remove the entry, call <see cref="Destroy" />.
        /// </para>
        /// <para>
        /// The invalidate is not propogated to the Gemfire cache server to which it is connected with.
        /// </para>
        /// <para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">key of the value to be invalidated</param>
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="EntryNotFoundException">
        /// if this entry does not exist in this region locally
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if the region is destroyed
        /// </exception>
        /// <seealso cref="LocalInvalidate" />
        /// <seealso cref="Destroy" />
        /// <seealso cref="ICacheListener.AfterInvalidate" />
        inline void Invalidate( GemStone::GemFire::Cache::ICacheableKey^ key )
        {
          Invalidate( key, nullptr );
        }

        /// <summary>
        /// Invalidates the entry with the specified key, passing the callback
        /// argument to any cache listeners that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Invalidate only removes the value from the entry -- the key is kept intact.
        /// To completely remove the entry, call <see cref="Destroy" />.
        /// </para>
        /// <para>
        /// The invalidate is not propogated to the Gemfire cache server to which it is connected with.
        /// </para>
        /// <para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">key of the value to be invalidated</param>
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="EntryNotFoundException">
        /// if this entry does not exist in this region locally
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if the region is destroyed
        /// </exception>
        /// <seealso cref="LocalInvalidate" />
        /// <seealso cref="Destroy" />
        /// <seealso cref="ICacheListener.AfterInvalidate" />
        inline void Invalidate( CacheableKey^ key )
        {
          Invalidate( key, nullptr );
        }

        /// <summary>
        /// Locally invalidates the entry with the specified key, passing the
        /// callback argument to any cache listeners that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Invalidate only removes the value from the entry -- the key is kept intact.
        /// To completely remove the entry, call <see cref="Destroy" />.
        /// </para><para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">key of the value to be invalidated</param>
        /// <param name="callback">
        /// a user-defined parameter to pass to callback events triggered by this method
        /// </param>
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="EntryNotFoundException">
        /// if this entry does not exist in this region locally
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if the region is destroyed
        /// </exception>
        /// <seealso cref="Invalidate" />
        /// <seealso cref="Destroy" />
        /// <seealso cref="ICacheListener.AfterInvalidate" />
        void LocalInvalidate( GemStone::GemFire::Cache::ICacheableKey^ key,
          IGFSerializable^ callback );

        /// <summary>
        /// Locally invalidates the entry with the specified key, passing the
        /// callback argument to any cache listeners that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Invalidate only removes the value from the entry -- the key is kept intact.
        /// To completely remove the entry, call <see cref="Destroy" />.
        /// </para><para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">key of the value to be invalidated</param>
        /// <param name="callback">
        /// a user-defined parameter to pass to callback events triggered by this method
        /// </param>
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="EntryNotFoundException">
        /// if this entry does not exist in this region locally
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if the region is destroyed
        /// </exception>
        /// <seealso cref="Invalidate" />
        /// <seealso cref="Destroy" />
        /// <seealso cref="ICacheListener.AfterInvalidate" />
        void LocalInvalidate( CacheableKey^ key,
          IGFSerializable^ callback );

        /// <summary>
        /// Locally invalidates the entry with the specified key, passing the
        /// callback argument to any cache listeners that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Invalidate only removes the value from the entry -- the key is kept intact.
        /// To completely remove the entry, call <see cref="Destroy" />.
        /// </para><para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">key of the value to be invalidated</param>
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="EntryNotFoundException">
        /// if this entry does not exist in this region locally
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if the region is destroyed
        /// </exception>
        /// <seealso cref="Invalidate" />
        /// <seealso cref="Destroy" />
        /// <seealso cref="ICacheListener.AfterInvalidate" />
        inline void LocalInvalidate( GemStone::GemFire::Cache::ICacheableKey^ key )
        {
          LocalInvalidate( key, nullptr );
        }

        /// <summary>
        /// Locally invalidates the entry with the specified key, passing the
        /// callback argument to any cache listeners that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Invalidate only removes the value from the entry -- the key is kept intact.
        /// To completely remove the entry, call <see cref="Destroy" />.
        /// </para><para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">key of the value to be invalidated</param>
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="EntryNotFoundException">
        /// if this entry does not exist in this region locally
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if the region is destroyed
        /// </exception>
        /// <seealso cref="Invalidate" />
        /// <seealso cref="Destroy" />
        /// <seealso cref="ICacheListener.AfterInvalidate" />
        inline void LocalInvalidate( CacheableKey^ key )
        {
          LocalInvalidate( key, nullptr );
        }

        /// <summary>
        /// Destroys the entry with the specified key, passing the callback
        /// argument to any cache writers that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Destroy removes not only the value, but also the key and entry
        /// from this region.
        /// </para>
        /// <para>
        /// The destroy is propogated to the Gemfire cache server to which it is connected with.
        /// </para>
        /// <para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">the key of the entry to destroy</param>
        /// <param name="cacheWriterArg">
        /// a user-defined parameter to pass to cache writers triggered by this method
        /// </param>
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if this region has been destroyed
        /// </exception>
        /// <seealso cref="LocalDestroy" />
        /// <seealso cref="Invalidate" />
        /// <seealso cref="ICacheListener.AfterDestroy" />
        /// <seealso cref="ICacheWriter.BeforeDestroy" />
        void Destroy( GemStone::GemFire::Cache::ICacheableKey^ key,
          IGFSerializable^ cacheWriterArg );

        /// <summary>
        /// Destroys the entry with the specified key, passing the callback
        /// argument to any cache writers that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Destroy removes not only the value, but also the key and entry
        /// from this region.
        /// </para>
        /// <para>
        /// The destroy is propogated to the Gemfire cache server to which it is connected with.
        /// </para>
        /// <para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">the key of the entry to destroy</param>
        /// <param name="cacheWriterArg">
        /// a user-defined parameter to pass to cache writers triggered by this method
        /// </param>
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if this region has been destroyed
        /// </exception>
        /// <seealso cref="LocalDestroy" />
        /// <seealso cref="Invalidate" />
        /// <seealso cref="ICacheListener.AfterDestroy" />
        /// <seealso cref="ICacheWriter.BeforeDestroy" />
        void Destroy( CacheableKey^ key,
          IGFSerializable^ cacheWriterArg );

        /// <summary>
        /// Destroys the entry with the specified key, passing the callback
        /// argument to any cache writers that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Destroy removes not only the value, but also the key and entry
        /// from this region.
        /// </para>
        /// <para>
        /// The destroy is propogated to the Gemfire cache server to which it is connected with.
        /// </para>
        /// <para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">the key of the entry to destroy</param>
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if this region has been destroyed
        /// </exception>
        /// <seealso cref="LocalDestroy" />
        /// <seealso cref="Invalidate" />
        /// <seealso cref="ICacheListener.AfterDestroy" />
        /// <seealso cref="ICacheWriter.BeforeDestroy" />
        inline void Destroy( GemStone::GemFire::Cache::ICacheableKey^ key )
        {
          Destroy( key, nullptr );
        }

        /// <summary>
        /// Destroys the entry with the specified key, passing the callback
        /// argument to any cache writers that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Destroy removes not only the value, but also the key and entry
        /// from this region.
        /// </para>
        /// <para>
        /// The destroy is propogated to the Gemfire cache server to which it is connected with.
        /// </para>
        /// <para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">the key of the entry to destroy</param>
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if this region has been destroyed
        /// </exception>
        /// <seealso cref="LocalDestroy" />
        /// <seealso cref="Invalidate" />
        /// <seealso cref="ICacheListener.AfterDestroy" />
        /// <seealso cref="ICacheWriter.BeforeDestroy" />
        inline void Destroy( CacheableKey^ key )
        {
          Destroy( key, nullptr );
        }

        /// <summary>
        /// Destroys the value with the specified key in the local cache only.
        /// Destroy removes not only the value but also the key and entry
        /// from this region.
        /// </summary>
        /// <remarks>
        /// <para>
        /// No <c>ICacheWriter</c> is invoked.
        /// </para><para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">the key of the entry to destroy</param>
        /// <param name="cacheListenerArg">
        /// a user-defined parameter to pass to cache listeners triggered by this method
        /// </param>
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="IllegalStateException">
        /// if this region has mirroring enabled
        /// </exception>
        /// <exception cref="EntryNotFoundException">
        /// if the entry does not exist in this region locally
        /// </exception>
        /// <seealso cref="Destroy" />
        /// <seealso cref="LocalInvalidate" />
        /// <seealso cref="ICacheListener.AfterDestroy" />
        /// <seealso cref="ICacheWriter.BeforeDestroy" />
        void LocalDestroy( GemStone::GemFire::Cache::ICacheableKey^ key,
          IGFSerializable^ cacheListenerArg );

        /// <summary>
        /// Destroys the value with the specified key in the local cache only.
        /// Destroy removes not only the value but also the key and entry
        /// from this region.
        /// </summary>
        /// <remarks>
        /// <para>
        /// No <c>ICacheWriter</c> is invoked.
        /// </para><para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">the key of the entry to destroy</param>
        /// <param name="cacheListenerArg">
        /// a user-defined parameter to pass to cache listeners triggered by this method
        /// </param>
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="IllegalStateException">
        /// if this region has mirroring enabled
        /// </exception>
        /// <exception cref="EntryNotFoundException">
        /// if the entry does not exist in this region locally
        /// </exception>
        /// <seealso cref="Destroy" />
        /// <seealso cref="LocalInvalidate" />
        /// <seealso cref="ICacheListener.AfterDestroy" />
        /// <seealso cref="ICacheWriter.BeforeDestroy" />
        void LocalDestroy( CacheableKey^ key,
          IGFSerializable^ cacheListenerArg );

        /// <summary>
        /// Destroys the value with the specified key in the local cache only.
        /// Destroy removes not only the value but also the key and entry
        /// from this region.
        /// </summary>
        /// <remarks>
        /// <para>
        /// No <c>ICacheWriter</c> is invoked.
        /// </para><para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">the key of the entry to destroy</param>
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="IllegalStateException">
        /// if this region has mirroring enabled
        /// </exception>
        /// <exception cref="EntryNotFoundException">
        /// if the entry does not exist in this region locally
        /// </exception>
        /// <seealso cref="Destroy" />
        /// <seealso cref="LocalInvalidate" />
        /// <seealso cref="ICacheListener.AfterDestroy" />
        /// <seealso cref="ICacheWriter.BeforeDestroy" />
        inline void LocalDestroy( GemStone::GemFire::Cache::ICacheableKey^ key )
        {
          LocalDestroy( key, nullptr );
        }

        /// <summary>
        /// Destroys the value with the specified key in the local cache only.
        /// Destroy removes not only the value but also the key and entry
        /// from this region.
        /// </summary>
        /// <remarks>
        /// <para>
        /// No <c>ICacheWriter</c> is invoked.
        /// </para><para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">the key of the entry to destroy</param>
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="IllegalStateException">
        /// if this region has mirroring enabled
        /// </exception>
        /// <exception cref="EntryNotFoundException">
        /// if the entry does not exist in this region locally
        /// </exception>
        /// <seealso cref="Destroy" />
        /// <seealso cref="LocalInvalidate" />
        /// <seealso cref="ICacheListener.AfterDestroy" />
        /// <seealso cref="ICacheWriter.BeforeDestroy" />
        inline void LocalDestroy( CacheableKey^ key )
        {
          LocalDestroy( key, nullptr );
        }

        /// <summary>
        /// Returns all the keys in the local process for this region. This includes
        /// keys for which the entry is invalid.
        /// </summary>
        /// <returns>array of keys</returns>
        array<GemStone::GemFire::Cache::ICacheableKey^>^ GetKeys( );

        /// <summary>
        /// Returns the set of keys defined in the server process associated with this
        /// client and region. If a server has the region defined as a mirror, then
        /// this will be the entire keyset for the region across all server
        /// <c>Peer</c>s in the distributed system.
        /// </summary>
        /// <exception cref="UnsupportedOperationException">
        /// if the member type is not <c>Client</c>
        /// or region is not a Native Client region.
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="MessageException">
        /// If the message received from server could not be handled. This will
        /// be the case when an unregistered typeId is received in the reply or
        /// reply is not well formed. More information can be found in the log.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if there is a timeout getting the keys
        /// </exception>
        /// <returns>array of keys</returns>
        array<GemStone::GemFire::Cache::ICacheableKey^>^ GetServerKeys( );

        /// <summary>
        /// Returns all values in the local process for this region. No value is included
        /// for entries that are invalidated.
        /// </summary>
        /// <returns>array of values</returns>
        array<IGFSerializable^>^ GetValues( );

        /// <summary>
        /// Gets the entries in this region.
        /// </summary>
        /// <param name="recursive">
        /// if true, also return all nested subregion entries
        /// </param>
        /// <returns>array of entries</returns>
        array<RegionEntry^>^ GetEntries( bool recursive );

        /// <summary>
        /// Get the size of region. For native client regions, this will give
        /// the number of entries in the local cache and not on the servers.
        /// </summary>
        /// <returns>number of entries in the region</returns>
        property uint32_t Size
        {
          uint32_t get();
        }

        /// <summary>
        /// Gets the cache for this region.
        /// </summary>
        /// <returns>region's cache</returns>
        /// <deprecated>as of NativeClient 3.5 </deprecated>
        property Cache^ Cache
        {
          GemStone::GemFire::Cache::Cache^ get( );
        }

        /// <summary>
        /// Gets the RegionService for this region.
        /// </summary>
        /// <returns>RegionService</returns>
        property IRegionService^ RegionService
        {
          GemStone::GemFire::Cache::IRegionService^ get( );
        }

        /// <summary>
        /// True if this region has been destroyed.
        /// </summary>
        /// <returns>true if destroyed</returns>
        property bool IsDestroyed
        {
          bool get( );
        }

        /// <summary>
        /// True if the region contains a value for the given key.
        /// This only searches in the local cache.
        /// </summary>
        /// <param name="key">key to search for</param>
        /// <returns>true if value is not null</returns>
        bool ContainsValueForKey( GemStone::GemFire::Cache::ICacheableKey^ key );

        /// <summary>
        /// True if the region contains a value for the given key.
        /// This only searches in the local cache.
        /// </summary>
        /// <param name="key">key to search for</param>
        /// <returns>true if value is not null</returns>
        bool ContainsValueForKey( CacheableKey^ key );

        /// <summary>
        /// True if the region contains the given key.
        /// This only searches in the local cache.
        /// </summary>
        /// <param name="key">key to search for</param>
        /// <returns>true if contained</returns>
        bool ContainsKey( GemStone::GemFire::Cache::ICacheableKey^ key );

        /// <summary>
        /// True if the region contains the given key.
        /// This only searches in the local cache.
        /// </summary>
        /// <param name="key">key to search for</param>
        /// <returns>true if contained</returns>
        bool ContainsKey( CacheableKey^ key );

        /// <summary>
        /// Registers an array of keys for getting updates from the server.
        /// Valid only for a Native Client region when client notification
        /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
        /// </summary>
        /// <param name="keys">the array of keys</param>
        /// <exception cref="IllegalArgumentException">
        /// If the array of keys is empty.
        /// </exception>
        /// <exception cref="IllegalStateException">
        /// If already registered interest for all keys.
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// If region destroy is pending.
        /// </exception>
        /// <exception cref="UnsupportedOperationException">
        /// If the region is not a Native Client region or
        /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="UnknownException">For other exceptions.</exception>
        inline void RegisterKeys( array<GemStone::GemFire::Cache::ICacheableKey^>^ keys )
        {
          RegisterKeys(keys, false, false);
        }

        /// <summary>
        /// Registers an array of keys for getting updates from the server.
        /// Valid only for a Native Client region when client notification
        /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
        /// Should only be called for durable clients and with cache server version 5.5 onwards.
        /// </summary>
        /// <param name="keys">the array of keys</param>
        /// <param name="isDurable">whether the registration should be durable</param>
        /// <param name="getInitialValues">
        /// true to populate the cache with values of the keys
        /// that were registered on the server
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// If the array of keys is empty.
        /// </exception>
        /// <exception cref="IllegalStateException">
        /// If already registered interest for all keys.
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// If region destroy is pending.
        /// </exception>
        /// <exception cref="UnsupportedOperationException">
        /// If the region is not a Native Client region or
        /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="UnknownException">For other exceptions.</exception>
        inline void RegisterKeys(array<GemStone::GemFire::Cache::ICacheableKey^>^ keys,
          bool isDurable, bool getInitialValues)
        {
          RegisterKeys(keys, isDurable, getInitialValues, true);
        }

        /// <summary>
        /// Registers an array of keys for getting updates from the server.
        /// Valid only for a Native Client region when client notification
        /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
        /// Should only be called for durable clients and with cache server version 5.5 onwards.
        /// </summary>
        /// <param name="keys">the array of keys</param>
        /// <param name="isDurable">whether the registration should be durable</param>
        /// <param name="getInitialValues">
        /// true to populate the cache with values of the keys
        /// that were registered on the server
        /// </param>
        /// <param name="receiveValues">
        /// whether to act like notify-by-subscription is true
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// If the array of keys is empty.
        /// </exception>
        /// <exception cref="IllegalStateException">
        /// If already registered interest for all keys.
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// If region destroy is pending.
        /// </exception>
        /// <exception cref="UnsupportedOperationException">
        /// If the region is not a Native Client region or
        /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="UnknownException">For other exceptions.</exception>
        void RegisterKeys(array<GemStone::GemFire::Cache::ICacheableKey^>^ keys,
          bool isDurable, bool getInitialValues, bool receiveValues);

        /// <summary>
        /// Registers an array of keys for getting updates from the server.
        /// Valid only for a Native Client region when client notification
        /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
        /// </summary>
        /// <param name="keys">the array of keys</param>
        /// <exception cref="IllegalArgumentException">
        /// If the array of keys is empty.
        /// </exception>
        /// <exception cref="IllegalStateException">
        /// If already registered interest for all keys.
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// If region destroy is pending.
        /// </exception>
        /// <exception cref="UnsupportedOperationException">
        /// If the region is not a Native Client region or
        /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="UnknownException">For other exceptions.</exception>
        inline void RegisterKeys( array<CacheableKey^>^ keys )
        {
          RegisterKeys(keys, false, false);
        }

        /// <summary>
        /// Registers an array of keys for getting updates from the server.
        /// Valid only for a Native Client region when client notification
        /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
        /// Should only be called for durable clients and with cache server version 5.5 onwards.
        /// </summary>
        /// <param name="keys">the array of keys</param>
        /// <param name="isDurable">whether the registration should be durable</param>
        /// <param name="getInitialValues">
        /// true to populate the cache with values of the keys
        /// that were registered on the server
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// If the array of keys is empty.
        /// </exception>
        /// <exception cref="IllegalStateException">
        /// If already registered interest for all keys.
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// If region destroy is pending.
        /// </exception>
        /// <exception cref="UnsupportedOperationException">
        /// If the region is not a Native Client region or
        /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="UnknownException">For other exceptions.</exception>
        inline void RegisterKeys(array<CacheableKey^>^ keys,
          bool isDurable, bool getInitialValues)
        {
          RegisterKeys(keys, isDurable, getInitialValues, true);
        }

        /// <summary>
        /// Registers an array of keys for getting updates from the server.
        /// Valid only for a Native Client region when client notification
        /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
        /// Should only be called for durable clients and with cache server version 5.5 onwards.
        /// </summary>
        /// <param name="keys">the array of keys</param>
        /// <param name="isDurable">whether the registration should be durable</param>
        /// <param name="getInitialValues">
        /// true to populate the cache with values of the keys
        /// that were registered on the server
        /// </param>
        /// <param name="receiveValues">
        /// whether to act like notify-by-subscription is true
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// If the array of keys is empty.
        /// </exception>
        /// <exception cref="IllegalStateException">
        /// If already registered interest for all keys.
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// If region destroy is pending.
        /// </exception>
        /// <exception cref="UnsupportedOperationException">
        /// If the region is not a Native Client region or
        /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="UnknownException">For other exceptions.</exception>
        void RegisterKeys(array<CacheableKey^>^ keys,
          bool isDurable, bool getInitialValues, bool receiveValues);

        /// <summary>
        /// Unregisters an array of keys to stop getting updates for them.
        /// Valid only for a Native Client region when client notification
        /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
        /// </summary>
        /// <param name="keys">the array of keys</param>
        /// <exception cref="IllegalArgumentException">
        /// If the array of keys is empty.
        /// </exception>
        /// <exception cref="IllegalStateException">
        /// If no keys were previously registered.
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// If region destroy is pending.
        /// </exception>
        /// <exception cref="UnsupportedOperationException">
        /// If the region is not a Native Client region or
        /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="UnknownException">For other exceptions.</exception>
        void UnregisterKeys(array<GemStone::GemFire::Cache::ICacheableKey^>^ keys);

        /// <summary>
        /// Unregisters an array of keys to stop getting updates for them.
        /// Valid only for a Native Client region when client notification
        /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
        /// </summary>
        /// <param name="keys">the array of keys</param>
        /// <exception cref="IllegalArgumentException">
        /// If the array of keys is empty.
        /// </exception>
        /// <exception cref="IllegalStateException">
        /// If no keys were previously registered.
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// If region destroy is pending.
        /// </exception>
        /// <exception cref="UnsupportedOperationException">
        /// If the region is not a Native Client region or
        /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="UnknownException">For other exceptions.</exception>
        void UnregisterKeys(array<CacheableKey^>^ keys);

        /// <summary>
        /// Register interest for all the keys of the region to get
        /// updates from the server.
        /// Valid only for a Native Client region when client notification
        /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
        /// </summary>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// If region destroy is pending.
        /// </exception>
        /// <exception cref="UnsupportedOperationException">
        /// If the region is not a Native Client region or
        /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="UnknownException">For other exceptions.</exception>
        inline void RegisterAllKeys( )
        {
          RegisterAllKeys( false, nullptr, false );
        }

	/// <summary>
        /// get the interest list on this client
        /// </summary>
        array<GemStone::GemFire::Cache::ICacheableKey^>^ GetInterestList();

	/// <summary>
        /// get the list of interest regular expressions on this client
        /// </summary>
        array<String^>^ GetInterestListRegex();

	/// <summary>
        /// remove all entries in the local region
	// and propagate the operation to server
        /// </summary>
        void Clear(IGFSerializable^ callback);

	/// <summary>
        /// remove all entries in the local region
        /// </summary>
        void LocalClear(IGFSerializable^ callback);

	/// <summary>
        /// remove all entries in the local region
	// and propagate the operation to server
        /// </summary>
        inline void Clear()
	{
           Clear(nullptr);
	}

	/// <summary>
        /// remove all entries in the local region
        /// </summary>
        inline void LocalClear()
	{
           LocalClear(nullptr);
	}

        /// <summary>
        /// Register interest for all the keys of the region to get
        /// updates from the server.
        /// Valid only for a Native Client region when client notification
        /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
        /// Should only be called for durable clients and with cache server version 5.5 onwards.
        /// </summary>
        /// <param name="isDurable">whether the registration should be durable</param>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// If region destroy is pending.
        /// </exception>
        /// <exception cref="UnsupportedOperationException">
        /// If the region is not a Native Client region or
        /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="UnknownException">For other exceptions.</exception>
        inline void RegisterAllKeys( bool isDurable )
        {
          RegisterAllKeys( isDurable, nullptr, false );
        }

        /// <summary>
        /// Register interest for all the keys of the region to get
        /// updates from the server.
        /// Valid only for a Native Client region when client notification
        /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
        /// Should only be called for durable clients and with cache server version 5.5 onwards.
        /// </summary>
        /// <param name="isDurable">whether the registration should be durable</param>
        /// <param name="resultKeys">
        /// if non-null then all keys on the server are returned
        /// </param>
        /// <param name="getInitialValues">
        /// true to populate the cache with values of all the keys
        /// from the server
        /// </param>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// If region destroy is pending.
        /// </exception>
        /// <exception cref="UnsupportedOperationException">
        /// If the region is not a Native Client region or
        /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="UnknownException">For other exceptions.</exception>
        inline void RegisterAllKeys(bool isDurable,
            List<GemStone::GemFire::Cache::ICacheableKey^>^ resultKeys, bool getInitialValues)
        {
          RegisterAllKeys(isDurable, resultKeys, getInitialValues, true);
        }

        /// <summary>
        /// Register interest for all the keys of the region to get
        /// updates from the server.
        /// Valid only for a Native Client region when client notification
        /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
        /// Should only be called for durable clients and with cache server version 5.5 onwards.
        /// </summary>
        /// <param name="isDurable">whether the registration should be durable</param>
        /// <param name="resultKeys">
        /// if non-null then all keys on the server are returned
        /// </param>
        /// <param name="getInitialValues">
        /// true to populate the cache with values of all the keys
        /// from the server
        /// </param>
        /// <param name="receiveValues">
        /// whether to act like notify-by-subscription is true
        /// </param>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// If region destroy is pending.
        /// </exception>
        /// <exception cref="UnsupportedOperationException">
        /// If the region is not a Native Client region or
        /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="UnknownException">For other exceptions.</exception>
        void RegisterAllKeys(bool isDurable,
            List<GemStone::GemFire::Cache::ICacheableKey^>^ resultKeys, bool getInitialValues, bool receiveValues);

        /// <summary>
        /// Unregister interest for all the keys of the region to stop
        /// getting updates for them.
        /// Valid only for a Native Client region when client notification
        /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
        /// </summary>
        /// <exception cref="IllegalStateException">
        /// If not previously registered all keys.
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// If region destroy is pending.
        /// </exception>
        /// <exception cref="UnsupportedOperationException">
        /// If the region is not a Native Client region or
        /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="UnknownException">For other exceptions.</exception>
        void UnregisterAllKeys( );

        /// <summary>
        /// Register interest for the keys of the region that match the
        /// given regular expression to get updates from the server.
        /// Valid only for a Native Client region when client notification
        /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
        /// </summary>
        /// <exception cref="IllegalArgumentException">
        /// If the regular expression string is empty.
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="MessageException">
        /// If the message received from server could not be handled. This will
        /// be the case when an unregistered typeId is received in the reply or
        /// reply is not well formed. More information can be found in the log.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// If region destroy is pending.
        /// </exception>
        /// <exception cref="UnsupportedOperationException">
        /// If the region is not a Native Client region or
        /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="UnknownException">For other exceptions.</exception>
        inline void RegisterRegex( String^ regex )
        {
          RegisterRegex(regex, false, nullptr, false);
        }

        /// <summary>
        /// Register interest for the keys of the region that match the
        /// given regular expression to get updates from the server.
        /// Valid only for a Native Client region when client notification
        /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
        /// Should only be called for durable clients and with cache server version 5.5 onwards.
        /// </summary>
        /// <param name="regex">the regular expression to register</param>
        /// <param name="isDurable">whether the registration should be durable</param>
        /// <exception cref="IllegalArgumentException">
        /// If the regular expression string is empty.
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="MessageException">
        /// If the message received from server could not be handled. This will
        /// be the case when an unregistered typeId is received in the reply or
        /// reply is not well formed. More information can be found in the log.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// If region destroy is pending.
        /// </exception>
        /// <exception cref="UnsupportedOperationException">
        /// If the region is not a Native Client region or
        /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="UnknownException">For other exceptions.</exception>
        inline void RegisterRegex( String^ regex, bool isDurable )
        {
          RegisterRegex(regex, isDurable, nullptr, false);
        }

        /// <summary>
        /// Register interest for the keys of the region that match the
        /// given regular expression to get updates from the server.
        /// Valid only for a Native Client region when client notification
        /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
        /// Should only be called for durable clients and with cache server version 5.5 onwards.
        /// </summary>
        /// <param name="regex">the regular expression to register</param>
        /// <param name="isDurable">whether the registration should be durable</param>
        /// <param name="resultKeys">
        /// if non-null then the keys that match the regular expression
        /// on the server are returned
        ///</param>
        /// <exception cref="IllegalArgumentException">
        /// If the regular expression string is empty.
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="MessageException">
        /// If the message received from server could not be handled. This will
        /// be the case when an unregistered typeId is received in the reply or
        /// reply is not well formed. More information can be found in the log.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// If region destroy is pending.
        /// </exception>
        /// <exception cref="UnsupportedOperationException">
        /// If the region is not a Native Client region or
        /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="UnknownException">For other exceptions.</exception>
        inline void RegisterRegex(String^ regex, bool isDurable,
            List<GemStone::GemFire::Cache::ICacheableKey^>^ resultKeys)
        {
          RegisterRegex(regex, isDurable, resultKeys, false);
        }

        /// <summary>
        /// Register interest for the keys of the region that match the
        /// given regular expression to get updates from the server.
        /// Valid only for a Native Client region when client notification
        /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
        /// Should only be called for durable clients and with cache server version 5.5 onwards.
        /// </summary>
        /// <param name="regex">the regular expression to register</param>
        /// <param name="isDurable">whether the registration should be durable</param>
        /// <param name="resultKeys">
        /// if non-null then the keys that match the regular expression
        /// on the server are returned
        ///</param>
        /// <param name="getInitialValues">
        /// true to populate the cache with values of the keys
        /// that were registered on the server
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// If the regular expression string is empty.
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="MessageException">
        /// If the message received from server could not be handled. This will
        /// be the case when an unregistered typeId is received in the reply or
        /// reply is not well formed. More information can be found in the log.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// If region destroy is pending.
        /// </exception>
        /// <exception cref="UnsupportedOperationException">
        /// If the region is not a Native Client region or
        /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="UnknownException">For other exceptions.</exception>
        inline void RegisterRegex(String^ regex, bool isDurable,
            List<GemStone::GemFire::Cache::ICacheableKey^>^ resultKeys, bool getInitialValues)
        {
          RegisterRegex(regex, isDurable, resultKeys, getInitialValues, true);
        }

        /// <summary>
        /// Register interest for the keys of the region that match the
        /// given regular expression to get updates from the server.
        /// Valid only for a Native Client region when client notification
        /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
        /// Should only be called for durable clients and with cache server version 5.5 onwards.
        /// </summary>
        /// <param name="regex">the regular expression to register</param>
        /// <param name="isDurable">whether the registration should be durable</param>
        /// <param name="resultKeys">
        /// if non-null then the keys that match the regular expression
        /// on the server are returned
        ///</param>
        /// <param name="getInitialValues">
        /// true to populate the cache with values of the keys
        /// that were registered on the server
        /// </param>
        /// <param name="receiveValues">
        /// whether to act like notify-by-subscription is true
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// If the regular expression string is empty.
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// </exception>
        /// <exception cref="MessageException">
        /// If the message received from server could not be handled. This will
        /// be the case when an unregistered typeId is received in the reply or
        /// reply is not well formed. More information can be found in the log.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// If region destroy is pending.
        /// </exception>
        /// <exception cref="UnsupportedOperationException">
        /// If the region is not a Native Client region or
        /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="UnknownException">For other exceptions.</exception>
        void RegisterRegex(String^ regex, bool isDurable,
            List<GemStone::GemFire::Cache::ICacheableKey^>^ resultKeys, bool getInitialValues, bool receiveValues);

        /// <summary>
        /// Unregister interest for the keys of the region that match the
        /// given regular expression to stop getting updates for them.
        /// The regular expression must have been registered previously using
        /// a <c>RegisterRegex</c> call.
        /// Valid only for a Native Client region when client notification
        /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
        /// </summary>
        /// <exception cref="IllegalArgumentException">
        /// If the regular expression string is empty.
        /// </exception>
        /// <exception cref="IllegalStateException">
        /// If this regular expression has not been registered by a previous
        /// call to <c>RegisterRegex</c>.
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// If region destroy is pending.
        /// </exception>
        /// <exception cref="UnsupportedOperationException">
        /// If the region is not a Native Client region or
        /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="UnknownException">For other exceptions.</exception>
        void UnregisterRegex( String^ regex );

        /// <summary>
        /// Gets values for an array of keys from the local cache or server.
        /// If value for a key is not present locally then it is requested from the
        /// java server. The value returned is not copied, so multi-threaded
        /// applications should not modify the value directly,
        /// but should use the update methods.
        ///
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" />
        /// and <see cref="CacheStatistics.HitCount" /> and
        /// <see cref="CacheStatistics.MissCount" /> for this region and the entry.
        /// </summary>
        /// <param name="keys">the array of keys</param>
        /// <param name="values">
        /// output parameter that provides the map of keys to
        /// respective values; ignored if NULL; when this is NULL then at least
        /// the <c>addToLocalCache</c> parameter should be true and caching
        /// should be enabled for the region to get values into the region
        /// otherwise an <c>IllegalArgumentException</c> is thrown.
        /// </param>
        /// <param name="exceptions">
        /// output parameter that provides the map of keys
        /// to any exceptions while obtaining the key; ignored if this is NULL
        /// </param>
        /// <param name="addToLocalCache">
        /// true if the obtained values have also to be added to the local cache
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// If the array of keys is null or empty. Other invalid case is when
        /// the <c>values</c> parameter is NULL, and either
        /// <c>addToLocalCache</c> is false or caching is disabled
        /// for this region.
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server while
        /// processing the request.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if region is not connected to the cache because the client
        /// cannot establish usable connections to any of the given servers
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// If region destroy is pending.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if operation timed out.
        /// </exception>
        /// <exception cref="UnknownException">
        /// For other exceptions.
        /// </exception>
        /// <seealso cref="Get"/>
        void GetAll(array<GemStone::GemFire::Cache::ICacheableKey^>^ keys,
          Dictionary<GemStone::GemFire::Cache::ICacheableKey^, IGFSerializable^>^ values,
          Dictionary<GemStone::GemFire::Cache::ICacheableKey^, System::Exception^>^ exceptions,
          bool addToLocalCache);

        /// <summary>
        /// Gets values for an array of keys from the local cache or server.
        /// If value for a key is not present locally then it is requested from the
        /// java server. The value returned is not copied, so multi-threaded
        /// applications should not modify the value directly,
        /// but should use the update methods.
        ///
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" />
        /// and <see cref="CacheStatistics.HitCount" /> and
        /// <see cref="CacheStatistics.MissCount" /> for this region and the entry.
        /// </summary>
        /// <param name="keys">the array of keys</param>
        /// <param name="values">
        /// output parameter that provides the map of keys to
        /// respective values; ignored if NULL; when this is NULL then at least
        /// the <c>addToLocalCache</c> parameter should be true and caching
        /// should be enabled for the region to get values into the region
        /// otherwise an <c>IllegalArgumentException</c> is thrown.
        /// </param>
        /// <param name="exceptions">
        /// output parameter that provides the map of keys
        /// to any exceptions while obtaining the key; ignored if this is NULL
        /// </param>
        /// <param name="addToLocalCache">
        /// true if the obtained values have also to be added to the local cache
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// If the array of keys is null or empty. Other invalid case is when
        /// the <c>values</c> parameter is NULL, and either
        /// <c>addToLocalCache</c> is false or caching is disabled
        /// for this region.
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server while
        /// processing the request.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if region is not connected to the cache because the client
        /// cannot establish usable connections to any of the given servers
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// If region destroy is pending.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if operation timed out.
        /// </exception>
        /// <exception cref="UnknownException">
        /// For other exceptions.
        /// </exception>
        /// <seealso cref="Get"/>
        void GetAll(array<CacheableKey^>^ keys,
          Dictionary<GemStone::GemFire::Cache::ICacheableKey^, IGFSerializable^>^ values,
          Dictionary<GemStone::GemFire::Cache::ICacheableKey^, System::Exception^>^ exceptions,
          bool addToLocalCache);

        /// <summary>
        /// Gets values for an array of keys from the local cache or server.
        /// If value for a key is not present locally then it is requested from the
        /// java server. The value returned is not copied, so multi-threaded
        /// applications should not modify the value directly,
        /// but should use the update methods.
        ///
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" />
        /// and <see cref="CacheStatistics.HitCount" /> and
        /// <see cref="CacheStatistics.MissCount" /> for this region and the entry.
        /// </summary>
        /// <param name="keys">the array of keys</param>
        /// <param name="values">
        /// output parameter that provides the map of keys to
        /// respective values; when this is NULL then an
        /// <c>IllegalArgumentException</c> is thrown.
        /// </param>
        /// <param name="exceptions">
        /// output parameter that provides the map of keys
        /// to any exceptions while obtaining the key; ignored if this is NULL
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// If the array of keys is null or empty,
        /// or <c>values</c> argument is null.
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server while
        /// processing the request.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if region is not connected to the cache because the client
        /// cannot establish usable connections to any of the given servers
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// If region destroy is pending.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if operation timed out.
        /// </exception>
        /// <exception cref="UnknownException">
        /// For other exceptions.
        /// </exception>
        /// <seealso cref="Get"/>
        inline void GetAll(array<GemStone::GemFire::Cache::ICacheableKey^>^ keys,
          Dictionary<GemStone::GemFire::Cache::ICacheableKey^, IGFSerializable^>^ values,
          Dictionary<GemStone::GemFire::Cache::ICacheableKey^, System::Exception^>^ exceptions)
        {
          GetAll(keys, values, exceptions, false);
        }

        /// <summary>
        /// Gets values for an array of keys from the local cache or server.
        /// If value for a key is not present locally then it is requested from the
        /// java server. The value returned is not copied, so multi-threaded
        /// applications should not modify the value directly,
        /// but should use the update methods.
        ///
        /// Updates the <see cref="CacheStatistics.LastAccessedTime" />
        /// and <see cref="CacheStatistics.HitCount" /> and
        /// <see cref="CacheStatistics.MissCount" /> for this region and the entry.
        /// </summary>
        /// <param name="keys">the array of keys</param>
        /// <param name="values">
        /// output parameter that provides the map of keys to
        /// respective values; when this is NULL then an
        /// <c>IllegalArgumentException</c> is thrown.
        /// </param>
        /// <param name="exceptions">
        /// output parameter that provides the map of keys
        /// to any exceptions while obtaining the key; ignored if this is NULL
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// If the array of keys is null or empty,
        /// or <c>values</c> argument is null.
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server while
        /// processing the request.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if region is not connected to the cache because the client
        /// cannot establish usable connections to any of the given servers
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// If region destroy is pending.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if operation timed out.
        /// </exception>
        /// <exception cref="UnknownException">
        /// For other exceptions.
        /// </exception>
        /// <seealso cref="Get"/>
        inline void GetAll(array<CacheableKey^>^ keys,
          Dictionary<GemStone::GemFire::Cache::ICacheableKey^, IGFSerializable^>^ values,
          Dictionary<GemStone::GemFire::Cache::ICacheableKey^, System::Exception^>^ exceptions)
        {
          GetAll(keys, values, exceptions, false);
        }

        /// <summary>
        /// Executes the query on the server based on the predicate.
        /// Valid only for a Native Client region.
        /// </summary>
        /// <param name="predicate">The query predicate (just the WHERE clause) or the entire query to execute</param>
        /// <param name="timeout">The time (in seconds) to wait for the query response, optional</param>
        /// <exception cref="IllegalArgumentException">
        /// If the predicate is empty.
        /// </exception>
        /// <exception cref="IllegalStateException">
        /// If some error occurred.
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="MessageException">
        /// If the message received from server could not be handled. This will
        /// be the case when an unregistered typeId is received in the reply or
        /// reply is not well formed. More information can be found in the log.
        /// </exception>
        /// <exception cref="QueryException">
        /// If some query error occurred at the server.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="CacheClosedException">
        /// if the cache has been closed
        /// </exception>
        /// <returns>
        /// The SelectResults which can either be a ResultSet or a StructSet.
        /// </returns>
        ISelectResults^ Query( String^ predicate, uint32_t timeout );

        /// <summary>
        /// Executes the query on the server based on the predicate.
        /// Valid only for a Native Client region.
        /// </summary>
        /// <param name="predicate">The query predicate (just the WHERE clause) or the entire query to execute</param>
        /// <exception cref="IllegalArgumentException">
        /// If the predicate is empty.
        /// </exception>
        /// <exception cref="IllegalStateException">
        /// If some error occurred.
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="MessageException">
        /// If the message received from server could not be handled. This will
        /// be the case when an unregistered typeId is received in the reply or
        /// reply is not well formed. More information can be found in the log.
        /// </exception>
        /// <exception cref="QueryException">
        /// If some query error occurred at the server.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="CacheClosedException">
        /// if the cache has been closed
        /// </exception>
        /// <returns>
        /// The SelectResults which can either be a ResultSet or a StructSet.
        /// </returns>
        ISelectResults^ Query( String^ predicate );

        /// <summary>
        /// Executes the query on the server based on the predicate
        /// and returns whether any result exists.
        /// Valid only for a  Native Client region.
        /// </summary>
        /// <param name="predicate">
        /// The query predicate (just the WHERE clause)
        /// or the entire query to execute
        /// </param>
        /// <param name="timeout">
        /// The time (in seconds) to wait for the query response
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// If the predicate is empty.
        /// </exception>
        /// <exception cref="IllegalStateException">
        /// If some error occurred.
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="MessageException">
        /// If the message received from server could not be handled. This will
        /// be the case when an unregistered typeId is received in the reply or
        /// reply is not well formed. More information can be found in the log.
        /// </exception>
        /// <exception cref="QueryException">
        /// If some query error occurred at the server.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="CacheClosedException">
        /// if the cache has been closed
        /// </exception>
        /// <returns>
        /// true if the result size is non-zero, false otherwise.
        /// </returns>
        bool ExistsValue( String^ predicate, uint32_t timeout );

        /// <summary>
        /// Executes the query on the server based on the predicate
        /// and returns whether any result exists.
        /// Valid only for a Native Client region.
        /// </summary>
        /// <param name="predicate">
        /// The query predicate (just the WHERE clause)
        /// or the entire query to execute
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// If the predicate is empty.
        /// </exception>
        /// <exception cref="IllegalStateException">
        /// If some error occurred.
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="MessageException">
        /// If the message received from server could not be handled. This will
        /// be the case when an unregistered typeId is received in the reply or
        /// reply is not well formed. More information can be found in the log.
        /// </exception>
        /// <exception cref="QueryException">
        /// If some query error occurred at the server.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="CacheClosedException">
        /// if the cache has been closed
        /// </exception>
        /// <returns>
        /// true if the result size is non-zero, false otherwise.
        /// </returns>
        bool ExistsValue( String^ predicate );

        /// <summary>
        /// Executes the query on the server based on the predicate
        /// and returns a single result value.
        /// Valid only for a Native Client region.
        /// </summary>
        /// <param name="predicate">
        /// The query predicate (just the WHERE clause)
        /// or the entire query to execute
        /// </param>
        /// <param name="timeout">
        /// The time (in seconds) to wait for the query response
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// If the predicate is empty.
        /// </exception>
        /// <exception cref="IllegalStateException">
        /// If some error occurred.
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="MessageException">
        /// If the message received from server could not be handled. This will
        /// be the case when an unregistered typeId is received in the reply or
        /// reply is not well formed. More information can be found in the log.
        /// </exception>
        /// <exception cref="QueryException">
        /// If some query error occurred at the server,
        /// or more than one result items are available.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="CacheClosedException">
        /// if the cache has been closed
        /// </exception>
        /// <returns>
        /// The single ResultSet or StructSet item,
        /// or NULL of no results are available.
        /// </returns>
        IGFSerializable^ SelectValue( String^ predicate, uint32_t timeout );

        /// <summary>
        /// Executes the query on the server based on the predicate
        /// and returns a single result value.
        /// Valid only for a Native Client region.
        /// </summary>
        /// <param name="predicate">
        /// The query predicate (just the WHERE clause)
        /// or the entire query to execute
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// If the predicate is empty.
        /// </exception>
        /// <exception cref="IllegalStateException">
        /// If some error occurred.
        /// </exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="MessageException">
        /// If the message received from server could not be handled. This will
        /// be the case when an unregistered typeId is received in the reply or
        /// reply is not well formed. More information can be found in the log.
        /// </exception>
        /// <exception cref="QueryException">
        /// If some query error occurred at the server,
        /// or more than one result items are available.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="CacheClosedException">
        /// if the cache has been closed
        /// </exception>
        /// <returns>
        /// The single ResultSet or StructSet item,
        /// or NULL of no results are available.
        /// </returns>
        IGFSerializable^ SelectValue( String^ predicate );

        //--------------------------------Remove Api--------------------------------------------

        /// <summary>
        /// Removes the entry with the specified key and value, passing the callback
        /// argument to any cache writers that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Remove removes not only the value, but also the key and entry
        /// from this region.
        /// </para>
        /// <para>
        /// The Remove is propogated to the Gemfire cache server to which it is connected with.
        /// </para>
        /// <para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">the key of the entry to Remove</param>
        /// <param name="value">the value of the entry to Remove</param>
        /// <param name="callbackArg"> the callback for user to pass in, It can also be null.
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if this region has been destroyed
        /// </exception>
        /// <returns>true if entry with key and its value are removed otherwise false.</returns>
        /// <seealso cref="Destroy" />
        /// <seealso cref="Invalidate" />
        /// <seealso cref="ICacheListener.AfterDestroy" />
        /// <seealso cref="ICacheWriter.BeforeDestroy" />
        bool Remove( GemStone::GemFire::Cache::ICacheableKey^ key, IGFSerializable^ value,
          IGFSerializable^ callbackArg );

        /// <summary>
        /// Removes the entry with the specified key and value, passing the callback
        /// argument to any cache writers that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Remove removes not only the value, but also the key and entry
        /// from this region.
        /// </para>
        /// <para>
        /// The Remove is propogated to the Gemfire cache server to which it is connected with.
        /// </para>
        /// <para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">the key of the entry to Remove</param>
        /// <param name="value">the value of the entry to Remove</param>
        /// <param name="callbackArg"> the callback for user to pass in, It can also be null.
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if this region has been destroyed
        /// </exception>
        /// <returns>true if entry with key and its value are removed otherwise false.</returns>
        /// <seealso cref="Destroy" />
        /// <seealso cref="Invalidate" />
        /// <seealso cref="ICacheListener.AfterDestroy" />
        /// <seealso cref="ICacheWriter.BeforeDestroy" />
        bool Remove( CacheableKey^ key, IGFSerializable^ value,
          IGFSerializable^ callbackArg );

        /// <summary>
        /// Removes the entry with the specified key and value, passing the callback
        /// argument to any cache writers that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Remove removes not only the value, but also the key and entry
        /// from this region.
        /// </para>
        /// <para>
        /// The Remove is propogated to the Gemfire cache server to which it is connected with.
        /// </para>
        /// <para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">the key of the entry to Remove</param>
        /// <param name="value">the value of the entry to Remove</param>
        /// <param name="callbackArg"> the callback for user to pass in, It can also be null.
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if this region has been destroyed
        /// </exception>
        /// <returns>true if entry with key and its value are removed otherwise false.</returns>
        /// <seealso cref="Destroy" />
        /// <seealso cref="Invalidate" />
        /// <seealso cref="ICacheListener.AfterDestroy" />
        /// <seealso cref="ICacheWriter.BeforeDestroy" />
        bool Remove( GemStone::GemFire::Cache::ICacheableKey^ key, GemStone::GemFire::Cache::Serializable^ value,
          IGFSerializable^ callbackArg );

        /// <summary>
        /// Removes the entry with the specified key and value, passing the callback
        /// argument to any cache writers that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Remove removes not only the value, but also the key and entry
        /// from this region.
        /// </para>
        /// <para>
        /// The Remove is propogated to the Gemfire cache server to which it is connected with.
        /// </para>
        /// <para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">the key of the entry to Remove</param>
        /// <param name="value">the value of the entry to Remove</param>
        /// <param name="callbackArg"> the callback for user to pass in, It can also be null.
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if this region has been destroyed
        /// </exception>
        /// <returns>true if entry with key and its value are removed otherwise false.</returns>
        /// <seealso cref="Destroy" />
        /// <seealso cref="Invalidate" />
        /// <seealso cref="ICacheListener.AfterDestroy" />
        /// <seealso cref="ICacheWriter.BeforeDestroy" />
        bool Remove( CacheableKey^ key, GemStone::GemFire::Cache::Serializable^ value,
          IGFSerializable^ callbackArg );

        /// <summary>
        /// Removes the entry with the specified key and value, passing the callback
        /// argument to any cache writers that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Remove removes not only the value, but also the key and entry
        /// from this region.
        /// </para>
        /// <para>
        /// The Remove is propogated to the Gemfire cache server to which it is connected with.
        /// </para>
        /// <para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">the key of the entry to Remove</param>
        /// <param name="value">the value of the entry to Remove</param>          
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if this region has been destroyed
        /// </exception>
        /// <returns>true if entry with key and its value are removed otherwise false.</returns>
        /// <seealso cref="Destroy" />
        /// <seealso cref="Invalidate" />
        /// <seealso cref="ICacheListener.AfterDestroy" />
        /// <seealso cref="ICacheWriter.BeforeDestroy" />
        inline bool Remove( GemStone::GemFire::Cache::ICacheableKey^ key, IGFSerializable^ value )
        {
          return Remove( key, value, nullptr );
        }

        /// <summary>
        /// Removes the entry with the specified key and value, passing the callback
        /// argument to any cache writers that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Remove removes not only the value, but also the key and entry
        /// from this region.
        /// </para>
        /// <para>
        /// The Remove is propogated to the Gemfire cache server to which it is connected with.
        /// </para>
        /// <para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">the key of the entry to Remove</param>
        /// <param name="value">the value of the entry to Remove</param>          
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if this region has been destroyed
        /// </exception>
        /// <returns>true if entry with key and its value are removed otherwise false.</returns>
        /// <seealso cref="Destroy" />
        /// <seealso cref="Invalidate" />
        /// <seealso cref="ICacheListener.AfterDestroy" />
        /// <seealso cref="ICacheWriter.BeforeDestroy" />
        inline bool Remove( CacheableKey^ key, IGFSerializable^ value )
        {
          return Remove( key, value, nullptr );
        }

        /// <summary>
        /// Removes the entry with the specified key and value, passing the callback
        /// argument to any cache writers that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Remove removes not only the value, but also the key and entry
        /// from this region.
        /// </para>
        /// <para>
        /// The Remove is propogated to the Gemfire cache server to which it is connected with.
        /// </para>
        /// <para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">the key of the entry to Remove</param>
        /// <param name="value">the value of the entry to Remove</param>          
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if this region has been destroyed
        /// </exception>
        /// <returns>true if entry with key and its value are removed otherwise false.</returns>
        /// <seealso cref="Destroy" />
        /// <seealso cref="Invalidate" />
        /// <seealso cref="ICacheListener.AfterDestroy" />
        /// <seealso cref="ICacheWriter.BeforeDestroy" />
        inline bool Remove( GemStone::GemFire::Cache::ICacheableKey^ key, GemStone::GemFire::Cache::Serializable^ value )
        {
          return Remove( key, value, nullptr );
        }

        /// <summary>
        /// Removes the entry with the specified key and value, passing the callback
        /// argument to any cache writers that are invoked in the operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Remove removes not only the value, but also the key and entry
        /// from this region.
        /// </para>
        /// <para>
        /// The Remove is propogated to the Gemfire cache server to which it is connected with.
        /// </para>
        /// <para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">the key of the entry to Remove</param>
        /// <param name="value">the value of the entry to Remove</param>          
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="CacheServerException">
        /// If an exception is received from the Java cache server.
        /// Only for Native Client regions.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if not connected to the GemFire system because the client cannot
        /// establish usable connections to any of the servers given to it.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// if the operation timed out
        /// </exception>
        /// <exception cref="RegionDestroyedException">
        /// if this region has been destroyed
        /// </exception>
        /// <returns>true if entry with key and its value are removed otherwise false.</returns>
        /// <seealso cref="Destroy" />
        /// <seealso cref="Invalidate" />
        /// <seealso cref="ICacheListener.AfterDestroy" />
        /// <seealso cref="ICacheWriter.BeforeDestroy" />
        inline bool Remove( CacheableKey^ key, GemStone::GemFire::Cache::Serializable^ value )
        {
          return Remove( key, value, nullptr );
        }

        /// <summary>
        /// Removes the value with the specified key in the local cache only.
        /// Remove removes not only the value but also the key and entry
        /// from this region.
        /// </summary>
        /// <remarks>
        /// <para>
        /// No <c>ICacheWriter</c> is invoked.
        /// </para><para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">the key of the entry to remove</param>
        /// <param name="value"> the value of the key to remove</param>
        /// <param name="callbackArg">
        /// a user-defined parameter to pass to cache listeners and writers triggered by this method
        /// </param>
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="IllegalStateException">
        /// if this region has mirroring enabled
        /// </exception>
        /// <returns>true if entry with key and its value are removed otherwise false.</returns>
        /// <seealso cref="Destroy" />
        /// <seealso cref="LocalInvalidate" />
        /// <seealso cref="ICacheListener.AfterDestroy" />
        /// <seealso cref="ICacheWriter.BeforeDestroy" />
        bool LocalRemove(GemStone::GemFire::Cache::ICacheableKey^ key, IGFSerializable^ value,
          IGFSerializable^ callbackArg);

        /// <summary>
        /// Removes the value with the specified key in the local cache only.
        /// Remove removes not only the value but also the key and entry
        /// from this region.
        /// </summary>
        /// <remarks>
        /// <para>
        /// No <c>ICacheWriter</c> is invoked.
        /// </para><para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">the key of the entry to remove</param>
        /// <param name="value"> the value of the key to remove</param>
        /// <param name="callbackArg">
        /// a user-defined parameter to pass to cache listeners and writers triggered by this method
        /// </param>
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="IllegalStateException">
        /// if this region has mirroring enabled
        /// </exception>
        /// <returns>true if entry with key and its value are removed otherwise false.</returns>
        /// <seealso cref="Destroy" />
        /// <seealso cref="LocalInvalidate" />
        /// <seealso cref="ICacheListener.AfterDestroy" />
        /// <seealso cref="ICacheWriter.BeforeDestroy" />
        bool LocalRemove(GemStone::GemFire::Cache::ICacheableKey^ key, GemStone::GemFire::Cache::Serializable^ value,
          IGFSerializable^ callbackArg);

        /// <summary>
        /// Removes the value with the specified key in the local cache only.
        /// Remove removes not only the value but also the key and entry
        /// from this region.
        /// </summary>
        /// <remarks>
        /// <para>
        /// No <c>ICacheWriter</c> is invoked.
        /// </para><para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">the key of the entry to remove</param>
        /// <param name="value"> the value of the key to remove</param>
        /// <param name="callbackArg">
        /// a user-defined parameter to pass to cache listeners and writers triggered by this method
        /// </param>
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="IllegalStateException">
        /// if this region has mirroring enabled
        /// </exception>
        /// <returns>true if entry with key and its value are removed otherwise false.</returns>
        /// <seealso cref="Destroy" />
        /// <seealso cref="LocalInvalidate" />
        /// <seealso cref="ICacheListener.AfterDestroy" />
        /// <seealso cref="ICacheWriter.BeforeDestroy" />
        bool LocalRemove(CacheableKey^ key, IGFSerializable^ value,
          IGFSerializable^ callbackArg);

        /// <summary>
        /// Removes the value with the specified key in the local cache only.
        /// Remove removes not only the value but also the key and entry
        /// from this region.
        /// </summary>
        /// <remarks>
        /// <para>
        /// No <c>ICacheWriter</c> is invoked.
        /// </para><para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">the key of the entry to remove</param>
        /// <param name="value"> the value of the key to remove</param>
        /// <param name="callbackArg">
        /// a user-defined parameter to pass to cache listeners and writers triggered by this method
        /// </param>
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="IllegalStateException">
        /// if this region has mirroring enabled
        /// </exception>
        /// <returns>true if entry with key and its value are removed otherwise false.</returns>
        /// <seealso cref="Destroy" />
        /// <seealso cref="LocalInvalidate" />
        /// <seealso cref="ICacheListener.AfterDestroy" />
        /// <seealso cref="ICacheWriter.BeforeDestroy" />
        bool LocalRemove(CacheableKey^ key, GemStone::GemFire::Cache::Serializable^ value,
          IGFSerializable^ callbackArg);

        /// <summary>
        /// Removes the value with the specified key in the local cache only.
        /// Remove removes not only the value but also the key and entry
        /// from this region.
        /// </summary>
        /// <remarks>
        /// <para>
        /// No <c>ICacheWriter</c> is invoked.
        /// </para><para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">the key of the entry to remove</param>
        /// <param name="value"> the value of the key to remove</param>
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="IllegalStateException">
        /// if this region has mirroring enabled
        /// </exception>
        /// <returns>true if entry with key and its value are removed otherwise false.</returns>
        /// <seealso cref="Destroy" />
        /// <seealso cref="LocalInvalidate" />
        /// <seealso cref="ICacheListener.AfterDestroy" />
        /// <seealso cref="ICacheWriter.BeforeDestroy" />
        inline bool LocalRemove(GemStone::GemFire::Cache::ICacheableKey^ key, IGFSerializable^ value)
        {
          return LocalRemove(key, value, nullptr);
        }

        /// <summary>
        /// Removes the value with the specified key in the local cache only.
        /// Remove removes not only the value but also the key and entry
        /// from this region.
        /// </summary>
        /// <remarks>
        /// <para>
        /// No <c>ICacheWriter</c> is invoked.
        /// </para><para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">the key of the entry to remove</param>
        /// <param name="value"> the value of the key to remove</param>
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="IllegalStateException">
        /// if this region has mirroring enabled
        /// </exception>
        /// <returns>true if entry with key and its value are removed otherwise false.</returns>
        /// <seealso cref="Destroy" />
        /// <seealso cref="LocalInvalidate" />
        /// <seealso cref="ICacheListener.AfterDestroy" />
        /// <seealso cref="ICacheWriter.BeforeDestroy" />
        inline bool LocalRemove(GemStone::GemFire::Cache::ICacheableKey^ key, GemStone::GemFire::Cache::Serializable^ value)
        {
          return LocalRemove(key, value, nullptr);
        }

        /// <summary>
        /// Removes the value with the specified key in the local cache only.
        /// Remove removes not only the value but also the key and entry
        /// from this region.
        /// </summary>
        /// <remarks>
        /// <para>
        /// No <c>ICacheWriter</c> is invoked.
        /// </para><para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">the key of the entry to remove</param>
        /// <param name="value"> the value of the key to remove</param>
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="IllegalStateException">
        /// if this region has mirroring enabled
        /// </exception>
        /// <returns>true if entry with key and its value are removed otherwise false.</returns>
        /// <seealso cref="Destroy" />
        /// <seealso cref="LocalInvalidate" />
        /// <seealso cref="ICacheListener.AfterDestroy" />
        /// <seealso cref="ICacheWriter.BeforeDestroy" />
        inline bool LocalRemove(CacheableKey^ key, IGFSerializable^ value)
        {
          return LocalRemove(key, value, nullptr);
        }

        /// <summary>
        /// Removes the value with the specified key in the local cache only.
        /// Remove removes not only the value but also the key and entry
        /// from this region.
        /// </summary>
        /// <remarks>
        /// <para>
        /// No <c>ICacheWriter</c> is invoked.
        /// </para><para>
        /// Does not update any <c>CacheStatistics</c>.
        /// </para>
        /// </remarks>
        /// <param name="key">the key of the entry to remove</param>
        /// <param name="value"> the value of the key to remove</param>
        /// <exception cref="IllegalArgumentException">if key is null</exception>
        /// <exception cref="IllegalStateException">
        /// if this region has mirroring enabled
        /// </exception>
        /// <returns>true if entry with key and its value are removed otherwise false.</returns>
        /// <seealso cref="Destroy" />
        /// <seealso cref="LocalInvalidate" />
        /// <seealso cref="ICacheListener.AfterDestroy" />
        /// <seealso cref="ICacheWriter.BeforeDestroy" />
        inline bool LocalRemove(CacheableKey^ key, GemStone::GemFire::Cache::Serializable^ value)
        {
          return LocalRemove(key, value, nullptr);
        }

      internal:
        /// <summary>
        /// Internal factory function to wrap a native object pointer inside
        /// this managed class with null pointer check.
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        /// <returns>
        /// The managed wrapper object; null if the native pointer is null.
        /// </returns>
        inline static Region^ Create( gemfire::Region* nativeptr )
        {
          return ( nativeptr != nullptr ?
            gcnew Region( nativeptr ) : nullptr );
        }


      //private:
        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline Region( gemfire::Region* nativeptr )
          : SBWrap( nativeptr ) { }
      };

    }
  }
}
