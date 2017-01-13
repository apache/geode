/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
//#include <gfcpp/Cache.hpp>
//#include "impl/NativeWrapper.hpp"
#include "RegionShortcut.hpp"
//#include "RegionFactory.hpp"
#include "IGemFireCache.hpp"
//#include "IRegionService.hpp"
#include "IRegion.hpp"
//#include "QueryService.hpp"


#include "RegionAttributes.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Generic
      {

				generic<class TKey, class TResult>
				ref class QueryService;

				ref class RegionFactory;
				enum class ExpirationAction;
      ref class DistributedSystem;
	  ref class CacheTransactionManager2PC;
      //ref class FunctionService;

      /// <summary>
      /// Provides a distributed cache.
      /// </summary>
      /// <remarks>
      /// Caches are obtained from Create methods on the
      /// <see cref="CacheFactory.Create"/> class.
      /// <para>
      /// When a cache will no longer be used, call <see cref="Cache.Close" />.
      /// Once it <see cref="Cache.IsClosed" /> any attempt to use it
      /// will cause a <c>CacheClosedException</c> to be thrown.
      /// </para><para>
      /// A cache can have multiple root regions, each with a different name.
      /// </para>
      /// </remarks>
      public ref class Cache sealed
        : public IGemFireCache, Internal::SBWrap<gemfire::Cache>
      {
      public:

        /// <summary>
        /// Initializes the cache from an XML file.
        /// </summary>
        /// <param name="cacheXml">pathname of a <c>cache.xml</c> file</param>
        virtual void InitializeDeclarativeCache( String^ cacheXml );

        /// <summary>
        /// Returns the name of this cache.
        /// </summary>
        /// <remarks>
        /// This method does not throw
        /// <c>CacheClosedException</c> if the cache is closed.
        /// </remarks>
        /// <returns>the string name of this cache</returns>
        virtual property String^ Name
        {
          String^ get( );
        }

        /// <summary>
        /// True if this cache has been closed.
        /// </summary>
        /// <remarks>
        /// After a new cache object is created, this method returns false.
        /// After <see cref="Close" /> is called on this cache object, this method
        /// returns true.
        /// </remarks>
        /// <returns>true if this cache is closed, otherwise false</returns>
        virtual property bool IsClosed
        {
          bool get( );
        }

        /// <summary>
        /// Returns the distributed system used to
        /// <see cref="CacheFactory.Create" /> this cache.
        /// </summary>
        /// <remarks>
        /// This method does not throw
        /// <c>CacheClosedException</c> if the cache is closed.
        /// </remarks>
        virtual property GemStone::GemFire::Cache::Generic::DistributedSystem^ DistributedSystem
        {
          GemStone::GemFire::Cache::Generic::DistributedSystem^ get( );
        }

        /// <summary>
        /// Returns the cache transaction manager of
        /// <see cref="CacheFactory.Create" /> this cache.
        /// </summary>
        virtual property GemStone::GemFire::Cache::Generic::CacheTransactionManager^ CacheTransactionManager
        {
          GemStone::GemFire::Cache::Generic::CacheTransactionManager^ get( );
        }

        /// <summary>
        /// Terminates this object cache and releases all the local resources.
        /// </summary>
        /// <remarks>
        /// After this cache is closed, any further
        /// method call on this cache or any region object will throw
        /// <c>CacheClosedException</c>, unless otherwise noted.
        /// </remarks>
        /// <exception cref="CacheClosedException">
        /// if the cache is already closed.
        /// </exception>
        virtual void Close( );

        /// <summary>
        /// Terminates this object cache and releases all the local resources.
        /// </summary>
        /// <remarks>
        /// After this cache is closed, any further
        /// method call on this cache or any region object will throw
        /// <c>CacheClosedException</c>, unless otherwise noted.
        /// </remarks>
        /// <param name="keepalive">whether to keep a durable client's queue alive</param>
        /// <exception cref="CacheClosedException">
        /// if the cache is already closed.
        /// </exception>
        virtual void Close( bool keepalive );

        /// <summary>
        /// Send the client-ready message to the server for a durable client.        
        /// </summary>
        /// <remarks>
        /// This method should only be called for durable clients and
        /// with a cache server version 5.5 onwards.
        /// </remarks>
        /// <exception cref="IllegalStateException">
        /// if there was a problem sending the message to the server.
        /// </exception>
        virtual void ReadyForEvents( );
  
        /// <summary>
        /// Returns an existing region given the full path from root, or null 
        /// if no such region exists.
        /// </summary>
        /// <remarks>
        /// If Pool attached with Region is in multiusersecure mode then don't use return instance of region as no credential are attached with this instance.
        /// Get region from RegionService instance of Cache.<see cref="Cache.CreateAuthenticatedView(PropertiesPtr)" />.
        /// </remarks>
        /// <param name="path">the pathname of the region</param>
        /// <returns>the region</returns>
        generic<class TKey, class TValue>
        virtual IRegion<TKey, TValue>^ GetRegion( String^ path );

        /// <summary>
        /// Returns an array of root regions in the cache. This set is a
        /// snapshot and is not backed by the cache.
        /// </summary>
        /// <remarks>
        /// It is not supported when Cache is created from Pool.
        /// </remarks>
        /// <returns>array of regions</returns>
        generic<class TKey, class TValue>
        virtual array<IRegion<TKey, TValue>^>^ RootRegions();

        /// <summary>
        /// Get a query service object to be able to query the cache.
        /// Supported only when cache is created from Pool(pool is in multiuserSecure mode)
        /// </summary>
        /// <remarks>
        /// Currently only works against the java server in native mode, and
        /// at least some endpoints must have been defined in some regions
        /// before actually firing a query.
        /// </remarks>
        generic<class TKey, class TResult>
        virtual Generic::QueryService<TKey, TResult>^ GetQueryService();

        /// <summary>
        /// Get a query service object to be able to query the cache.
        /// Use only when Cache has more than one Pool.
        /// </summary>
        /// <remarks>
        /// Currently only works against the java server in native mode, and
        /// at least some endpoints must have been defined in some regions
        /// before actually firing a query.
        /// </remarks>
        generic<class TKey, class TResult>
        virtual Generic::QueryService<TKey, TResult>^ GetQueryService(String^ poolName );

        /// <summary>
        /// Returns the instance of <see cref="RegionFactory" /> to create the region
        /// </summary>
        /// <remarks>
        /// Pass the <see cref="RegionShortcut" /> to set the deafult region attributes
        /// </remarks>
        /// <param name="regionShortcut">the regionShortcut to set the default region attributes</param>
        /// <returns>Instance of RegionFactory</returns>
        RegionFactory^ CreateRegionFactory(RegionShortcut regionShortcut); 

        /// <summary>
        /// Returns the instance of <see cref="IRegionService" /> to do the operation on Cache with different Credential.
        /// </summary>
        /// <remarks>
        /// Deafault pool should be in multiuser mode <see cref="CacheFactory.SetMultiuserAuthentication" />
        /// </remarks>
        /// <param name="credentials">the user Credentials.</param>
        /// <returns>Instance of IRegionService</returns>
        IRegionService^ CreateAuthenticatedView(Properties<String^, Object^>^ credentials);

        /// <summary>
        /// Returns the instance of <see cref="IRegionService" /> to do the operation on Cache with different Credential.
        /// </summary>
        /// <remarks>
        /// Deafault pool should be in multiuser mode <see cref="CacheFactory.SetMultiuserAuthentication" />
        /// </remarks>
        /// <param name="credentials">the user Credentials.</param>
        /// <param name="poolName">Pool, which is in multiuser mode.</param>
        /// <returns>Instance of IRegionService</returns>
        IRegionService^ CreateAuthenticatedView(Properties<String^, Object^>^ credentials, String^ poolName);

				///<summary>
				/// Returns whether Cache saves unread fields for Pdx types.
				///</summary>
				virtual bool GetPdxIgnoreUnreadFields();

        ///<summary>
        /// Returns whether { @link PdxInstance} is preferred for PDX types instead of .NET object.
        ///</summary>
        virtual bool GetPdxReadSerialized();

        /// <summary>
        /// Returns a factory that can create a {@link PdxInstance}.
        /// @param className the fully qualified class name that the PdxInstance will become
        ///   when it is fully deserialized.
        /// @return the factory
        /// </summary>
        virtual IPdxInstanceFactory^ CreatePdxInstanceFactory(String^ className);

      internal:

        /// <summary>
        /// Internal factory function to wrap a native object pointer inside
        /// this managed class with null pointer check.
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        /// <returns>
        /// The managed wrapper object; null if the native pointer is null.
        /// </returns>
        inline static Cache^ Create( gemfire::Cache* nativeptr )
        {
          return ( nativeptr != nullptr ?
            gcnew Cache( nativeptr ) : nullptr );
        }


      private:

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline Cache( gemfire::Cache* nativeptr )
          : SBWrap( nativeptr ) { }
      };
      } // end namespace Generic
    }
  }
}
