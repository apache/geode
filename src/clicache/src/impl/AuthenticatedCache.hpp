/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "../gf_defs.hpp"
#include <gfcpp/RegionService.hpp>
#include "NativeWrapper.hpp"
#include "../RegionShortcut.hpp"
#include "../RegionFactory.hpp"
//#include "../gf_includes.hpp"
#include "../IRegionService.hpp"
#include "../Region.hpp"

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

      //ref class DistributedSystem;
      //ref class Region;
      //ref class RegionAttributes;
      //ref class QueryService;
      //ref class FunctionService;

      /// <summary>
      /// Provides a distributed cache.
      /// </summary>
      /// <remarks>
      /// Caches are obtained from static methods on the
      /// <see cref="CacheFactory"/> class.
      /// <para>
      /// When a cache is created a <see cref="DistributedSystem" />
      /// must be specified.
      /// </para><para>
      /// When a cache will no longer be used, call <see cref="Cache.Close" />.
      /// Once it <see cref="Cache.IsClosed" /> any attempt to use it
      /// will cause a <c>CacheClosedException</c> to be thrown.
      /// </para><para>
      /// A cache can have multiple root regions, each with a different name.
      /// </para>
      /// </remarks>
      public ref class AuthenticatedCache 
        : public IRegionService, Internal::SBWrap<gemfire::RegionService>
      {
      public:

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
        /// Terminates this object cache and releases all the local resources.
        /// If Cache instance created from Pool(pool is in multiuser mode), then it reset user related security data.
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
        /// Returns an existing region given the full path from root, or null 
        /// if no such region exists.
        /// </summary>
        /// <remarks>
        /// If Pool attached with Region is in multiusersecure mode then don't use return instance of region as no credential are attached with this instance.
        /// Get logical instance of region Pool->CreateSecureUserCache(<Credential>).getRegion(<name>) to do the operation on Cache. 
        /// </remarks>
        /// <param name="path">the pathname of the region</param>
        /// <returns>the region</returns>
        generic<class TKey, class TValue>
        virtual IRegion<TKey, TValue>^ GetRegion( String^ path );

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
        virtual QueryService<TKey, TResult>^ GetQueryService();

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
        inline static AuthenticatedCache^ Create( gemfire::RegionService* nativeptr )
        {
          return ( nativeptr != nullptr ?
            gcnew AuthenticatedCache( nativeptr ) : nullptr );
        }

      private:

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline AuthenticatedCache( gemfire::RegionService* nativeptr )
          : SBWrap( nativeptr ) { }
      };
      } // end namespace Generic
    }
  }
}
