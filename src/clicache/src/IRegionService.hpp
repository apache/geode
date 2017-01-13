/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
//#include "gf_includes.hpp"
#include "QueryService.hpp"
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
      interface class IPdxInstanceFactory;
      /// <summary>
      /// A RegionService provides access to existing regions that exist
      /// in a <see cref="Cache" />.
      /// Regions can be obtained using <see cref="Cache.GetRegion" />
      /// and queried using <see cref="Cache.GetQueryService/>.
      /// </summary>
      /// <remarks>
      /// Caches are obtained from  methods on the
      /// <see cref="CacheFactory.Create"/> class.
      /// <para>
      /// When a cache will no longer be used, call <see cref="Cache.Close" />.
      /// Once it <see cref="Cache.IsClosed" /> any attempt to use it
      /// will cause a <c>CacheClosedException</c> to be thrown.
      /// </para><para>
      /// A cache can have multiple root regions, each with a different name.
      /// </para>
      /// </remarks>
      public interface class IRegionService 
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
        property bool IsClosed
        {
          bool get( );
        }        

        /// <summary>
        /// Terminates this object cache and releases all the local resources.
        /// If RegionService is created from <see cref="Cache.CreateAuthenticatedView" />, then it clears user related security data.
        /// </summary>
        /// <remarks>
        /// After this cache is closed, any further
        /// method call on this cache or any region object will throw
        /// <c>CacheClosedException</c>, unless otherwise noted.
        /// </remarks>
        /// <exception cref="CacheClosedException">
        /// if the cache is already closed.
        /// </exception>
        void Close( );

        /// <summary>
        /// Returns an existing region given the full path from root, or null 
        /// if no such region exists.
        /// </summary>
        /// <param name="name">the name of the region</param>
        /// <returns>the region</returns>
        generic<class TKey, class TValue>
        IRegion<TKey, TValue>^ GetRegion( String^ name );

        /// <summary>
        /// Get a query service object to be able to query the cache.
        /// </summary>
        /// <remarks>
        /// Currently only works against the java server in native mode, and
        /// at least some endpoints must have been defined in some regions
        /// before actually firing a query.
        /// </remarks>
        generic<class TKey, class TResult>
        Generic::QueryService<TKey, TResult>^ GetQueryService();
        /// <summary>
        /// Returns an array of root regions in the cache. This set is a
        /// snapshot and is not backed by the cache.
        /// </summary>
        /// <remarks>
        /// It is not supported when Cache is created from Pool.
        /// </remarks>
        /// <returns>array of regions</returns>
        generic<class TKey, class TValue>
        array<IRegion<TKey, TValue>^>^ RootRegions( );

        /// <summary>
        /// Returns a factory that can create a {@link PdxInstance}.
        /// @param className the fully qualified class name that the PdxInstance will become
        ///   when it is fully deserialized.
        /// @return the factory
        /// </summary>
        IPdxInstanceFactory^ CreatePdxInstanceFactory(String^ className);
      };
      } // end namespace Generic
    }
  }
}
