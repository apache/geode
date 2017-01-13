/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include <gfcpp/AttributesMutator.hpp>
//#include "impl/NativeWrapper.hpp"
#include "ExpirationAction.hpp"
#include "ICacheListener.hpp"
#include "ICacheLoader.hpp"
#include "ICacheWriter.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      /// <summary>
      /// Supports modification of certain region attributes after the region
      /// has been created.
      /// </summary>
      /// <remarks>
      /// <para>
      /// It is required that the attributes be completely initialized using an
      /// <see cref="AttributesFactory" /> before creating the region.
      /// AttributesMutator can be applied to adjusting and tuning a subset of
      /// attributes that are modifiable at runtime.
      /// </para><para>
      /// The setter methods all return the previous value of the attribute.
      /// </para>
      /// </remarks>
      /// <seealso cref="Region.AttributesMutator" />
      /// <seealso cref="RegionAttributes" />
      /// <seealso cref="AttributesFactory" />
      generic<class TKey, class TValue>
      public ref class AttributesMutator sealed
        : public Internal::SBWrap<gemfire::AttributesMutator>
      {
      public:

        /// <summary>
        /// Sets the idleTimeout duration for region entries.
        /// </summary>
        /// <param name="idleTimeout">
        /// the idleTimeout in seconds for entries in this region, or 0 for no idle timeout
        /// </param>
        /// <returns>the previous value</returns>
        /// <exception cref="IllegalStateException">
        /// if the new idleTimeout changes entry expiration from
        /// disabled to enabled or enabled to disabled.
        /// </exception>
        int32_t SetEntryIdleTimeout( int32_t idleTimeout );

        /// <summary>
        /// Sets the idleTimeout action for region entries.
        /// </summary>
        /// <param name="action">
        /// the idleTimeout action for entries in this region
        /// </param>
        /// <returns>the previous action</returns>
        ExpirationAction SetEntryIdleTimeoutAction( ExpirationAction action );

        /// <summary>
        /// Sets the timeToLive duration for region entries.
        /// </summary>
        /// <param name="timeToLive">
        /// the timeToLive in seconds for entries in this region, or 0 to disable time-to-live
        /// </param>
        /// <returns>the previous value</returns>
        /// <exception cref="IllegalStateException">
        /// if the new timeToLive changes entry expiration from
        /// disabled to enabled or enabled to disabled
        /// </exception>
        int32_t SetEntryTimeToLive( int32_t timeToLive );

        /// <summary>
        /// Set the timeToLive action for region entries.
        /// </summary>
        /// <param name="action">
        /// the timeToLive action for entries in this region
        /// </param>
        /// <returns>the previous action</returns>
        ExpirationAction SetEntryTimeToLiveAction( ExpirationAction action );

        /// <summary>
        /// Sets the idleTimeout duration for the region itself.
        /// </summary>
        /// <param name="idleTimeout">
        /// the idleTimeout for this region, in seconds, or 0 to disable idle timeout
        /// </param>
        /// <returns>the previous value</returns>
        /// <exception cref="IllegalStateException">
        /// if the new idleTimeout changes region expiration from
        /// disabled to enabled or enabled to disabled.
        /// </exception>
        int32_t SetRegionIdleTimeout( int32_t idleTimeout );

        /// <summary>
        /// Sets the idleTimeout action for the region itself.
        /// </summary>
        /// <param name="action">
        /// the idleTimeout action for this region
        /// </param>
        /// <returns>the previous action</returns>
        ExpirationAction SetRegionIdleTimeoutAction( ExpirationAction action );

        /// <summary>
        /// Sets the timeToLive duration for the region itself.
        /// </summary>
        /// <param name="timeToLive">
        /// the timeToLive for this region, in seconds, or 0 to disable time-to-live
        /// </param>
        /// <returns>the previous value</returns>
        /// <exception cref="IllegalStateException">
        /// if the new timeToLive changes region expiration from
        /// disabled to enabled or enabled to disabled.
        /// </exception>
        int32_t SetRegionTimeToLive( int32_t timeToLive );

        /// <summary>
        /// Sets the timeToLive action for the region itself.
        /// </summary>
        /// <param name="action">
        /// the timeToLiv eaction for this region
        /// </param>
        /// <returns>the previous action</returns>
        ExpirationAction SetRegionTimeToLiveAction( ExpirationAction action );

        /// <summary>
        /// Sets the maximum entry count in the region before LRU eviction.
        /// </summary>
        /// <param name="entriesLimit">the number of entries to allow, or 0 to disable LRU</param>
        /// <returns>the previous value</returns>
        /// <exception cref="IllegalStateException">
        /// if the new entriesLimit changes LRU from
        /// disabled to enabled or enabled to disabled.
        /// </exception>
        uint32_t SetLruEntriesLimit( uint32_t entriesLimit );

        /// <summary>
        /// Sets the CacheListener for the region.
        /// The previous cache listener (if any) will be replaced with the given <c>cacheListener</c>.
        /// </summary>
        /// <param name="cacheListener">
        /// user-defined cache listener, or null for no cache listener
        /// </param>
        void SetCacheListener( ICacheListener<TKey, TValue>^ cacheListener );

        /// <summary>
        /// Sets the library path for the library that will be invoked for the listener of the region.
        /// The previous cache listener will be replaced with a listener created
        /// using the factory function provided in the given library.
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
        void SetCacheListener( String^ libPath, String^ factoryFunctionName );

        /// <summary>
        /// Sets the CacheLoader for the region.
        /// The previous cache loader (if any) will be replaced with the given <c>cacheLoader</c>.
        /// </summary>
        /// <param name="cacheLoader">
        /// user-defined cache loader, or null for no cache loader
        /// </param>
        void SetCacheLoader( ICacheLoader<TKey, TValue>^ cacheLoader );

        /// <summary>
        /// Sets the library path for the library that will be invoked for the loader of the region.
        /// The previous cache loader will be replaced with a loader created
        /// using the factory function provided in the given library.
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
        void SetCacheLoader( String^ libPath, String^ factoryFunctionName );

        /// <summary>
        /// Sets the CacheListener for the region.
        /// The previous cache writer (if any) will be replaced with the given <c>cacheWriter</c>.
        /// </summary>
        /// <param name="cacheWriter">
        /// user-defined cache writer, or null for no cache writer
        /// </param>
        void SetCacheWriter( ICacheWriter<TKey, TValue>^ cacheWriter );

        /// <summary>
        /// Sets the library path for the library that will be invoked for the writer of the region.
        /// The previous cache writer will be replaced with a writer created
        /// using the factory function provided in the given library.
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
        void SetCacheWriter( String^ libPath, String^ factoryFunctionName );


      internal:
        /// <summary>
        /// Internal factory function to wrap a native object pointer inside
        /// this managed class with null pointer check.
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        /// <returns>
        /// The managed wrapper object; null if the native pointer is null.
        /// </returns>
        inline static AttributesMutator<TKey, TValue>^ Create( gemfire::AttributesMutator* nativeptr )
        {
          return ( nativeptr != nullptr ?
            gcnew AttributesMutator<TKey, TValue>( nativeptr ) : nullptr );
        }


      private:

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline AttributesMutator<TKey, TValue>( gemfire::AttributesMutator* nativeptr )
          : SBWrap( nativeptr ) { }
      };

    }
  }
}
 } //namespace 
