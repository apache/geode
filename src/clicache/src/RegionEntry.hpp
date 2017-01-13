/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include <gfcpp/RegionEntry.hpp>
#include "impl/NativeWrapper.hpp"
//#include "ICacheableKey.hpp"
#include "IRegion.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache {
      namespace Generic
    {

      //ref class Region;
      ref class CacheStatistics;

      /// <summary>
      /// An object in a region that represents an <em>entry</em>, that is, a key-value pair.
      /// </summary>
      /// <remarks>
      /// This object's operations are not distributed, do not acquire any locks, and do not affect
      /// <c>CacheStatistics</c>.
      ///
      /// Unless otherwise noted, all of these methods throw a
      /// <c>CacheClosedException</c> if the cache is closed at the time of
      /// invocation, or an <c>EntryDestroyedException</c> if the entry has been
      /// destroyed.
      ///
      /// Call <see cref="IsDestroyed" /> to see if an entry has already been destroyed.
      /// </remarks>
      generic<class TKey, class TValue>
      public ref class RegionEntry sealed
        : public Internal::SBWrap<gemfire::RegionEntry>
      {
      public:

        /// <summary>
        /// Returns the key for this entry.
        /// </summary>
        /// <returns>the key for this entry</returns>
        /// <exception cref="CacheClosedException">
        /// if the cache is closed at the time of invocation
        /// </exception>
        /// <exception cref="EntryDestroyedException">
        /// if the entry has been destroyed
        /// </exception>
        property TKey Key
        {
          TKey get( );
        }

        /// <summary>
        /// Returns the value of this entry in the local cache. Does not invoke
        /// an <c>ICacheLoader</c>, does not do a netSearch, netLoad, etc.
        /// </summary>
        /// <returns>
        /// the value, or null if this entry is invalid -- see <see cref="IsDestroyed" />
        /// </returns>
        /// <exception cref="CacheClosedException">
        /// if the cache is closed at the time of invocation
        /// </exception>
        /// <exception cref="EntryDestroyedException">
        /// if the entry has been destroyed
        /// </exception>
        property TValue Value
        {
          TValue get( );
        }

        /// <summary>
        /// Returns the region that contains this entry.
        /// </summary>
        /// <returns>the region that contains this entry</returns>
        /// <exception cref="CacheClosedException">
        /// if the cache is closed at the time of invocation
        /// </exception>
        /// <exception cref="EntryDestroyedException">
        /// if the entry has been destroyed
        /// </exception>
        property IRegion<TKey, TValue>^ Region
        {
          IRegion<TKey, TValue>^ get( );
        }

        /// <summary>
        /// Returns the statistics for this entry.
        /// </summary>
        /// <returns>the CacheStatistics for this entry</returns>
        /// <exception cref="StatisticsDisabledException">
        /// if statistics have been disabled for this region
        /// </exception>
        property GemStone::GemFire::Cache::Generic::CacheStatistics^ Statistics
        {
          GemStone::GemFire::Cache::Generic::CacheStatistics^ get( );
        }

        ///// <summary>
        ///// Returns the user attribute for this entry in the local cache.
        ///// </summary>
        ///// <returns>the user attribute for this entry</returns>
        ////Object^ GetUserAttribute( );

        ///// <summary>
        ///// Sets the user attribute for this entry. Does not distribute the user
        ///// attribute to other caches.
        ///// </summary>
        ///// <param name="uptr">a pointer to the user attribute for this entry</param>
        ///// <returns>
        ///// the previous user attribute or null if no user attributes have been
        ///// set for this entry
        ///// </returns>
        ////void SetUserAttribute( Object^ uptr );

        /// <summary>
        /// True if this entry has been destroyed.
        /// </summary>
        /// <remarks>
        /// Does not throw a <c>EntryDestroyedException</c> if this entry
        /// has been destroyed.
        /// </remarks>
        /// <returns>true if this entry has been destroyed</returns>
        /// <exception cref="CacheClosedException">
        /// if the cache is closed at the time of invocation
        /// </exception>
        property bool IsDestroyed
        {
          bool get( );
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
        inline static Generic::RegionEntry<TKey, TValue>^ Create( gemfire::RegionEntry* nativeptr )
        {
          return ( nativeptr != nullptr ?
            gcnew Generic::RegionEntry<TKey, TValue>( nativeptr ) : nullptr );
        }


      private:

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline RegionEntry( gemfire::RegionEntry* nativeptr )
          : SBWrap( nativeptr ) { }
      };

    }
  }
}
 } //namespace 
