/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*=========================================================================
*/
#ifdef CSTX_COMMENTED
#pragma once

#include "gf_defs.hpp"
#include <cppcache/TransactionEvent.hpp>
#include "impl/NativeWrapper.hpp"
//#include "TransactionId.hpp"
//#include "Cache.hpp"

using namespace System;
//namespace GemStone
//{
//  namespace GemFire
//  {
//    namespace Cache 
//    {
//      ref class Cache;
//    }
//  }
//}

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache 
    {
      namespace Generic
      {
				ref class TransactionId;
				ref class Cache;

				generic<class TKey, class TValue>
        ref class EntryEvent;

        /// <summary>
        /// This class encapsulates events that occur for an transaction in a cache.
        /// </summary>
        generic<class TKey, class TValue>
          public ref class TransactionEvent sealed
            : public Internal::UMWrap<gemfire::TransactionEvent>
          {
          public:
            /// <summary>
            /// Gets the transaction id for this transaction.
            /// </summary>
						property GemStone::GemFire::Cache::Generic::TransactionId^ TransactionId
            {
							GemStone::GemFire::Cache::Generic::TransactionId^ get( );
            }

         		/// <summary>
            /// Returns an ordered list of every event for this transaction.
	          /// The event order is consistent with the order in which the operations were
	          /// performed during the transaction.
            /// </summary>
            property array<EntryEvent<TKey, TValue>^>^ Events
            {
              array<EntryEvent<TKey, TValue>^>^ get( );
            }
            
            /// <summary>
            /// Gets the Cache for this transaction event
            /// </summary>
						property GemStone::GemFire::Cache::Generic::Cache^ Cache
            {
              GemStone::GemFire::Cache::Generic::Cache^ get( );
            }

          internal:
            /// <summary>
            /// Internal constructor to wrap a native object pointer
            /// </summary>
            /// <param name="nativeptr">The native object pointer</param>
            inline TransactionEvent( gemfire::TransactionEvent* nativeptr )
              : UMWrap( nativeptr, false ) { }
          };

      }
    }
  }
} //namespace 
#endif