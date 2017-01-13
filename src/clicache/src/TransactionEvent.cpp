/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifdef CSTX_COMMENTED
//#include "gf_includes.hpp"
#include "TransactionEvent.hpp"
#include "Log.hpp"
#include "impl/SafeConvert.hpp"
#include "TransactionId.hpp"
#include "Cache.hpp"
#include "EntryEvent.hpp"
#include "Cache.hpp"


using namespace System;
using namespace GemStone::GemFire::Cache::Generic;


namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      generic<class TKey, class TValue>
      Cache^ TransactionEvent<TKey, TValue>::Cache::get( )
      {
        gemfire::CachePtr & nativeptr(
          NativePtr->getCache( ) );

				return GemStone::GemFire::Cache::Generic::Cache::Create(
          nativeptr.ptr( ) );
      }
      
      generic<class TKey, class TValue>
			GemStone::GemFire::Cache::Generic::TransactionId^ TransactionEvent<TKey, TValue>::TransactionId::get( )
      {
        gemfire::TransactionIdPtr & nativeptr(
          NativePtr->getTransactionId( ) );

				return GemStone::GemFire::Cache::Generic::TransactionId::Create(
          nativeptr.ptr( ) );
      }
    
      generic<class TKey, class TValue>
      array<EntryEvent<TKey, TValue>^>^ TransactionEvent<TKey, TValue>::Events::get( )
      {
        gemfire::VectorOfEntryEvent vee;
        vee = NativePtr->getEvents();
        array<EntryEvent<TKey, TValue>^>^ events =
          gcnew array<EntryEvent<TKey, TValue>^>( vee.size( ) );
        // Loop through the unmanaged event objects to convert them to the managed generic objects. 
        for( int32_t index = 0; index < vee.size( ); index++ )
        {
          gemfire::EntryEventPtr& nativeptr( vee[ index ] );
          EntryEvent<TKey, TValue> entryEvent( nativeptr.ptr( ) );
          events[ index ] = (%entryEvent);
        }
        return events;
      }

    }
  }
}
 } //namespace 
#endif