/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifdef CSTX_COMMENTED
#include "gf_includes.hpp"
#include "TransactionEventM.hpp"
#include "impl/SafeConvert.hpp"
#include "CacheM.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      GemStone::GemFire::Cache::Cache^ TransactionEvent::Cache::get( )
      {
        gemfire::CachePtr& cacheptr( NativePtr->getCache( ) );
        return GemStone::GemFire::Cache::Cache::Create( cacheptr.ptr( ) );
      }

      TransactionId^ TransactionEvent::TransactionId::get( )
      {
        gemfire::TransactionIdPtr& tidptr( NativePtr->getTransactionId( ) );
        return GemStone::GemFire::Cache::TransactionId::Create( tidptr.ptr( ) );
      }

      array<EntryEvent^>^ TransactionEvent::Events::get( )
      {
        gemfire::VectorOfEntryEvent vee;
        vee = NativePtr->getEvents();
        array<EntryEvent^>^ events =
          gcnew array<EntryEvent^>( vee.size( ) );
        // Loop through the unmanaged event objects to convert them to the managed objects. 
        for( int32_t index = 0; index < vee.size( ); index++ )
        {
          gemfire::EntryEventPtr& nativeptr( vee[ index ] );
          EntryEvent entryEvent( nativeptr.ptr( ) );
          events[ index ] = (%entryEvent);
        }
        return events;
      }
    }
  }
}
#endif