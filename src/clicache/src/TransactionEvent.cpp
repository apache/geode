/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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