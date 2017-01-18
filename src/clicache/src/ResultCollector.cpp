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


//#include "gf_includes.hpp"
#include "ResultCollector.hpp"
#include "impl/ManagedString.hpp"
#include "ExceptionTypes.hpp"
#include "impl/SafeConvert.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      generic<class TResult>
      void ResultCollector<TResult>::AddResult( TResult rs )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          apache::geode::client::Serializable * result = SafeGenericMSerializableConvert((IGFSerializable^)rs);
        NativePtr->addResult( result==NULL ? (NULLPTR) : (apache::geode::client::CacheablePtr(result)) );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TResult>
      System::Collections::Generic::ICollection<TResult>^  ResultCollector<TResult>::GetResult()
      {
        return GetResult( DEFAULT_QUERY_RESPONSE_TIMEOUT );
      }

      generic<class TResult>
      System::Collections::Generic::ICollection<TResult>^  ResultCollector<TResult>::GetResult(UInt32 timeout)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          apache::geode::client::CacheableVectorPtr results = NativePtr->getResult(timeout);
        array<TResult>^ rs =
          gcnew array<TResult>( results->size( ) );
        for( int32_t index = 0; index < results->size( ); index++ )
        {
          apache::geode::client::CacheablePtr& nativeptr(results->operator[](index));

          rs[ index] =  Serializable::GetManagedValueGeneric<TResult>( nativeptr);
        }
        ICollection<TResult>^ collectionlist = (ICollection<TResult>^)rs;
        return collectionlist;

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TResult>
      void ResultCollector<TResult>::EndResults()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          NativePtr->endResults();
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TResult>
      void ResultCollector<TResult>::ClearResults(/*bool*/)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          NativePtr->clearResults();
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }
    }
    }
  }
} //namespace 
