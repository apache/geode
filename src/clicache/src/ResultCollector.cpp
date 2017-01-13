/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*=========================================================================
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

          gemfire::Serializable * result = SafeGenericMSerializableConvert((IGFSerializable^)rs);
        NativePtr->addResult( result==NULL ? (NULLPTR) : (gemfire::CacheablePtr(result)) );

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
          gemfire::CacheableVectorPtr results = NativePtr->getResult(timeout);
        array<TResult>^ rs =
          gcnew array<TResult>( results->size( ) );
        for( int32_t index = 0; index < results->size( ); index++ )
        {
          gemfire::CacheablePtr& nativeptr(results->operator[](index));

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
