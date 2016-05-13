/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "ResultCollectorM.hpp"
#include "ExceptionTypesM.hpp"
#include "impl/SafeConvert.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      void ResultCollector::AddResult( IGFSerializable^ rs )
      {
	  _GF_MG_EXCEPTION_TRY
	  
      gemfire::Serializable * result = SafeMSerializableConvert(rs);
      NativePtr->addResult( result==NULL ? (NULLPTR) : (gemfire::CacheablePtr(result)) );
	  
         _GF_MG_EXCEPTION_CATCH_ALL
      }
      array<IGFSerializable^>^  ResultCollector::GetResult()
      {
	return GetResult( DEFAULT_QUERY_RESPONSE_TIMEOUT );
      }
      array<IGFSerializable^>^  ResultCollector::GetResult(UInt32 timeout)
      {
	  _GF_MG_EXCEPTION_TRY
	  gemfire::CacheableVectorPtr results = NativePtr->getResult(timeout);
	  array<IGFSerializable^>^ rs =
	       gcnew array<IGFSerializable^>( results->size( ) );
	  for( int32_t index = 0; index < results->size( ); index++ )
	  {
	        gemfire::CacheablePtr& nativeptr(results->operator[](index));

                rs[ index] =  SafeUMSerializableConvert( nativeptr.ptr( ) );
          }
          return rs;
         _GF_MG_EXCEPTION_CATCH_ALL
      }
      void ResultCollector::EndResults()
      {
	_GF_MG_EXCEPTION_TRY
	 NativePtr->endResults();
	_GF_MG_EXCEPTION_CATCH_ALL
      }

      void ResultCollector::ClearResults()
      {
      }
    }
  }
}
