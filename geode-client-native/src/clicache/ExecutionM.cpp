/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "ExecutionM.hpp"
#include "cppcache/Execution.hpp"
#include "ResultCollectorM.hpp"
#include "impl/ManagedResultCollector.hpp"
#include "impl/SafeConvert.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      Execution^ Execution::WithFilter(array<IGFSerializable^>^ routingObj)
      {
        _GF_MG_EXCEPTION_TRY
           gemfire::CacheableVectorPtr rsptr = gemfire::CacheableVector::create();
           for( int index = 0; index < routingObj->Length; index++ )
           {
                rsptr->push_back(gemfire::CacheablePtr(SafeMSerializableConvert( routingObj[ index])));
	   }
	 return Execution::Create(NativePtr->withFilter(rsptr).ptr());
        _GF_MG_EXCEPTION_CATCH_ALL
      }

      Execution^ Execution::WithArgs(IGFSerializable^ args)
      {
        _GF_MG_EXCEPTION_TRY
	 gemfire::CacheablePtr argsptr( SafeMSerializableConvert( args ) );
	 return Execution::Create(NativePtr->withArgs(argsptr).ptr());
        _GF_MG_EXCEPTION_CATCH_ALL
      }

      Execution^ Execution::WithCollector(IResultCollector^ rc)
      {
        _GF_MG_EXCEPTION_TRY
	 gemfire::ResultCollectorPtr rcptr;
	 if ( rc != nullptr ) {
		rcptr = new gemfire::ManagedResultCollector( rc );
	 }
         return Execution::Create( NativePtr->withCollector(rcptr).ptr());
        _GF_MG_EXCEPTION_CATCH_ALL
      }
      IResultCollector^ Execution::Execute(String^ func)
      {
        return Execute(func, false);
      }
      IResultCollector^ Execution::Execute(String^ func, Boolean getResult)
      {
        return Execute(func, getResult, DEFAULT_QUERY_RESPONSE_TIMEOUT);
      }
      IResultCollector^ Execution::Execute(String^ func, Boolean getResult, UInt32 timeout)
      {
        return Execute(func, getResult, timeout, true);
      }

      IResultCollector^ Execution::Execute(String^ func, Boolean getResult, UInt32 timeout, Boolean isHA, Boolean optimizeForWrite)
      {
          _GF_MG_EXCEPTION_TRY
	  ManagedString mg_function( func );
	gemfire::ResultCollectorPtr rc = NativePtr->execute(mg_function.CharPtr, getResult, timeout, isHA, optimizeForWrite);
	return gcnew ResultCollector(rc.ptr());
        _GF_MG_EXCEPTION_CATCH_ALL
      }

      IResultCollector^ Execution::Execute(String^ func, Boolean getResult, UInt32 timeout, Boolean isHA)
      {
        return Execute(func, getResult, timeout, isHA, false);;
      }

    }
  }
}
