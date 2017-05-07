/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*=========================================================================
*/

//#include "gf_includesN.hpp"
#include "ExecutionMN.hpp"
#include <cppcache/Execution.hpp>
#include "ResultCollectorMN.hpp"
#include "impl/ManagedResultCollectorN.hpp"

#include "impl/ManagedStringN.hpp"
#include "ExceptionTypesMN.hpp"
#include "impl/SafeConvertN.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      generic<class TResult>
      generic<class TFilter>
      Execution<TResult>^ Execution<TResult>::WithFilter(System::Collections::Generic::ICollection<TFilter>^ routingObj)
      {
        if (routingObj != nullptr) {
          _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          gemfire::CacheableVectorPtr rsptr = gemfire::CacheableVector::create();
        
          for each(TFilter item in routingObj)
          {
            rsptr->push_back(Serializable::GetUnmanagedValueGeneric<TFilter>( item ));
          }
          
          return Execution<TResult>::Create(NativePtr->withFilter(rsptr).ptr(), this->m_rc);
          _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
        }
        else {
          throw gcnew IllegalArgumentException("Execution<TResult>::WithFilter: null TFilter provided");
        }
      }

      generic<class TResult>
      generic<class TArgs>
      Execution<TResult>^ Execution<TResult>::WithArgs( TArgs args )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          
          gemfire::CacheablePtr argsptr( Serializable::GetUnmanagedValueGeneric<TArgs>( args ) );
        return Execution<TResult>::Create(NativePtr->withArgs(argsptr).ptr(), this->m_rc);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TResult>
      Execution<TResult>^ Execution<TResult>::WithCollector(Generic::IResultCollector<TResult>^ rc)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          gemfire::ResultCollectorPtr rcptr;
        if ( rc != nullptr ) {
          ResultCollectorGeneric<TResult>^ rcg = gcnew ResultCollectorGeneric<TResult>();
          rcg->SetResultCollector(rc);
          
          rcptr = new gemfire::ManagedResultCollectorGeneric(  rcg );
          //((gemfire::ManagedResultCollectorGeneric*)rcptr.ptr())->setptr(rcg);
        }
        return Execution<TResult>::Create( NativePtr->withCollector(rcptr).ptr(), rc);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TResult>
      IResultCollector<TResult>^ Execution<TResult>::Execute(String^ func, Boolean getResult, UInt32 timeout, Boolean isHA)
      {
        return Execute(func, getResult, timeout, isHA, false);;
      }

      generic<class TResult>
      IResultCollector<TResult>^ Execution<TResult>::Execute(String^ func, Boolean getResult, UInt32 timeout, Boolean isHA, Boolean optimizeForWrite)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          ManagedString mg_function( func );
        gemfire::ResultCollectorPtr rc = NativePtr->execute(mg_function.CharPtr, getResult, timeout, isHA, optimizeForWrite);
        if(m_rc == nullptr)
          return gcnew ResultCollector<TResult>(rc.ptr());
        else
          return m_rc;
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TResult>
      IResultCollector<TResult>^ Execution<TResult>::Execute(String^ func, Boolean getResult, UInt32 timeout)
      {
        return Execute(func, getResult, timeout, true);
      }

      generic<class TResult>
      IResultCollector<TResult>^ Execution<TResult>::Execute(String^ func, Boolean getResult)
      {
        return Execute(func, getResult, DEFAULT_QUERY_RESPONSE_TIMEOUT);
      }

      generic<class TResult>
      IResultCollector<TResult>^ Execution<TResult>::Execute(String^ func)
      {
        return Execute(func, false);
      }
    }
    }
  }
} //namespace 
