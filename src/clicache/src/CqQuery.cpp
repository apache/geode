/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "gf_includes.hpp"
#include "CqQuery.hpp"
#include "Query.hpp"
#include "CqAttributes.hpp"
#include "CqAttributesMutator.hpp"
#include "CqStatistics.hpp"
#include "ISelectResults.hpp"
#include "ResultSet.hpp"
#include "StructSet.hpp"
#include "ExceptionTypes.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      generic<class TKey, class TResult>
      ICqResults<TResult>^ CqQuery<TKey, TResult>::ExecuteWithInitialResults()
      {
        return ExecuteWithInitialResults(DEFAULT_QUERY_RESPONSE_TIMEOUT);
      }

      generic<class TKey, class TResult>
      ICqResults<TResult>^ CqQuery<TKey, TResult>::ExecuteWithInitialResults(uint32_t timeout)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::CqResultsPtr& nativeptr =
            NativePtr->executeWithInitialResults(timeout);
          if (nativeptr.ptr() == NULL) return nullptr;

          gemfire::ResultSet* resultptr = dynamic_cast<gemfire::ResultSet*>(
            nativeptr.ptr( ) );
          if ( resultptr == NULL )
          {
            gemfire::StructSet* structptr = dynamic_cast<gemfire::StructSet*>(
              nativeptr.ptr( ) );
            if ( structptr == NULL )
            {
              return nullptr;
            }
            return StructSet<TResult>::Create(structptr);
          }
          /*else
          {
            return ResultSet::Create(resultptr);
          }*/

          return nullptr;

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TResult>
      void CqQuery<TKey, TResult>::Execute()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          NativePtr->execute();

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TResult>
      String^ CqQuery<TKey, TResult>::QueryString::get( )
      {
        return ManagedString::Get( NativePtr->getQueryString( ) );
      }

      generic<class TKey, class TResult>
      String^ CqQuery<TKey, TResult>::Name::get( )
      {
        return ManagedString::Get( NativePtr->getName( ) );
      }

      generic<class TKey, class TResult>
      Query<TResult>^ CqQuery<TKey, TResult>::GetQuery( )
      {
        return Query<TResult>::Create(NativePtr->getQuery().ptr());
      }

      generic<class TKey, class TResult>
      CqAttributes<TKey, TResult>^ CqQuery<TKey, TResult>::GetCqAttributes( )
      {
        return CqAttributes<TKey, TResult>::Create(NativePtr->getCqAttributes( ).ptr());
      }

      generic<class TKey, class TResult>
      CqAttributesMutator<TKey, TResult>^ CqQuery<TKey, TResult>::GetCqAttributesMutator( )
      {
        return CqAttributesMutator<TKey, TResult>::Create(NativePtr->getCqAttributesMutator().ptr());
      }

      generic<class TKey, class TResult>
      CqStatistics^ CqQuery<TKey, TResult>::GetStatistics( )
      {
        return CqStatistics::Create(NativePtr->getStatistics().ptr());
      }

      generic<class TKey, class TResult>
      CqStateType CqQuery<TKey, TResult>::GetState( )
      {
        gemfire::CqState::StateType st =  NativePtr->getState( );
        CqStateType state;
        switch (st)
        {
          case gemfire::CqState::STOPPED: {
            state = CqStateType::STOPPED;
            break;
          }
          case gemfire::CqState::RUNNING: {
            state = CqStateType::RUNNING;
            break;
          }
          case gemfire::CqState::CLOSED: {
            state = CqStateType::CLOSED;
            break;
          }
          case gemfire::CqState::CLOSING: {
            state = CqStateType::CLOSING;
            break;
          }
          default: {
            state = CqStateType::INVALID;
            break;
          }
        }
        return state;
      }

      generic<class TKey, class TResult>
      void CqQuery<TKey, TResult>::Stop( )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          NativePtr->stop( );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TResult>
      void CqQuery<TKey, TResult>::Close( )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          NativePtr->close( );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TResult>
      bool CqQuery<TKey, TResult>::IsRunning( )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          return NativePtr->isRunning( );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TResult>
      bool CqQuery<TKey, TResult>::IsStopped( )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          return NativePtr->isStopped( );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TResult>
      bool CqQuery<TKey, TResult>::IsClosed( )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          return NativePtr->isClosed( );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

    }
  }
}
 } //namespace 
