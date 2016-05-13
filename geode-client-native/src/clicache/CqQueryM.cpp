/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "CqQueryM.hpp"
#include "QueryM.hpp"
#include "CqAttributesM.hpp"
#include "CqAttributesMutatorM.hpp"
#include "CqStatisticsM.hpp"
#include "ISelectResults.hpp"
#include "ResultSetM.hpp"
#include "StructSetM.hpp"
#include "ExceptionTypesM.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      ICqResults^ CqQuery::ExecuteWithInitialResults()
      {
        return ExecuteWithInitialResults(DEFAULT_QUERY_RESPONSE_TIMEOUT);
      }

      ICqResults^ CqQuery::ExecuteWithInitialResults(uint32_t timeout)
      {
        _GF_MG_EXCEPTION_TRY

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
            return StructSet::Create(structptr);
          }
          /*else
          {
            return ResultSet::Create(resultptr);
          }*/

          return nullptr;

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void CqQuery::Execute()
      {
        _GF_MG_EXCEPTION_TRY

          NativePtr->execute();

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      String^ CqQuery::QueryString::get( )
      {
        return ManagedString::Get( NativePtr->getQueryString( ) );
      }

      String^ CqQuery::Name::get( )
      {
        return ManagedString::Get( NativePtr->getName( ) );
      }

      Query^ CqQuery::GetQuery( )
      {
        return Query::Create(NativePtr->getQuery().ptr());
      }

      CqAttributes^ CqQuery::GetCqAttributes( )
      {
        return CqAttributes::Create(NativePtr->getCqAttributes( ).ptr());
      }

      CqAttributesMutator^ CqQuery::GetCqAttributesMutator( )
      {
        return CqAttributesMutator::Create(NativePtr->getCqAttributesMutator().ptr());
      }

      CqStatistics^ CqQuery::GetStatistics( )
      {
        return CqStatistics::Create(NativePtr->getStatistics().ptr());
      }

      CqStateType CqQuery::GetState( )
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

      void CqQuery::Stop( )
      {
        _GF_MG_EXCEPTION_TRY

          NativePtr->stop( );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void CqQuery::Close( )
      {
        _GF_MG_EXCEPTION_TRY

          NativePtr->close( );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      bool CqQuery::IsRunning( )
      {
        _GF_MG_EXCEPTION_TRY

          return NativePtr->isRunning( );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      bool CqQuery::IsStopped( )
      {
        _GF_MG_EXCEPTION_TRY

          return NativePtr->isStopped( );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      bool CqQuery::IsClosed( )
      {
        _GF_MG_EXCEPTION_TRY

          return NativePtr->isClosed( );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

    }
  }
}
