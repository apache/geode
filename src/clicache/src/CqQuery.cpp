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

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {
namespace Generic
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

          apache::geode::client::CqResultsPtr& nativeptr =
            NativePtr->executeWithInitialResults(timeout);
          if (nativeptr.ptr() == NULL) return nullptr;

          apache::geode::client::ResultSet* resultptr = dynamic_cast<apache::geode::client::ResultSet*>(
            nativeptr.ptr( ) );
          if ( resultptr == NULL )
          {
            apache::geode::client::StructSet* structptr = dynamic_cast<apache::geode::client::StructSet*>(
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
        apache::geode::client::CqState::StateType st =  NativePtr->getState( );
        CqStateType state;
        switch (st)
        {
          case apache::geode::client::CqState::STOPPED: {
            state = CqStateType::STOPPED;
            break;
          }
          case apache::geode::client::CqState::RUNNING: {
            state = CqStateType::RUNNING;
            break;
          }
          case apache::geode::client::CqState::CLOSED: {
            state = CqStateType::CLOSED;
            break;
          }
          case apache::geode::client::CqState::CLOSING: {
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
