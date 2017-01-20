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
#include "Execution.hpp"
#include <gfcpp/Execution.hpp>
#include "ResultCollector.hpp"
#include "impl/ManagedResultCollector.hpp"

#include "impl/ManagedString.hpp"
#include "ExceptionTypes.hpp"
#include "impl/SafeConvert.hpp"

using namespace System;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {
namespace Generic
    {

      generic<class TResult>
      generic<class TFilter>
      Execution<TResult>^ Execution<TResult>::WithFilter(System::Collections::Generic::ICollection<TFilter>^ routingObj)
      {
        if (routingObj != nullptr) {
          _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          apache::geode::client::CacheableVectorPtr rsptr = apache::geode::client::CacheableVector::create();
        
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
          
          apache::geode::client::CacheablePtr argsptr( Serializable::GetUnmanagedValueGeneric<TArgs>( args ) );
        return Execution<TResult>::Create(NativePtr->withArgs(argsptr).ptr(), this->m_rc);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TResult>
      Execution<TResult>^ Execution<TResult>::WithCollector(Generic::IResultCollector<TResult>^ rc)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          apache::geode::client::ResultCollectorPtr rcptr;
        if ( rc != nullptr ) {
          ResultCollectorGeneric<TResult>^ rcg = gcnew ResultCollectorGeneric<TResult>();
          rcg->SetResultCollector(rc);
          
          rcptr = new apache::geode::client::ManagedResultCollectorGeneric(  rcg );
          //((apache::geode::client::ManagedResultCollectorGeneric*)rcptr.ptr())->setptr(rcg);
        }
        return Execution<TResult>::Create( NativePtr->withCollector(rcptr).ptr(), rc);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TResult>
      IResultCollector<TResult>^ Execution<TResult>::Execute(String^ func, UInt32 timeout)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          ManagedString mg_function( func );
        apache::geode::client::ResultCollectorPtr rc = NativePtr->execute(mg_function.CharPtr, timeout);
        if(m_rc == nullptr)
          return gcnew ResultCollector<TResult>(rc.ptr());
        else
          return m_rc;
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TResult>
      IResultCollector<TResult>^ Execution<TResult>::Execute(String^ func)
      {
        return Execute(func, DEFAULT_QUERY_RESPONSE_TIMEOUT);
      }
    }
    }
  }
} //namespace 
