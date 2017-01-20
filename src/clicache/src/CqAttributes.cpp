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
#include "CqAttributes.hpp"
#include "impl/ManagedCqListener.hpp"
#include "ICqListener.hpp"
#include "impl/ManagedCqStatusListener.hpp"
#include "ICqStatusListener.hpp"

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
      array<ICqListener<TKey, TResult>^>^ CqAttributes<TKey, TResult>::getCqListeners( )
      {
        apache::geode::client::VectorOfCqListener vrr;
        NativePtr->getCqListeners( vrr );
        array<ICqListener<TKey, TResult>^>^ listners = gcnew array<ICqListener<TKey, TResult>^>( vrr.size( ) );

        for( int32_t index = 0; index < vrr.size( ); index++ )
        {
          apache::geode::client::CqListenerPtr& nativeptr( vrr[ index ] );
          apache::geode::client::ManagedCqListenerGeneric* mg_listener =
            dynamic_cast<apache::geode::client::ManagedCqListenerGeneric*>( nativeptr.ptr( ) );
          if (mg_listener != nullptr)
          {
            listners[ index ] =  (ICqListener<TKey, TResult>^) mg_listener->userptr( );
          }else 
          {
            apache::geode::client::ManagedCqStatusListenerGeneric* mg_statuslistener =
              dynamic_cast<apache::geode::client::ManagedCqStatusListenerGeneric*>( nativeptr.ptr( ) );
            if (mg_statuslistener != nullptr) {
              listners[ index ] =  (ICqStatusListener<TKey, TResult>^) mg_statuslistener->userptr( );
            }
            else {
              listners[ index ] =  nullptr;
            }
          }
        }
        return listners;
      }

    }
    }
  }
} //namespace 
