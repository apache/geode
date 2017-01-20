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
#include "FunctionService.hpp"
#include "Pool.hpp"
#include "Region.hpp"
#include "Execution.hpp"
#include <gfcpp/RegionService.hpp>
#include "impl/AuthenticatedCache.hpp"
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
      generic <class TResult>
      generic <class TKey, class TValue>
      Execution<TResult>^ FunctionService<TResult>::OnRegion( IRegion<TKey, TValue>^ rg )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          
          IRegion<TKey, TValue>^ regImpl = safe_cast<IRegion<TKey, TValue>^>( rg);

        apache::geode::client::RegionPtr regionptr(GetNativePtrFromSBWrapGeneric((Generic::Region<TKey, TValue>^)regImpl));

          apache::geode::client::ExecutionPtr& nativeptr( apache::geode::client::FunctionService::onRegion(
            regionptr ) );
          return Execution<TResult>::Create( nativeptr.ptr( ), nullptr );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic <class TResult>
      Execution<TResult>^ FunctionService<TResult>::OnServer( Pool/*<TKey, TValue>*/^ pl )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          apache::geode::client::PoolPtr poolptr(GetNativePtrFromSBWrapGeneric<apache::geode::client::Pool>( pl ) );

          apache::geode::client::ExecutionPtr& nativeptr( apache::geode::client::FunctionService::onServer(
            poolptr ) );
          return Execution<TResult>::Create( nativeptr.ptr( ) , nullptr);

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }
      
      generic <class TResult>
      Execution<TResult>^ FunctionService<TResult>::OnServers( Pool/*<TKey, TValue>*/^ pl )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          apache::geode::client::PoolPtr poolptr(GetNativePtrFromSBWrapGeneric<apache::geode::client::Pool>( pl ) );
          apache::geode::client::ExecutionPtr& nativeptr( apache::geode::client::FunctionService::onServers(
            poolptr ) );
          return Execution<TResult>::Create( nativeptr.ptr( ) , nullptr);

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TResult>
      Execution<TResult>^ FunctionService<TResult>::OnServer( IRegionService^ cache )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          Apache::Geode::Client::Generic::Cache^ realCache =
            dynamic_cast<Apache::Geode::Client::Generic::Cache^>(cache);

        if(realCache != nullptr)
        {
            apache::geode::client::RegionServicePtr cacheptr(GetNativePtr<apache::geode::client::RegionService>( realCache ) );

            apache::geode::client::ExecutionPtr& nativeptr( apache::geode::client::FunctionService::onServer(
              cacheptr ) );
            return Execution<TResult>::Create( nativeptr.ptr( ), nullptr );
        }
        else
        {
          Apache::Geode::Client::Generic::AuthenticatedCache^ authCache =
            dynamic_cast<Apache::Geode::Client::Generic::AuthenticatedCache^>(cache);
          apache::geode::client::RegionServicePtr cacheptr(GetNativePtr<apache::geode::client::RegionService>( authCache ) );

            apache::geode::client::ExecutionPtr& nativeptr( apache::geode::client::FunctionService::onServer(
              cacheptr ) );
            return Execution<TResult>::Create( nativeptr.ptr( ), nullptr );
        }


        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TResult>
      Execution<TResult>^ FunctionService<TResult>::OnServers( IRegionService^ cache )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          Apache::Geode::Client::Generic::Cache^ realCache =
          dynamic_cast<Apache::Geode::Client::Generic::Cache^>(cache);

          if(realCache != nullptr)
          {
            apache::geode::client::RegionServicePtr cacheptr(GetNativePtr<apache::geode::client::RegionService>( realCache ) );

            apache::geode::client::ExecutionPtr& nativeptr( apache::geode::client::FunctionService::onServers(
              cacheptr ) );
            return Execution<TResult>::Create( nativeptr.ptr( ), nullptr );
          }
          else
          {
            Apache::Geode::Client::Generic::AuthenticatedCache^ authCache =
              dynamic_cast<Apache::Geode::Client::Generic::AuthenticatedCache^>(cache);
            apache::geode::client::RegionServicePtr cacheptr(GetNativePtr<apache::geode::client::RegionService>( authCache ) );

            apache::geode::client::ExecutionPtr& nativeptr( apache::geode::client::FunctionService::onServers(
              cacheptr ) );
            return Execution<TResult>::Create( nativeptr.ptr( ) , nullptr);
          }

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }
    }
  }
}
 } //namespace 
