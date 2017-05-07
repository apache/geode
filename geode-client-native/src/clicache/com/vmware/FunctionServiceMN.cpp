/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "gf_includesN.hpp"
#include "FunctionServiceMN.hpp"
#include "PoolMN.hpp"
#include "RegionMN.hpp"
#include "ExecutionMN.hpp"
#include <cppcache/RegionService.hpp>
#include "impl/AuthenticatedCacheMN.hpp"
#include "impl/SafeConvertN.hpp"
using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      generic <class TResult>
      generic <class TKey, class TValue>
      Execution<TResult>^ FunctionService<TResult>::OnRegion( IRegion<TKey, TValue>^ rg )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          
          IRegion<TKey, TValue>^ regImpl = safe_cast<IRegion<TKey, TValue>^>( rg);

        gemfire::RegionPtr regionptr(GetNativePtrFromSBWrapGeneric((Generic::Region<TKey, TValue>^)regImpl));

          gemfire::ExecutionPtr& nativeptr( gemfire::FunctionService::onRegion(
            regionptr ) );
          return Execution<TResult>::Create( nativeptr.ptr( ), nullptr );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic <class TResult>
      Execution<TResult>^ FunctionService<TResult>::OnServer( Pool/*<TKey, TValue>*/^ pl )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::PoolPtr poolptr(GetNativePtrFromSBWrapGeneric<gemfire::Pool>( pl ) );

          gemfire::ExecutionPtr& nativeptr( gemfire::FunctionService::onServer(
            poolptr ) );
          return Execution<TResult>::Create( nativeptr.ptr( ) , nullptr);

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }
      
      generic <class TResult>
      Execution<TResult>^ FunctionService<TResult>::OnServers( Pool/*<TKey, TValue>*/^ pl )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::PoolPtr poolptr(GetNativePtrFromSBWrapGeneric<gemfire::Pool>( pl ) );
          gemfire::ExecutionPtr& nativeptr( gemfire::FunctionService::onServers(
            poolptr ) );
          return Execution<TResult>::Create( nativeptr.ptr( ) , nullptr);

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TResult>
      Execution<TResult>^ FunctionService<TResult>::OnServer( IRegionService^ cache )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          GemStone::GemFire::Cache::Generic::Cache^ realCache =
            dynamic_cast<GemStone::GemFire::Cache::Generic::Cache^>(cache);

        if(realCache != nullptr)
        {
            gemfire::RegionServicePtr cacheptr(GetNativePtr<gemfire::RegionService>( realCache ) );

            gemfire::ExecutionPtr& nativeptr( gemfire::FunctionService::onServer(
              cacheptr ) );
            return Execution<TResult>::Create( nativeptr.ptr( ), nullptr );
        }
        else
        {
          GemStone::GemFire::Cache::Generic::AuthenticatedCache^ authCache =
            dynamic_cast<GemStone::GemFire::Cache::Generic::AuthenticatedCache^>(cache);
          gemfire::RegionServicePtr cacheptr(GetNativePtr<gemfire::RegionService>( authCache ) );

            gemfire::ExecutionPtr& nativeptr( gemfire::FunctionService::onServer(
              cacheptr ) );
            return Execution<TResult>::Create( nativeptr.ptr( ), nullptr );
        }


        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TResult>
      Execution<TResult>^ FunctionService<TResult>::OnServers( IRegionService^ cache )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          GemStone::GemFire::Cache::Generic::Cache^ realCache =
          dynamic_cast<GemStone::GemFire::Cache::Generic::Cache^>(cache);

          if(realCache != nullptr)
          {
            gemfire::RegionServicePtr cacheptr(GetNativePtr<gemfire::RegionService>( realCache ) );

            gemfire::ExecutionPtr& nativeptr( gemfire::FunctionService::onServers(
              cacheptr ) );
            return Execution<TResult>::Create( nativeptr.ptr( ), nullptr );
          }
          else
          {
            GemStone::GemFire::Cache::Generic::AuthenticatedCache^ authCache =
              dynamic_cast<GemStone::GemFire::Cache::Generic::AuthenticatedCache^>(cache);
            gemfire::RegionServicePtr cacheptr(GetNativePtr<gemfire::RegionService>( authCache ) );

            gemfire::ExecutionPtr& nativeptr( gemfire::FunctionService::onServers(
              cacheptr ) );
            return Execution<TResult>::Create( nativeptr.ptr( ) , nullptr);
          }

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }
    }
  }
}
 } //namespace 
