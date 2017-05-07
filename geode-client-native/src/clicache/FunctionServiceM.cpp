/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "FunctionServiceM.hpp"
#include "cppcache/FunctionService.hpp"
#include "PoolM.hpp"
#include "RegionM.hpp"
#include "ExecutionM.hpp"
#include "cppcache/RegionService.hpp"
#include "impl/AuthenticatedCacheM.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      Execution^ FunctionService::OnRegion( Region^ rg )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::RegionPtr regionptr(
            GetNativePtr<gemfire::Region>( rg ) );

          gemfire::ExecutionPtr& nativeptr( gemfire::FunctionService::onRegion(
            regionptr ) );
          return Execution::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL
      }
      Execution^ FunctionService::OnServer( Pool^ pl )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::PoolPtr poolptr(GetNativePtr<gemfire::Pool>( pl ) );

          gemfire::ExecutionPtr& nativeptr( gemfire::FunctionService::onServer(
            poolptr ) );
          return Execution::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      Execution^ FunctionService::OnServers( Pool^ pl )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::PoolPtr poolptr(GetNativePtr<gemfire::Pool>( pl ) );
          gemfire::ExecutionPtr& nativeptr( gemfire::FunctionService::onServers(
            poolptr ) );
          return Execution::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      Execution^ FunctionService::OnServer( IRegionService^ cache )
      {
        _GF_MG_EXCEPTION_TRY

          Cache^ realCache = dynamic_cast<Cache^>(cache);

        if(realCache != nullptr)
        {
            gemfire::RegionServicePtr cacheptr(GetNativePtr<gemfire::RegionService>( realCache ) );

            gemfire::ExecutionPtr& nativeptr( gemfire::FunctionService::onServer(
              cacheptr ) );
            return Execution::Create( nativeptr.ptr( ) );
        }
        else
        {
          AuthenticatedCache^ authCache = dynamic_cast<AuthenticatedCache^>(cache);
          gemfire::RegionServicePtr cacheptr(GetNativePtr<gemfire::RegionService>( authCache ) );

            gemfire::ExecutionPtr& nativeptr( gemfire::FunctionService::onServer(
              cacheptr ) );
            return Execution::Create( nativeptr.ptr( ) );
        }


        _GF_MG_EXCEPTION_CATCH_ALL
      }

      Execution^ FunctionService::OnServers( IRegionService^ cache )
      {
        _GF_MG_EXCEPTION_TRY

          Cache^ realCache = dynamic_cast<Cache^>(cache);

          if(realCache != nullptr)
          {
            gemfire::RegionServicePtr cacheptr(GetNativePtr<gemfire::RegionService>( realCache ) );

            gemfire::ExecutionPtr& nativeptr( gemfire::FunctionService::onServers(
              cacheptr ) );
            return Execution::Create( nativeptr.ptr( ) );
          }
          else
          {
            AuthenticatedCache^ authCache = dynamic_cast<AuthenticatedCache^>(cache);
            gemfire::RegionServicePtr cacheptr(GetNativePtr<gemfire::RegionService>( authCache ) );

            gemfire::ExecutionPtr& nativeptr( gemfire::FunctionService::onServers(
              cacheptr ) );
            return Execution::Create( nativeptr.ptr( ) );
          }

        _GF_MG_EXCEPTION_CATCH_ALL
      }
    }
  }
}
