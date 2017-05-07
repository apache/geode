/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "../gf_includes.hpp"
#include "../CacheM.hpp"
#include "../DistributedSystemM.hpp"
#include "../RegionM.hpp"
#include "../com/vmware/RegionMN.hpp"
#include "../RegionAttributesM.hpp"
#include "../QueryServiceM.hpp"
#include "../FunctionServiceM.hpp"
#include "../ExecutionM.hpp"
#include "AuthenticatedCacheM.hpp"

using namespace System;

using namespace GemStone::GemFire::Cache::Generic;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      bool AuthenticatedCache::IsClosed::get( )
      {
        return NativePtr->isClosed( );
      }

      void AuthenticatedCache::Close( )
      {
        _GF_MG_EXCEPTION_TRY

          NativePtr->close(  );

        _GF_MG_EXCEPTION_CATCH_ALL
      }
      
      Region^ AuthenticatedCache::GetRegion( String^ path )
      {
        _GF_MG_EXCEPTION_TRY

          ManagedString mg_path( path );
          gemfire::RegionPtr& nativeptr(
            NativePtr->getRegion( mg_path.CharPtr ) );

          return Region::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      QueryService^ AuthenticatedCache::GetQueryService( )
      {
        _GF_MG_EXCEPTION_TRY

          return QueryService::Create( NativePtr->getQueryService( ).ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL
      }
      
      array<Region^>^ AuthenticatedCache::RootRegions( )
      {
        gemfire::VectorOfRegion vrr;
        NativePtr->rootRegions( vrr );
        array<Region^>^ rootRegions =
          gcnew array<Region^>( vrr.size( ) );

        for( int32_t index = 0; index < vrr.size( ); index++ )
        {
          gemfire::RegionPtr& nativeptr( vrr[ index ] );
          rootRegions[ index ] = Region::Create( nativeptr.ptr( ) );
        }
        return rootRegions;
      }
    }
  }
}
