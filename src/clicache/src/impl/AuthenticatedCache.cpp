/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "../gf_includes.hpp"
#include "../Cache.hpp"
#include "../DistributedSystem.hpp"
#include "../Region.hpp"
#include "../RegionAttributes.hpp"
#include "../QueryService.hpp"
#include "../FunctionService.hpp"
#include "../Execution.hpp"
#include "AuthenticatedCache.hpp"
#include "PdxInstanceFactoryImpl.hpp"
using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Generic
      {
      bool AuthenticatedCache::IsClosed::get( )
      {
        return NativePtr->isClosed( );
      }

      void AuthenticatedCache::Close( )
      {
        _GF_MG_EXCEPTION_TRY2

          NativePtr->close(  );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }
      
			//TODO::split
      generic<class TKey, class TValue>
      IRegion<TKey, TValue>^ AuthenticatedCache::GetRegion( String^ path )
      {
        _GF_MG_EXCEPTION_TRY2

          ManagedString mg_path( path );
          gemfire::RegionPtr& nativeptr(
            NativePtr->getRegion( mg_path.CharPtr ) );

          return Generic::Region<TKey, TValue>::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }
      
      generic<class TKey, class TResult>
      Generic::QueryService<TKey, TResult>^ AuthenticatedCache::GetQueryService( )
      {
        _GF_MG_EXCEPTION_TRY2

          return Generic::QueryService<TKey, TResult>::Create( NativePtr->getQueryService( ).ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      generic<class TKey, class TValue>
      array<IRegion<TKey, TValue>^>^ AuthenticatedCache::RootRegions( )
      {
        gemfire::VectorOfRegion vrr;
        NativePtr->rootRegions( vrr );
        array<IRegion<TKey, TValue>^>^ rootRegions =
          gcnew array<IRegion<TKey, TValue>^>( vrr.size( ) );

        for( int32_t index = 0; index < vrr.size( ); index++ )
        {
          gemfire::RegionPtr& nativeptr( vrr[ index ] );
          rootRegions[ index ] = Generic::Region<TKey, TValue>::Create( nativeptr.ptr( ) );
        }
        return rootRegions;
      }

       IPdxInstanceFactory^ AuthenticatedCache::CreatePdxInstanceFactory(String^ className)
        {
          return gcnew Internal::PdxInstanceFactoryImpl(className);;
        }

      } // end namespace Generic
    }
  }
}
