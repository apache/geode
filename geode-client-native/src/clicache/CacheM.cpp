/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "CacheM.hpp"
#include "DistributedSystemM.hpp"
#include "RegionM.hpp"
#include "RegionAttributesM.hpp"
#include "QueryServiceM.hpp"
#include "FunctionServiceM.hpp"
#include "ExecutionM.hpp"
#include "CacheFactoryM.hpp"
#include "impl/AuthenticatedCacheM.hpp"

//#include "impl/DistributedSystemImpl.hpp"

#pragma warning(disable:4091)

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      void Cache::InitializeDeclarativeCache( String^ cacheXml )
      {
        ManagedString mg_cacheXml( cacheXml );
        NativePtr->initializeDeclarativeCache( mg_cacheXml.CharPtr );
      }

      String^ Cache::Name::get( )
      {
        return ManagedString::Get( NativePtr->getName( ) );
      }

      bool Cache::IsClosed::get( )
      {
        return NativePtr->isClosed( );
      }

      DistributedSystem^ Cache::DistributedSystem::get( )
      {
        gemfire::DistributedSystemPtr& nativeptr(
          NativePtr->getDistributedSystem( ) );

        return GemStone::GemFire::Cache::DistributedSystem::Create(
          nativeptr.ptr( ) );
      }

      void Cache::Close( )
      {
        Close( false );
      }

      void Cache::Close( bool keepalive )
      {
        _GF_MG_EXCEPTION_TRY

          GemStone::GemFire::Cache::DistributedSystem::acquireDisconnectLock();

          GemStone::GemFire::Cache::DistributedSystem::disconnectInstance();
          GemStone::GemFire::Cache::CacheFactory::m_connected = false;

          NativePtr->close( keepalive );

          // If DS automatically disconnected due to the new bootstrap API, then cleanup the C++/CLI side
          //if (!gemfire::DistributedSystem::isConnected())
          {
            GemStone::GemFire::Cache::DistributedSystem::UnregisterBuiltinManagedTypes();
          }

        _GF_MG_EXCEPTION_CATCH_ALL
        finally
        {
          GemStone::GemFire::Cache::DistributedSystem::releaseDisconnectLock();
        }
      }

      void Cache::ReadyForEvents( )
      {
        _GF_MG_EXCEPTION_TRY

          NativePtr->readyForEvents( );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      Region^ Cache::CreateRegion( String^ name, RegionAttributes^ attributes )
      {
        _GF_MG_EXCEPTION_TRY

          ManagedString mg_name( name );
          gemfire::RegionAttributesPtr regionAttribsPtr(
            GetNativePtr<gemfire::RegionAttributes>( attributes ) );

          gemfire::RegionPtr& nativeptr( NativePtr->createRegion(
            mg_name.CharPtr, regionAttribsPtr ) );
          return Region::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      Region^ Cache::GetRegion( String^ path )
      {
        _GF_MG_EXCEPTION_TRY

          ManagedString mg_path( path );
          gemfire::RegionPtr& nativeptr(
            NativePtr->getRegion( mg_path.CharPtr ) );

          return Region::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      QueryService^ Cache::GetQueryService( )
      {
        _GF_MG_EXCEPTION_TRY

          return QueryService::Create( NativePtr->getQueryService( ).ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL
      }
      
      QueryService^ Cache::GetQueryService(String^ poolName )
      {
        _GF_MG_EXCEPTION_TRY

          ManagedString mg_poolName( poolName );
          return QueryService::Create( NativePtr->getQueryService(mg_poolName.CharPtr).ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      RegionFactory^ Cache::CreateRegionFactory(RegionShortcut preDefinedRegionAttributes)
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::RegionShortcut preDefineRegionAttr = gemfire::CACHING_PROXY;

          switch(preDefinedRegionAttributes)
          {
          case RegionShortcut::PROXY:
              preDefineRegionAttr = gemfire::PROXY;
              break;
          case RegionShortcut::CACHING_PROXY:
              preDefineRegionAttr = gemfire::CACHING_PROXY;
              break;
          case RegionShortcut::CACHING_PROXY_ENTRY_LRU:
              preDefineRegionAttr = gemfire::CACHING_PROXY_ENTRY_LRU;
              break;
          case RegionShortcut::LOCAL:
              preDefineRegionAttr = gemfire::LOCAL;
              break;
          case RegionShortcut::LOCAL_ENTRY_LRU:
              preDefineRegionAttr = gemfire::LOCAL_ENTRY_LRU;
              break;          
          }

          return RegionFactory::Create(NativePtr->createRegionFactory(preDefineRegionAttr).ptr());
          
        _GF_MG_EXCEPTION_CATCH_ALL
      }

      IRegionService^ Cache::CreateAuthenticatedView(Properties^ credentials)
      {
        gemfire::Properties* prop = NULL;

        if (credentials != nullptr)
          prop = GetNativePtr<gemfire::Properties>( credentials );

        gemfire::PropertiesPtr credPtr(prop);
        
        _GF_MG_EXCEPTION_TRY

          return AuthenticatedCache::Create( (NativePtr->createAuthenticatedView(credPtr)).ptr());

        _GF_MG_EXCEPTION_CATCH_ALL   
      }

      IRegionService^ Cache::CreateAuthenticatedView(Properties^ credentials, String^ poolName)
      {
        gemfire::Properties* prop = NULL;

        if (credentials != nullptr)
          prop = GetNativePtr<gemfire::Properties>( credentials );

        gemfire::PropertiesPtr credPtr(prop);

        ManagedString mg_poolName( poolName );
        
        _GF_MG_EXCEPTION_TRY

          return AuthenticatedCache::Create( (NativePtr->createAuthenticatedView(credPtr, mg_poolName.CharPtr)).ptr());

        _GF_MG_EXCEPTION_CATCH_ALL   
      }

			array<Region^>^ Cache::RootRegions( )
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
