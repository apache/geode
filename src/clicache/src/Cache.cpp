/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "gf_includes.hpp"
#include "Cache.hpp"
#include "ExceptionTypes.hpp"
#include "DistributedSystem.hpp"
#include "Region.hpp"
#include "RegionAttributes.hpp"
#include "QueryService.hpp"
//#include "FunctionService.hpp"
//#include "Execution.hpp"
#include "CacheFactory.hpp"
#include "impl/AuthenticatedCache.hpp"
#include "impl/ManagedString.hpp"
#include "impl/SafeConvert.hpp"
#include "impl/PdxTypeRegistry.hpp"
#include "impl/PdxInstanceFactoryImpl.hpp"

#pragma warning(disable:4091)

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Generic
      {
     

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

        return GemStone::GemFire::Cache::Generic::DistributedSystem::Create(
          nativeptr.ptr( ) );
      }

      CacheTransactionManager^ Cache::CacheTransactionManager::get( )
      {
        gemfire::InternalCacheTransactionManager2PCPtr& nativeptr = static_cast<InternalCacheTransactionManager2PCPtr>(
          NativePtr->getCacheTransactionManager( ) );

        return GemStone::GemFire::Cache::Generic::CacheTransactionManager::Create(
          nativeptr.ptr( ) );
      }

      void Cache::Close( )
      {
        Close( false );
      }

      void Cache::Close( bool keepalive )
      {
        _GF_MG_EXCEPTION_TRY2

          GemStone::GemFire::Cache::Generic::DistributedSystem::acquireDisconnectLock();

        GemStone::GemFire::Cache::Generic::DistributedSystem::disconnectInstance();
          CacheFactory::m_connected = false;

          NativePtr->close( keepalive );

          // If DS automatically disconnected due to the new bootstrap API, then cleanup the C++/CLI side
          //if (!gemfire::DistributedSystem::isConnected())
          {
            GemStone::GemFire::Cache::Generic::DistributedSystem::UnregisterBuiltinManagedTypes();
          }

        _GF_MG_EXCEPTION_CATCH_ALL2
        finally
        {
					GemStone::GemFire::Cache::Generic::Internal::PdxTypeRegistry::clear();
          Serializable::Clear();
          GemStone::GemFire::Cache::Generic::DistributedSystem::releaseDisconnectLock();
          GemStone::GemFire::Cache::Generic::DistributedSystem::unregisterCliCallback();
        }
      }

      void Cache::ReadyForEvents( )
      {
        _GF_MG_EXCEPTION_TRY2

          NativePtr->readyForEvents( );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      generic<class TKey, class TValue>
      Generic::IRegion<TKey,TValue>^ Cache::GetRegion( String^ path )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          ManagedString mg_path( path );
          gemfire::RegionPtr& nativeptr(
            NativePtr->getRegion( mg_path.CharPtr ) );

          return Generic::Region<TKey,TValue>::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      array<Generic::IRegion<TKey, TValue>^>^ Cache::RootRegions( )
      {
        gemfire::VectorOfRegion vrr;
        NativePtr->rootRegions( vrr );
        array<Generic::IRegion<TKey, TValue>^>^ rootRegions =
          gcnew array<Generic::IRegion<TKey, TValue>^>( vrr.size( ) );

        for( int32_t index = 0; index < vrr.size( ); index++ )
        {
          gemfire::RegionPtr& nativeptr( vrr[ index ] );
          rootRegions[ index ] = Generic::Region<TKey, TValue>::Create( nativeptr.ptr( ) );
        }
        return rootRegions;
      }

      generic<class TKey, class TResult>
      Generic::QueryService<TKey, TResult>^ Cache::GetQueryService( )
      {
        _GF_MG_EXCEPTION_TRY2

          return Generic::QueryService<TKey, TResult>::Create( NativePtr->getQueryService( ).ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      generic<class TKey, class TResult>
      Generic::QueryService<TKey, TResult>^ Cache::GetQueryService(String^ poolName )
      {
        _GF_MG_EXCEPTION_TRY2

          ManagedString mg_poolName( poolName );
          return QueryService<TKey, TResult>::Create( NativePtr->getQueryService(mg_poolName.CharPtr).ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      RegionFactory^ Cache::CreateRegionFactory(RegionShortcut preDefinedRegionAttributes)
      {
        _GF_MG_EXCEPTION_TRY2

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
          
        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      IRegionService^ Cache::CreateAuthenticatedView(Properties<String^, Object^>^ credentials)
      {
        //  TODO:
				//TODO::split
        gemfire::Properties* prop = NULL;

        if (credentials != nullptr)
          prop = GetNativePtr<gemfire::Properties>( credentials );

        gemfire::PropertiesPtr credPtr(prop);
        
        
        _GF_MG_EXCEPTION_TRY2

          return AuthenticatedCache::Create( (NativePtr->createAuthenticatedView(credPtr)).ptr());

        _GF_MG_EXCEPTION_CATCH_ALL2   
      }

			bool Cache::GetPdxIgnoreUnreadFields()
			{
				_GF_MG_EXCEPTION_TRY2

					return	NativePtr->getPdxIgnoreUnreadFields();

				_GF_MG_EXCEPTION_CATCH_ALL2   
			}

      bool Cache::GetPdxReadSerialized()
			{
				_GF_MG_EXCEPTION_TRY2

					return	NativePtr->getPdxReadSerialized();

				_GF_MG_EXCEPTION_CATCH_ALL2   
			}

      IRegionService^ Cache::CreateAuthenticatedView(Properties<String^, Object^>^ credentials, String^ poolName)
      {
         // TODO:
				//TODO::split
        gemfire::Properties* prop = NULL;

        if (credentials != nullptr)
          prop = GetNativePtr<gemfire::Properties>( credentials );

        gemfire::PropertiesPtr credPtr(prop);

        ManagedString mg_poolName( poolName );
        
        
        _GF_MG_EXCEPTION_TRY2

          return AuthenticatedCache::Create( (NativePtr->createAuthenticatedView(credPtr, mg_poolName.CharPtr)).ptr());

        _GF_MG_EXCEPTION_CATCH_ALL2   
      }

			 void Cache::InitializeDeclarativeCache( String^ cacheXml )
      {
        ManagedString mg_cacheXml( cacheXml );
        NativePtr->initializeDeclarativeCache( mg_cacheXml.CharPtr );
      }

        IPdxInstanceFactory^ Cache::CreatePdxInstanceFactory(String^ className)
        {
          return gcnew Internal::PdxInstanceFactoryImpl(className);
        }
      } // end namespace Generic
    }
  }
}
