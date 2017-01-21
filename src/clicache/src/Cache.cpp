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

namespace Apache
{
  namespace Geode
  {
    namespace Client
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
        apache::geode::client::DistributedSystemPtr& nativeptr(
          NativePtr->getDistributedSystem( ) );

        return Apache::Geode::Client::DistributedSystem::Create(
          nativeptr.ptr( ) );
      }

      CacheTransactionManager^ Cache::CacheTransactionManager::get( )
      {
        apache::geode::client::InternalCacheTransactionManager2PCPtr& nativeptr = static_cast<InternalCacheTransactionManager2PCPtr>(
          NativePtr->getCacheTransactionManager( ) );

        return Apache::Geode::Client::CacheTransactionManager::Create(
          nativeptr.ptr( ) );
      }

      void Cache::Close( )
      {
        Close( false );
      }

      void Cache::Close( bool keepalive )
      {
        _GF_MG_EXCEPTION_TRY2

          Apache::Geode::Client::DistributedSystem::acquireDisconnectLock();

        Apache::Geode::Client::DistributedSystem::disconnectInstance();
          CacheFactory::m_connected = false;

          NativePtr->close( keepalive );

          // If DS automatically disconnected due to the new bootstrap API, then cleanup the C++/CLI side
          //if (!apache::geode::client::DistributedSystem::isConnected())
          {
            Apache::Geode::Client::DistributedSystem::UnregisterBuiltinManagedTypes();
          }

        _GF_MG_EXCEPTION_CATCH_ALL2
        finally
        {
					Apache::Geode::Client::Internal::PdxTypeRegistry::clear();
          Serializable::Clear();
          Apache::Geode::Client::DistributedSystem::releaseDisconnectLock();
          Apache::Geode::Client::DistributedSystem::unregisterCliCallback();
        }
      }

      void Cache::ReadyForEvents( )
      {
        _GF_MG_EXCEPTION_TRY2

          NativePtr->readyForEvents( );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      generic<class TKey, class TValue>
      Client::IRegion<TKey,TValue>^ Cache::GetRegion( String^ path )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          ManagedString mg_path( path );
          apache::geode::client::RegionPtr& nativeptr(
            NativePtr->getRegion( mg_path.CharPtr ) );

          return Client::Region<TKey,TValue>::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      array<Client::IRegion<TKey, TValue>^>^ Cache::RootRegions( )
      {
        apache::geode::client::VectorOfRegion vrr;
        NativePtr->rootRegions( vrr );
        array<Client::IRegion<TKey, TValue>^>^ rootRegions =
          gcnew array<Client::IRegion<TKey, TValue>^>( vrr.size( ) );

        for( int32_t index = 0; index < vrr.size( ); index++ )
        {
          apache::geode::client::RegionPtr& nativeptr( vrr[ index ] );
          rootRegions[ index ] = Client::Region<TKey, TValue>::Create( nativeptr.ptr( ) );
        }
        return rootRegions;
      }

      generic<class TKey, class TResult>
      Client::QueryService<TKey, TResult>^ Cache::GetQueryService( )
      {
        _GF_MG_EXCEPTION_TRY2

          return Client::QueryService<TKey, TResult>::Create( NativePtr->getQueryService( ).ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      generic<class TKey, class TResult>
      Client::QueryService<TKey, TResult>^ Cache::GetQueryService(String^ poolName )
      {
        _GF_MG_EXCEPTION_TRY2

          ManagedString mg_poolName( poolName );
          return QueryService<TKey, TResult>::Create( NativePtr->getQueryService(mg_poolName.CharPtr).ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      RegionFactory^ Cache::CreateRegionFactory(RegionShortcut preDefinedRegionAttributes)
      {
        _GF_MG_EXCEPTION_TRY2

          apache::geode::client::RegionShortcut preDefineRegionAttr = apache::geode::client::CACHING_PROXY;

          switch(preDefinedRegionAttributes)
          {
          case RegionShortcut::PROXY:
              preDefineRegionAttr = apache::geode::client::PROXY;
              break;
          case RegionShortcut::CACHING_PROXY:
              preDefineRegionAttr = apache::geode::client::CACHING_PROXY;
              break;
          case RegionShortcut::CACHING_PROXY_ENTRY_LRU:
              preDefineRegionAttr = apache::geode::client::CACHING_PROXY_ENTRY_LRU;
              break;
          case RegionShortcut::LOCAL:
              preDefineRegionAttr = apache::geode::client::LOCAL;
              break;
          case RegionShortcut::LOCAL_ENTRY_LRU:
              preDefineRegionAttr = apache::geode::client::LOCAL_ENTRY_LRU;
              break;          
          }

          return RegionFactory::Create(NativePtr->createRegionFactory(preDefineRegionAttr).ptr());
          
        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      IRegionService^ Cache::CreateAuthenticatedView(Properties<String^, Object^>^ credentials)
      {
        //  TODO:
				//TODO::split
        apache::geode::client::Properties* prop = NULL;

        if (credentials != nullptr)
          prop = GetNativePtr<apache::geode::client::Properties>( credentials );

        apache::geode::client::PropertiesPtr credPtr(prop);
        
        
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
        apache::geode::client::Properties* prop = NULL;

        if (credentials != nullptr)
          prop = GetNativePtr<apache::geode::client::Properties>( credentials );

        apache::geode::client::PropertiesPtr credPtr(prop);

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
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache

}
