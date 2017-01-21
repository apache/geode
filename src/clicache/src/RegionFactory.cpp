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

#pragma once

//#include "gf_includes.hpp"
#include "RegionFactory.hpp"
//#include "AttributesFactory.hpp"
#include "RegionAttributes.hpp"
#include "impl/SafeConvert.hpp"

#include "impl/ManagedCacheLoader.hpp"
#include "impl/ManagedCacheWriter.hpp"
#include "impl/ManagedCacheListener.hpp"
#include "impl/ManagedPartitionResolver.hpp"
#include "impl/ManagedFixedPartitionResolver.hpp"
#include "impl/ManagedFixedPartitionResolver.hpp"
#include "impl/ManagedPersistenceManager.hpp"

#include "impl/CacheLoader.hpp"
#include "impl/CacheWriter.hpp"
#include "impl/CacheListener.hpp"
#include "impl/PartitionResolver.hpp"
#include "impl/FixedPartitionResolver.hpp"
#include "impl/FixedPartitionResolver.hpp"
#include "impl/PersistenceManagerProxy.hpp"

using namespace System;
using namespace System::Collections::Generic;


namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      RegionFactory^ RegionFactory::SetCacheLoader( String^ libPath, String^ factoryFunctionName )
      {
        throw gcnew System::NotSupportedException;
        ManagedString mg_libpath( libPath );
        ManagedString mg_factoryFunctionName( factoryFunctionName );

        NativePtr->setCacheLoader( mg_libpath.CharPtr,
          mg_factoryFunctionName.CharPtr );
        return this;
      }

      RegionFactory^ RegionFactory::SetCacheWriter( String^ libPath, String^ factoryFunctionName )
      {
        throw gcnew System::NotSupportedException;
        ManagedString mg_libpath( libPath );
        ManagedString mg_factoryFunctionName( factoryFunctionName );

        NativePtr->setCacheWriter( mg_libpath.CharPtr,
          mg_factoryFunctionName.CharPtr );
        return this;
      }

      RegionFactory^ RegionFactory::SetCacheListener( String^ libPath, String^ factoryFunctionName )
      {
        throw gcnew System::NotSupportedException;
        ManagedString mg_libpath( libPath );
        ManagedString mg_factoryFunctionName( factoryFunctionName );

        NativePtr->setCacheListener( mg_libpath.CharPtr,
          mg_factoryFunctionName.CharPtr );
        return this;
      }

      RegionFactory^ RegionFactory::SetPartitionResolver( String^ libPath, String^ factoryFunctionName )
      {
        throw gcnew System::NotSupportedException;
        ManagedString mg_libpath( libPath );
        ManagedString mg_factoryFunctionName( factoryFunctionName );

        NativePtr->setPartitionResolver( mg_libpath.CharPtr,
          mg_factoryFunctionName.CharPtr );
        return this;
      }

      // EXPIRATION ATTRIBUTES

      RegionFactory^ RegionFactory::SetEntryIdleTimeout( ExpirationAction action, uint32_t idleTimeout )
      {
        NativePtr->setEntryIdleTimeout(
          static_cast<apache::geode::client::ExpirationAction::Action>( action ), idleTimeout );
        return this;
      }

      RegionFactory^ RegionFactory::SetEntryTimeToLive( ExpirationAction action, uint32_t timeToLive )
      {
        NativePtr->setEntryTimeToLive(
          static_cast<apache::geode::client::ExpirationAction::Action>( action ), timeToLive );
        return this;
      }

      RegionFactory^ RegionFactory::SetRegionIdleTimeout( ExpirationAction action, uint32_t idleTimeout )
      {
        NativePtr->setRegionIdleTimeout(
          static_cast<apache::geode::client::ExpirationAction::Action>( action ), idleTimeout );
        return this;
      }

      RegionFactory^ RegionFactory::SetRegionTimeToLive( ExpirationAction action, uint32_t timeToLive )
      {
        NativePtr->setRegionTimeToLive(
          static_cast<apache::geode::client::ExpirationAction::Action>( action ), timeToLive );
        return this;
      }

      // PERSISTENCE

       generic <class TKey, class TValue>
      RegionFactory^ RegionFactory::SetPersistenceManager( Client::IPersistenceManager<TKey, TValue>^ persistenceManager, 
          Properties<String^, String^>^ config)
      {
        apache::geode::client::PersistenceManagerPtr persistenceManagerptr;
        if ( persistenceManager != nullptr ) {
          PersistenceManagerGeneric<TKey, TValue>^ clg = gcnew PersistenceManagerGeneric<TKey, TValue>();
          clg->SetPersistenceManager(persistenceManager);
          persistenceManagerptr = new apache::geode::client::ManagedPersistenceManagerGeneric( /*clg,*/ persistenceManager);
          ((apache::geode::client::ManagedPersistenceManagerGeneric*)persistenceManagerptr.ptr())->setptr(clg);
        }
        apache::geode::client::PropertiesPtr configptr(GetNativePtr<apache::geode::client::Properties>( config ) );
        NativePtr->setPersistenceManager( persistenceManagerptr, configptr );
        return this;
      }

      generic <class TKey, class TValue>
      RegionFactory^ RegionFactory::SetPersistenceManager( Client::IPersistenceManager<TKey, TValue>^ persistenceManager )
      {
        return SetPersistenceManager(persistenceManager, nullptr);
      }

      RegionFactory^ RegionFactory::SetPersistenceManager( String^ libPath,
        String^ factoryFunctionName )
      {        
        SetPersistenceManager( libPath, factoryFunctionName, nullptr );
        return this;
      }

      RegionFactory^ RegionFactory::SetPersistenceManager( String^ libPath,
        String^ factoryFunctionName, /*Dictionary<Object^, Object^>*/Properties<String^, String^>^ config )
      {        
        ManagedString mg_libpath( libPath );
        ManagedString mg_factoryFunctionName( factoryFunctionName );
				//TODO:split
        apache::geode::client::PropertiesPtr configptr(
          GetNativePtr<apache::geode::client::Properties>( config ) );

        NativePtr->setPersistenceManager( mg_libpath.CharPtr,
          mg_factoryFunctionName.CharPtr, configptr );
        return this;
      }

      RegionFactory^ RegionFactory::SetPoolName( String^ poolName )
      {
        ManagedString mg_poolName( poolName );

        NativePtr->setPoolName( mg_poolName.CharPtr );
        return this;
      }

      // MAP ATTRIBUTES

      RegionFactory^ RegionFactory::SetInitialCapacity( int32_t initialCapacity )
      {
        _GF_MG_EXCEPTION_TRY2

          NativePtr->setInitialCapacity( initialCapacity );
          return this;

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      RegionFactory^ RegionFactory::SetLoadFactor( Single loadFactor )
      {
        _GF_MG_EXCEPTION_TRY2

          NativePtr->setLoadFactor( loadFactor );
          return this;

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      RegionFactory^ RegionFactory::SetConcurrencyLevel( int32_t concurrencyLevel )
      {
        _GF_MG_EXCEPTION_TRY2

          NativePtr->setConcurrencyLevel( concurrencyLevel );
          return this;

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      RegionFactory^ RegionFactory::SetLruEntriesLimit( uint32_t entriesLimit )
      {
        NativePtr->setLruEntriesLimit( entriesLimit );
        return this;
      }

      RegionFactory^ RegionFactory::SetDiskPolicy( DiskPolicyType diskPolicy )
      {
        NativePtr->setDiskPolicy(
          static_cast<apache::geode::client::DiskPolicyType::PolicyType>( diskPolicy ) );
        return this;
      }

      RegionFactory^ RegionFactory::SetCachingEnabled( bool cachingEnabled )
      {
        NativePtr->setCachingEnabled( cachingEnabled );
        return this;
      }

      RegionFactory^ RegionFactory::SetCloningEnabled( bool cloningEnabled )
      {
        NativePtr->setCloningEnabled( cloningEnabled );
        return this;
      }

      RegionFactory^ RegionFactory::SetConcurrencyChecksEnabled( bool concurrencyChecksEnabled )
      {
        NativePtr->setConcurrencyChecksEnabled( concurrencyChecksEnabled );
        return this;
      }
      // NEW GENERIC APIs:

      generic <class TKey, class TValue>
      IRegion<TKey,TValue>^ RegionFactory::Create(String^ regionName)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

        ManagedString mg_name( regionName );
          
        apache::geode::client::RegionPtr& nativeptr( NativePtr->create(
            mg_name.CharPtr ) );
          return Client::Region<TKey,TValue>::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic <class TKey, class TValue>
      RegionFactory^ RegionFactory::SetCacheLoader( Client::ICacheLoader<TKey, TValue>^ cacheLoader )
      {
        apache::geode::client::CacheLoaderPtr loaderptr;
        if ( cacheLoader != nullptr ) {
          CacheLoaderGeneric<TKey, TValue>^ clg = gcnew CacheLoaderGeneric<TKey, TValue>();
          clg->SetCacheLoader(cacheLoader);
          loaderptr = new apache::geode::client::ManagedCacheLoaderGeneric( /*clg,*/ cacheLoader );
          ((apache::geode::client::ManagedCacheLoaderGeneric*)loaderptr.ptr())->setptr(clg);
        }
        NativePtr->setCacheLoader( loaderptr );
        return this;
      }

      generic <class TKey, class TValue>
      RegionFactory^ RegionFactory::SetCacheWriter( Client::ICacheWriter<TKey, TValue>^ cacheWriter )
      {
        apache::geode::client::CacheWriterPtr writerptr;
        if ( cacheWriter != nullptr ) {
          CacheWriterGeneric<TKey, TValue>^ cwg = gcnew CacheWriterGeneric<TKey, TValue>();
          cwg->SetCacheWriter(cacheWriter);
          writerptr = new apache::geode::client::ManagedCacheWriterGeneric( /*cwg,*/ cacheWriter );
          ((apache::geode::client::ManagedCacheWriterGeneric*)writerptr.ptr())->setptr(cwg);
        }
        NativePtr->setCacheWriter( writerptr );
        return this;
      }

      generic <class TKey, class TValue>
      RegionFactory^ RegionFactory::SetCacheListener( Client::ICacheListener<TKey, TValue>^ cacheListener )
      {
        apache::geode::client::CacheListenerPtr listenerptr;
        if ( cacheListener != nullptr ) {
          CacheListenerGeneric<TKey, TValue>^ clg = gcnew CacheListenerGeneric<TKey, TValue>();
          clg->SetCacheListener(cacheListener);
          listenerptr = new apache::geode::client::ManagedCacheListenerGeneric( /*clg,*/ cacheListener );
          ((apache::geode::client::ManagedCacheListenerGeneric*)listenerptr.ptr())->setptr(clg);
          /*
          listenerptr = new apache::geode::client::ManagedCacheListenerGeneric(
            (Client::ICacheListener<Object^, Object^>^)cacheListener);
            */
        }
        NativePtr->setCacheListener( listenerptr );
        return this;
      }

      generic <class TKey, class TValue>
      RegionFactory^ RegionFactory::SetPartitionResolver( Client::IPartitionResolver<TKey, TValue>^ partitionresolver )
      {
        apache::geode::client::PartitionResolverPtr resolverptr;
        if ( partitionresolver != nullptr ) {
          Client::IFixedPartitionResolver<TKey, TValue>^ resolver = 
            dynamic_cast<Client::IFixedPartitionResolver<TKey, TValue>^>(partitionresolver);
          if (resolver != nullptr) {            
            FixedPartitionResolverGeneric<TKey, TValue>^ prg = gcnew FixedPartitionResolverGeneric<TKey, TValue>();
            prg->SetPartitionResolver(resolver);
            resolverptr = new apache::geode::client::ManagedFixedPartitionResolverGeneric( resolver ); 
            ((apache::geode::client::ManagedFixedPartitionResolverGeneric*)resolverptr.ptr())->setptr(prg);
          }
          else {            
            PartitionResolverGeneric<TKey, TValue>^ prg = gcnew PartitionResolverGeneric<TKey, TValue>();
            prg->SetPartitionResolver(partitionresolver);
            resolverptr = new apache::geode::client::ManagedPartitionResolverGeneric( partitionresolver );
            ((apache::geode::client::ManagedPartitionResolverGeneric*)resolverptr.ptr())->setptr(prg);            
          }         
        }
        NativePtr->setPartitionResolver( resolverptr );
        return this;
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache

}
