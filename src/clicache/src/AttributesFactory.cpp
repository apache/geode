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
#include "AttributesFactory.hpp"
#include "Region.hpp"
#include "impl/ManagedCacheLoader.hpp"
#include "impl/ManagedPersistenceManager.hpp"
#include "impl/ManagedCacheWriter.hpp"
#include "impl/ManagedCacheListener.hpp"
#include "impl/ManagedPartitionResolver.hpp"
#include "impl/ManagedFixedPartitionResolver.hpp"
#include "impl/CacheLoader.hpp"
#include "impl/CacheWriter.hpp"
#include "impl/CacheListener.hpp"
#include "impl/PartitionResolver.hpp"
#include "impl/PersistenceManagerProxy.hpp"
#include "RegionAttributes.hpp"
#include "ICacheLoader.hpp"
#include "IPersistenceManager.hpp"
#include "ICacheWriter.hpp"
#include "IPartitionResolver.hpp"
#include "IFixedPartitionResolver.hpp"
#include "impl/SafeConvert.hpp"
#include "ExceptionTypes.hpp"

using namespace System;
using namespace System::Collections::Generic;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {
namespace Generic
    {
      generic<class TKey, class TValue>
      AttributesFactory<TKey, TValue>::AttributesFactory( Apache::Geode::Client::Generic::RegionAttributes<TKey, TValue>^ regionAttributes )
        : UMWrap( )
      {
        apache::geode::client::RegionAttributesPtr attribptr(
          GetNativePtrFromSBWrapGeneric<apache::geode::client::RegionAttributes>( regionAttributes ) );
        SetPtr( new apache::geode::client::AttributesFactory( attribptr ), true );
      }

      // CALLBACKS

      generic<class TKey, class TValue>
      void AttributesFactory<TKey, TValue>::SetCacheLoader( ICacheLoader<TKey, TValue>^ cacheLoader )
      {
        apache::geode::client::CacheLoaderPtr loaderptr;
        if ( cacheLoader != nullptr ) {
          CacheLoaderGeneric<TKey, TValue>^ clg = gcnew CacheLoaderGeneric<TKey, TValue>();
          clg->SetCacheLoader(cacheLoader);
          loaderptr = new apache::geode::client::ManagedCacheLoaderGeneric( /*clg,*/ cacheLoader );
          ((apache::geode::client::ManagedCacheLoaderGeneric*)loaderptr.ptr())->setptr(clg);
        }
        NativePtr->setCacheLoader( loaderptr );
      }

      generic<class TKey, class TValue>
      void AttributesFactory<TKey, TValue>::SetCacheWriter( ICacheWriter<TKey, TValue>^ cacheWriter )
      {
        apache::geode::client::CacheWriterPtr writerptr;
        if ( cacheWriter != nullptr ) {
          CacheWriterGeneric<TKey, TValue>^ cwg = gcnew CacheWriterGeneric<TKey, TValue>();
          cwg->SetCacheWriter(cacheWriter);
          writerptr = new apache::geode::client::ManagedCacheWriterGeneric( /*cwg,*/ cacheWriter );
          ((apache::geode::client::ManagedCacheWriterGeneric*)writerptr.ptr())->setptr(cwg);
        }
        NativePtr->setCacheWriter( writerptr );
      }

      generic<class TKey, class TValue>
      void AttributesFactory<TKey, TValue>::SetCacheListener( ICacheListener<TKey, TValue>^ cacheListener )
      {
        apache::geode::client::CacheListenerPtr listenerptr;
        if ( cacheListener != nullptr ) {
          CacheListenerGeneric<TKey, TValue>^ clg = gcnew CacheListenerGeneric<TKey, TValue>();
          clg->SetCacheListener(cacheListener);
          //listenerptr = new apache::geode::client::ManagedCacheListenerGeneric( (ICacheListener<Object^, Object^>^)cacheListener );
          listenerptr = new apache::geode::client::ManagedCacheListenerGeneric( /*clg,*/ cacheListener );
          ((apache::geode::client::ManagedCacheListenerGeneric*)listenerptr.ptr())->setptr(clg);
        }
        NativePtr->setCacheListener( listenerptr );
      }

      generic<class TKey, class TValue>
      void AttributesFactory<TKey, TValue>::SetPartitionResolver( IPartitionResolver<TKey, TValue>^ partitionresolver )
      {
        apache::geode::client::PartitionResolverPtr resolverptr;
        if ( partitionresolver != nullptr ) {
          Generic::IFixedPartitionResolver<TKey, TValue>^ resolver = 
            dynamic_cast<Generic::IFixedPartitionResolver<TKey, TValue>^>(partitionresolver);
          if (resolver != nullptr) {            
            FixedPartitionResolverGeneric<TKey, TValue>^ prg = gcnew FixedPartitionResolverGeneric<TKey, TValue>();
            prg->SetPartitionResolver(partitionresolver);
            resolverptr = new apache::geode::client::ManagedFixedPartitionResolverGeneric( partitionresolver ); 
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
      }

      generic<class TKey, class TValue>
      void AttributesFactory<TKey, TValue>::SetCacheLoader( String^ libPath, String^ factoryFunctionName )
      {
        throw gcnew System::NotSupportedException;
        ManagedString mg_libpath( libPath );
        ManagedString mg_factoryFunctionName( factoryFunctionName );

        NativePtr->setCacheLoader( mg_libpath.CharPtr,
          mg_factoryFunctionName.CharPtr );
      }

      generic<class TKey, class TValue>
      void AttributesFactory<TKey, TValue>::SetCacheWriter( String^ libPath, String^ factoryFunctionName )
      {
        throw gcnew System::NotSupportedException;
        ManagedString mg_libpath( libPath );
        ManagedString mg_factoryFunctionName( factoryFunctionName );

        NativePtr->setCacheWriter( mg_libpath.CharPtr,
          mg_factoryFunctionName.CharPtr );
      }

      generic<class TKey, class TValue>
      void AttributesFactory<TKey, TValue>::SetCacheListener( String^ libPath, String^ factoryFunctionName )
      {
        throw gcnew System::NotSupportedException;
        ManagedString mg_libpath( libPath );
        ManagedString mg_factoryFunctionName( factoryFunctionName );

        NativePtr->setCacheListener( mg_libpath.CharPtr,
          mg_factoryFunctionName.CharPtr );
      }

      generic<class TKey, class TValue>
      void AttributesFactory<TKey, TValue>::SetPartitionResolver( String^ libPath, String^ factoryFunctionName )
      {
        throw gcnew System::NotSupportedException;
        ManagedString mg_libpath( libPath );
        ManagedString mg_factoryFunctionName( factoryFunctionName );

        NativePtr->setPartitionResolver( mg_libpath.CharPtr,
          mg_factoryFunctionName.CharPtr );
      }

      // EXPIRATION ATTRIBUTES

      generic<class TKey, class TValue>
      void AttributesFactory<TKey, TValue>::SetEntryIdleTimeout( ExpirationAction action, uint32_t idleTimeout )
      {
        NativePtr->setEntryIdleTimeout(
          static_cast<apache::geode::client::ExpirationAction::Action>( action ), idleTimeout );
      }

      generic<class TKey, class TValue>
      void AttributesFactory<TKey, TValue>::SetEntryTimeToLive( ExpirationAction action, uint32_t timeToLive )
      {
        NativePtr->setEntryTimeToLive(
          static_cast<apache::geode::client::ExpirationAction::Action>( action ), timeToLive );
      }

      generic<class TKey, class TValue>
      void AttributesFactory<TKey, TValue>::SetRegionIdleTimeout( ExpirationAction action, uint32_t idleTimeout )
      {
        NativePtr->setRegionIdleTimeout(
          static_cast<apache::geode::client::ExpirationAction::Action>( action ), idleTimeout );
      }

      generic<class TKey, class TValue>
      void AttributesFactory<TKey, TValue>::SetRegionTimeToLive( ExpirationAction action, uint32_t timeToLive )
      {
        NativePtr->setRegionTimeToLive(
          static_cast<apache::geode::client::ExpirationAction::Action>( action ), timeToLive );
      }

      // PERSISTENCE
       generic<class TKey, class TValue>
      void AttributesFactory<TKey, TValue>::SetPersistenceManager(IPersistenceManager<TKey, TValue>^ persistenceManager, Properties<String^, String^>^ config )
      {
        apache::geode::client::PersistenceManagerPtr persistenceManagerptr;
        if ( persistenceManager != nullptr ) {
          PersistenceManagerGeneric<TKey, TValue>^ clg = gcnew PersistenceManagerGeneric<TKey, TValue>();
          clg->SetPersistenceManager(persistenceManager);
          persistenceManagerptr = new apache::geode::client::ManagedPersistenceManagerGeneric( /*clg,*/ persistenceManager );
          ((apache::geode::client::ManagedPersistenceManagerGeneric*)persistenceManagerptr.ptr())->setptr(clg);
        }
         apache::geode::client::PropertiesPtr configptr(GetNativePtr<apache::geode::client::Properties>( config ) );
         NativePtr->setPersistenceManager( persistenceManagerptr, configptr );
      }
      
      generic<class TKey, class TValue>
      void AttributesFactory<TKey, TValue>::SetPersistenceManager(IPersistenceManager<TKey, TValue>^ persistenceManager )
      {
        SetPersistenceManager(persistenceManager, nullptr);
      }
        
      generic<class TKey, class TValue>
      void AttributesFactory<TKey, TValue>::SetPersistenceManager( String^ libPath,
        String^ factoryFunctionName )
      {        
        SetPersistenceManager( libPath, factoryFunctionName, nullptr );
      }

      generic<class TKey, class TValue>
      void AttributesFactory<TKey, TValue>::SetPersistenceManager( String^ libPath,
        String^ factoryFunctionName, Properties<String^, String^>^ config )
      {        
        ManagedString mg_libpath( libPath );
        ManagedString mg_factoryFunctionName( factoryFunctionName );
        //  TODO:
				//TODO::split
        apache::geode::client::PropertiesPtr configptr(
          GetNativePtr<apache::geode::client::Properties>( config ) );

        NativePtr->setPersistenceManager( mg_libpath.CharPtr,
          mg_factoryFunctionName.CharPtr, configptr );
          
      }

      // STORAGE ATTRIBUTES

      generic<class TKey, class TValue>
      void AttributesFactory<TKey, TValue>::SetPoolName( String^ poolName )
      {
        ManagedString mg_poolName( poolName );

        NativePtr->setPoolName( mg_poolName.CharPtr );
      }

      // MAP ATTRIBUTES

      generic<class TKey, class TValue>
      void AttributesFactory<TKey, TValue>::SetInitialCapacity( int32_t initialCapacity )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          NativePtr->setInitialCapacity( initialCapacity );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void AttributesFactory<TKey, TValue>::SetLoadFactor( Single loadFactor )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          NativePtr->setLoadFactor( loadFactor );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void AttributesFactory<TKey, TValue>::SetConcurrencyLevel( int32_t concurrencyLevel )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          NativePtr->setConcurrencyLevel( concurrencyLevel );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void AttributesFactory<TKey, TValue>::SetLruEntriesLimit( uint32_t entriesLimit )
      {
        NativePtr->setLruEntriesLimit( entriesLimit );
      }

      generic<class TKey, class TValue>
      void AttributesFactory<TKey, TValue>::SetDiskPolicy( DiskPolicyType diskPolicy )
      {
        NativePtr->setDiskPolicy(
          static_cast<apache::geode::client::DiskPolicyType::PolicyType>( diskPolicy ) );
      }

      generic<class TKey, class TValue>
      void AttributesFactory<TKey, TValue>::SetCachingEnabled( bool cachingEnabled )
      {
        NativePtr->setCachingEnabled( cachingEnabled );
      }

      generic<class TKey, class TValue>
      void AttributesFactory<TKey, TValue>::SetCloningEnabled( bool cloningEnabled )
      {
        NativePtr->setCloningEnabled( cloningEnabled );
      }

      generic<class TKey, class TValue>
      void  AttributesFactory<TKey, TValue>::SetConcurrencyChecksEnabled( bool concurrencyChecksEnabled )
      {
        NativePtr->setConcurrencyChecksEnabled( concurrencyChecksEnabled );
      }
      // FACTORY METHOD

      generic<class TKey, class TValue>
      Apache::Geode::Client::Generic::RegionAttributes<TKey, TValue>^ AttributesFactory<TKey, TValue>::CreateRegionAttributes()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          apache::geode::client::RegionAttributesPtr& nativeptr (
            NativePtr->createRegionAttributes( ) );
          return Apache::Geode::Client::Generic::RegionAttributes<TKey, TValue>::Create(nativeptr.ptr());

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

    }
  }
}
} //namespace 
