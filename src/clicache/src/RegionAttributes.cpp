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
#include "RegionAttributes.hpp"
//#include "Region.hpp"
#include "impl/ManagedCacheLoader.hpp"
#include "impl/ManagedCacheWriter.hpp"
#include "impl/ManagedCacheListener.hpp"
#include "impl/ManagedPartitionResolver.hpp"
#include "impl/ManagedFixedPartitionResolver.hpp"
#include "impl/CacheLoader.hpp"
#include "impl/CacheWriter.hpp"
#include "impl/CacheListener.hpp"
#include "impl/PartitionResolver.hpp"
#include "Properties.hpp"
#include "ICacheLoader.hpp"
#include "ICacheWriter.hpp"
#include "ICacheListener.hpp"
#include "IPartitionResolver.hpp"
#include "CacheListenerAdapter.hpp"
#include "CacheWriterAdapter.hpp"
#include "impl/SafeConvert.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache {

      namespace Generic
    {

      generic <class TKey, class TValue>
      void Generic::RegionAttributes<TKey, TValue>::ToData(
        GemStone::GemFire::Cache::Generic::DataOutput^ output )
      {
        apache::geode::client::DataOutput* nativeOutput =
          GemStone::GemFire::Cache::Generic::GetNativePtrFromUMWrapGeneric<apache::geode::client::DataOutput>( output );
        if (nativeOutput != nullptr)
        {
          NativePtr->toData( *nativeOutput );
        }
      }

      generic <class TKey, class TValue>
      GemStone::GemFire::Cache::Generic::IGFSerializable^ Generic::RegionAttributes<TKey, TValue>::FromData(
        GemStone::GemFire::Cache::Generic::DataInput^ input )
      {
        apache::geode::client::DataInput* nativeInput =
          GemStone::GemFire::Cache::Generic::GetNativePtrFromUMWrapGeneric<apache::geode::client::DataInput>( input );
        if (nativeInput != nullptr)
        {
          AssignPtr( static_cast<apache::geode::client::RegionAttributes*>(
            NativePtr->fromData( *nativeInput ) ) );
        }
        return this;
      }

      generic <class TKey, class TValue>
      ICacheLoader<TKey, TValue>^ Generic::RegionAttributes<TKey, TValue>::CacheLoader::get()
      {
        apache::geode::client::CacheLoaderPtr& loaderptr( NativePtr->getCacheLoader( ) );
        apache::geode::client::ManagedCacheLoaderGeneric* mg_loader =
          dynamic_cast<apache::geode::client::ManagedCacheLoaderGeneric*>( loaderptr.ptr( ) );

        if (mg_loader != nullptr)
        {
          return (ICacheLoader<TKey, TValue>^) mg_loader->userptr( );
        }
        return nullptr;
      }

      generic <class TKey, class TValue>
      ICacheWriter<TKey, TValue>^ Generic::RegionAttributes<TKey, TValue>::CacheWriter::get()
      {
        apache::geode::client::CacheWriterPtr& writerptr( NativePtr->getCacheWriter( ) );
        apache::geode::client::ManagedCacheWriterGeneric* mg_writer =
          dynamic_cast<apache::geode::client::ManagedCacheWriterGeneric*>( writerptr.ptr( ) );

        if (mg_writer != nullptr)
        {
          return (ICacheWriter<TKey, TValue>^)mg_writer->userptr( );
        }
        return nullptr;
      }

      generic <class TKey, class TValue>
      ICacheListener<TKey, TValue>^ Generic::RegionAttributes<TKey, TValue>::CacheListener::get()
      {
        apache::geode::client::CacheListenerPtr& listenerptr( NativePtr->getCacheListener( ) );
        apache::geode::client::ManagedCacheListenerGeneric* mg_listener =
          dynamic_cast<apache::geode::client::ManagedCacheListenerGeneric*>( listenerptr.ptr( ) );

        if (mg_listener != nullptr)
        {
          /*
          CacheListenerGeneric<TKey, TValue>^ clg = gcnew CacheListenerGeneric<TKey, TValue>();
          clg->SetCacheListener((ICacheListener<TKey, TValue>^)mg_listener->userptr());
          mg_listener->setptr(clg);
          */
          return (ICacheListener<TKey, TValue>^)mg_listener->userptr( );
        }
        return nullptr;
      }

      generic <class TKey, class TValue>
      IPartitionResolver<TKey, TValue>^ Generic::RegionAttributes<TKey, TValue>::PartitionResolver::get()
      {
        apache::geode::client::PartitionResolverPtr& resolverptr( NativePtr->getPartitionResolver( ) );
        apache::geode::client::ManagedPartitionResolverGeneric* mg_resolver =
          dynamic_cast<apache::geode::client::ManagedPartitionResolverGeneric*>( resolverptr.ptr( ) );

        if (mg_resolver != nullptr)
        {
          return (IPartitionResolver<TKey, TValue>^)mg_resolver->userptr( );
        }

        apache::geode::client::ManagedFixedPartitionResolverGeneric* mg_fixedResolver =
          dynamic_cast<apache::geode::client::ManagedFixedPartitionResolverGeneric*>( resolverptr.ptr( ) );

        if (mg_fixedResolver != nullptr)
        {
          return (IPartitionResolver<TKey, TValue>^)mg_fixedResolver->userptr( );
        }

        return nullptr;
      }

      generic <class TKey, class TValue>
      int32_t Generic::RegionAttributes<TKey, TValue>::RegionTimeToLive::get()
      {
        return NativePtr->getRegionTimeToLive( );
      }

      generic <class TKey, class TValue>
      ExpirationAction Generic::RegionAttributes<TKey, TValue>::RegionTimeToLiveAction::get()
      {
        return static_cast<ExpirationAction>( NativePtr->getRegionTimeToLiveAction( ) );
      }

      generic <class TKey, class TValue>
      int32_t Generic::RegionAttributes<TKey, TValue>::RegionIdleTimeout::get()
      {
        return NativePtr->getRegionIdleTimeout( );
      }

      generic <class TKey, class TValue>
      ExpirationAction Generic::RegionAttributes<TKey, TValue>::RegionIdleTimeoutAction::get()
      {
        return static_cast<ExpirationAction>( NativePtr->getRegionIdleTimeoutAction( ) );
      }

      generic <class TKey, class TValue>
      int32_t Generic::RegionAttributes<TKey, TValue>::EntryTimeToLive::get()
      {
        return NativePtr->getEntryTimeToLive( );
      }

      generic <class TKey, class TValue>
      ExpirationAction Generic::RegionAttributes<TKey, TValue>::EntryTimeToLiveAction::get()
      {
        return static_cast<ExpirationAction>( NativePtr->getEntryTimeToLiveAction( ) );
      }

      generic <class TKey, class TValue>
      int32_t Generic::RegionAttributes<TKey, TValue>::EntryIdleTimeout::get()
      {
        return NativePtr->getEntryIdleTimeout( );
      }

      generic <class TKey, class TValue>
      ExpirationAction Generic::RegionAttributes<TKey, TValue>::EntryIdleTimeoutAction::get()
      {
        return static_cast<ExpirationAction>( NativePtr->getEntryIdleTimeoutAction( ) );
      }

      generic <class TKey, class TValue>
      bool Generic::RegionAttributes<TKey, TValue>::CachingEnabled::get()
      {
        return NativePtr->getCachingEnabled( );
      }

      generic <class TKey, class TValue>
      bool Generic::RegionAttributes<TKey, TValue>::CloningEnabled::get()
      {
        return NativePtr->getCloningEnabled( );
      }

      generic <class TKey, class TValue>
      int32_t Generic::RegionAttributes<TKey, TValue>::InitialCapacity::get()
      {
        return NativePtr->getInitialCapacity( );
      }

      generic <class TKey, class TValue>
      Single Generic::RegionAttributes<TKey, TValue>::LoadFactor::get()
      {
        return NativePtr->getLoadFactor( );
      }

      generic <class TKey, class TValue>
        int32_t Generic::RegionAttributes<TKey, TValue>::ConcurrencyLevel::get()
      {
        return NativePtr->getConcurrencyLevel( );
      }

      generic <class TKey, class TValue>
      uint32_t Generic::RegionAttributes<TKey, TValue>::LruEntriesLimit::get()
      {
        return NativePtr->getLruEntriesLimit( );
      }

      generic <class TKey, class TValue>
      DiskPolicyType Generic::RegionAttributes<TKey, TValue>::DiskPolicy::get()
      {
        return static_cast<DiskPolicyType>( NativePtr->getDiskPolicy( ) );
      }

      generic <class TKey, class TValue>
      ExpirationAction Generic::RegionAttributes<TKey, TValue>::LruEvictionAction::get()
      {
        return static_cast<ExpirationAction>( NativePtr->getLruEvictionAction( ) );
      }

      generic <class TKey, class TValue>
      String^ Generic::RegionAttributes<TKey, TValue>::CacheLoaderLibrary::get()
      {
        return ManagedString::Get( NativePtr->getCacheLoaderLibrary( ) );
      }

      generic <class TKey, class TValue>
      String^ Generic::RegionAttributes<TKey, TValue>::CacheLoaderFactory::get()
      {
        return ManagedString::Get( NativePtr->getCacheLoaderFactory( ) );
      }

      generic <class TKey, class TValue>
      String^ Generic::RegionAttributes<TKey, TValue>::CacheListenerLibrary::get()
      {
        return ManagedString::Get( NativePtr->getCacheListenerLibrary( ) );
      }

      generic <class TKey, class TValue>
      String^ Generic::RegionAttributes<TKey, TValue>::PartitionResolverLibrary::get()
      {
        return ManagedString::Get( NativePtr->getPartitionResolverLibrary( ) );
      }

      generic <class TKey, class TValue>
      String^ Generic::RegionAttributes<TKey, TValue>::PartitionResolverFactory::get()
      {
        return ManagedString::Get( NativePtr->getPartitionResolverFactory( ) );
      }

      generic <class TKey, class TValue>
      String^ Generic::RegionAttributes<TKey, TValue>::CacheListenerFactory::get()
      {
        return ManagedString::Get( NativePtr->getCacheListenerFactory( ) );
      }

      generic <class TKey, class TValue>
      String^ Generic::RegionAttributes<TKey, TValue>::CacheWriterLibrary::get()
      {
        return ManagedString::Get( NativePtr->getCacheWriterLibrary( ) );
      }

      generic <class TKey, class TValue>
      String^ Generic::RegionAttributes<TKey, TValue>::CacheWriterFactory::get()
      {
        return ManagedString::Get( NativePtr->getCacheWriterFactory( ) );
      }

      generic <class TKey, class TValue>
      bool Generic::RegionAttributes<TKey, TValue>::Equals(Generic::RegionAttributes<TKey, TValue>^ other)
      {
        apache::geode::client::RegionAttributes* otherPtr =
          GetNativePtrFromSBWrapGeneric<apache::geode::client::RegionAttributes>( other );
        if (_NativePtr != nullptr && otherPtr != nullptr) {
          return NativePtr->operator==(*otherPtr);
        }
        return (_NativePtr == otherPtr);
      }

      generic <class TKey, class TValue>
      bool Generic::RegionAttributes<TKey, TValue>::Equals(Object^ other)
      {
        apache::geode::client::RegionAttributes* otherPtr = GetNativePtrFromSBWrapGeneric<apache::geode::client::
          RegionAttributes>( dynamic_cast<Generic::RegionAttributes<TKey, TValue>^>( other ) );
        if (_NativePtr != nullptr && otherPtr != nullptr) {
          return NativePtr->operator==(*otherPtr);
        }
        return (_NativePtr == otherPtr);
      }

      generic <class TKey, class TValue>
      void Generic::RegionAttributes<TKey, TValue>::ValidateSerializableAttributes()
      {
        NativePtr->validateSerializableAttributes( );
      }

      generic <class TKey, class TValue>
      String^ Generic::RegionAttributes<TKey, TValue>::Endpoints::get()
      {
        return ManagedString::Get( NativePtr->getEndpoints( ) );
      }

      generic <class TKey, class TValue>
      String^ Generic::RegionAttributes<TKey, TValue>::PoolName::get()
      {
        return ManagedString::Get( NativePtr->getPoolName( ) );
      }

      generic <class TKey, class TValue>
      Boolean Generic::RegionAttributes<TKey, TValue>::ClientNotificationEnabled::get()
      {
        return NativePtr->getClientNotificationEnabled( );
      }

      generic <class TKey, class TValue>
      String^ Generic::RegionAttributes<TKey, TValue>::PersistenceLibrary::get()
      {
        return ManagedString::Get( NativePtr->getPersistenceLibrary( ) );
      }

      generic <class TKey, class TValue>
      String^ Generic::RegionAttributes<TKey, TValue>::PersistenceFactory::get()
      {
        return ManagedString::Get( NativePtr->getPersistenceFactory( ) );
      }
      generic <class TKey, class TValue>
      bool Generic::RegionAttributes<TKey, TValue>::ConcurrencyChecksEnabled::get()
      {
        return NativePtr->getConcurrencyChecksEnabled( );
      }

      generic <class TKey, class TValue>
      Properties<String^, String^>^Generic::RegionAttributes<TKey, TValue>::PersistenceProperties::get()
      {
        apache::geode::client::PropertiesPtr& nativeptr(
          NativePtr->getPersistenceProperties( ) );
        return Properties<String^, String^>::Create<String^, String^>( nativeptr.ptr( ) );
      }
    }
  }
}
 } //namespace 

