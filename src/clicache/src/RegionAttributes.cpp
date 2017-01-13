/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
        gemfire::DataOutput* nativeOutput =
          GemStone::GemFire::Cache::Generic::GetNativePtrFromUMWrapGeneric<gemfire::DataOutput>( output );
        if (nativeOutput != nullptr)
        {
          NativePtr->toData( *nativeOutput );
        }
      }

      generic <class TKey, class TValue>
      GemStone::GemFire::Cache::Generic::IGFSerializable^ Generic::RegionAttributes<TKey, TValue>::FromData(
        GemStone::GemFire::Cache::Generic::DataInput^ input )
      {
        gemfire::DataInput* nativeInput =
          GemStone::GemFire::Cache::Generic::GetNativePtrFromUMWrapGeneric<gemfire::DataInput>( input );
        if (nativeInput != nullptr)
        {
          AssignPtr( static_cast<gemfire::RegionAttributes*>(
            NativePtr->fromData( *nativeInput ) ) );
        }
        return this;
      }

      generic <class TKey, class TValue>
      ICacheLoader<TKey, TValue>^ Generic::RegionAttributes<TKey, TValue>::CacheLoader::get()
      {
        gemfire::CacheLoaderPtr& loaderptr( NativePtr->getCacheLoader( ) );
        gemfire::ManagedCacheLoaderGeneric* mg_loader =
          dynamic_cast<gemfire::ManagedCacheLoaderGeneric*>( loaderptr.ptr( ) );

        if (mg_loader != nullptr)
        {
          return (ICacheLoader<TKey, TValue>^) mg_loader->userptr( );
        }
        return nullptr;
      }

      generic <class TKey, class TValue>
      ICacheWriter<TKey, TValue>^ Generic::RegionAttributes<TKey, TValue>::CacheWriter::get()
      {
        gemfire::CacheWriterPtr& writerptr( NativePtr->getCacheWriter( ) );
        gemfire::ManagedCacheWriterGeneric* mg_writer =
          dynamic_cast<gemfire::ManagedCacheWriterGeneric*>( writerptr.ptr( ) );

        if (mg_writer != nullptr)
        {
          return (ICacheWriter<TKey, TValue>^)mg_writer->userptr( );
        }
        return nullptr;
      }

      generic <class TKey, class TValue>
      ICacheListener<TKey, TValue>^ Generic::RegionAttributes<TKey, TValue>::CacheListener::get()
      {
        gemfire::CacheListenerPtr& listenerptr( NativePtr->getCacheListener( ) );
        gemfire::ManagedCacheListenerGeneric* mg_listener =
          dynamic_cast<gemfire::ManagedCacheListenerGeneric*>( listenerptr.ptr( ) );

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
        gemfire::PartitionResolverPtr& resolverptr( NativePtr->getPartitionResolver( ) );
        gemfire::ManagedPartitionResolverGeneric* mg_resolver =
          dynamic_cast<gemfire::ManagedPartitionResolverGeneric*>( resolverptr.ptr( ) );

        if (mg_resolver != nullptr)
        {
          return (IPartitionResolver<TKey, TValue>^)mg_resolver->userptr( );
        }

        gemfire::ManagedFixedPartitionResolverGeneric* mg_fixedResolver =
          dynamic_cast<gemfire::ManagedFixedPartitionResolverGeneric*>( resolverptr.ptr( ) );

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
        gemfire::RegionAttributes* otherPtr =
          GetNativePtrFromSBWrapGeneric<gemfire::RegionAttributes>( other );
        if (_NativePtr != nullptr && otherPtr != nullptr) {
          return NativePtr->operator==(*otherPtr);
        }
        return (_NativePtr == otherPtr);
      }

      generic <class TKey, class TValue>
      bool Generic::RegionAttributes<TKey, TValue>::Equals(Object^ other)
      {
        gemfire::RegionAttributes* otherPtr = GetNativePtrFromSBWrapGeneric<gemfire::
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
        gemfire::PropertiesPtr& nativeptr(
          NativePtr->getPersistenceProperties( ) );
        return Properties<String^, String^>::Create<String^, String^>( nativeptr.ptr( ) );
      }
    }
  }
}
 } //namespace 

