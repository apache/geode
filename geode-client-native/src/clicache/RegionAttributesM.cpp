/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "RegionAttributesM.hpp"
#include "RegionM.hpp"
#include "impl/ManagedCacheLoader.hpp"
#include "impl/ManagedCacheWriter.hpp"
#include "impl/ManagedCacheListener.hpp"
#include "impl/ManagedPartitionResolver.hpp"
#include "PropertiesM.hpp"
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
    namespace Cache
    {

      ICacheLoader^ RegionAttributes::CacheLoader::get( )
      {
        gemfire::CacheLoaderPtr& loaderptr( NativePtr->getCacheLoader( ) );
        gemfire::ManagedCacheLoader* mg_loader =
          dynamic_cast<gemfire::ManagedCacheLoader*>( loaderptr.ptr( ) );

        if (mg_loader != nullptr)
        {
          return mg_loader->ptr( );
        }
        return nullptr;
      }

      ICacheWriter^ RegionAttributes::CacheWriter::get( )
      {
        gemfire::CacheWriterPtr& writerptr( NativePtr->getCacheWriter( ) );
        gemfire::ManagedCacheWriter* mg_writer =
          dynamic_cast<gemfire::ManagedCacheWriter*>( writerptr.ptr( ) );

        if (mg_writer != nullptr)
        {
          return mg_writer->ptr( );
        }
        return nullptr;
      }

      ICacheListener^ RegionAttributes::CacheListener::get( )
      {
        gemfire::CacheListenerPtr& listenerptr( NativePtr->getCacheListener( ) );
        gemfire::ManagedCacheListener* mg_listener =
          dynamic_cast<gemfire::ManagedCacheListener*>( listenerptr.ptr( ) );

        if (mg_listener != nullptr)
        {
          return mg_listener->ptr( );
        }
        return nullptr;
      }

      IPartitionResolver^ RegionAttributes::PartitionResolver::get( )
      {
        gemfire::PartitionResolverPtr& resolverptr( NativePtr->getPartitionResolver( ) );
        gemfire::ManagedPartitionResolver* mg_resolver =
          dynamic_cast<gemfire::ManagedPartitionResolver*>( resolverptr.ptr( ) );

        if (mg_resolver != nullptr)
        {
          return mg_resolver->ptr( );
        }
        return nullptr;
      }

      int32_t RegionAttributes::RegionTimeToLive::get( )
      {
        return NativePtr->getRegionTimeToLive( );
      }

      ExpirationAction RegionAttributes::RegionTimeToLiveAction::get( )
      {
        return static_cast<ExpirationAction>( NativePtr->getRegionTimeToLiveAction( ) );
      }

      int32_t RegionAttributes::RegionIdleTimeout::get( )
      {
        return NativePtr->getRegionIdleTimeout( );
      }

      ExpirationAction RegionAttributes::RegionIdleTimeoutAction::get( )
      {
        return static_cast<ExpirationAction>( NativePtr->getRegionIdleTimeoutAction( ) );
      }

      int32_t RegionAttributes::EntryTimeToLive::get( )
      {
        return NativePtr->getEntryTimeToLive( );
      }

      ExpirationAction RegionAttributes::EntryTimeToLiveAction::get( )
      {
        return static_cast<ExpirationAction>( NativePtr->getEntryTimeToLiveAction( ) );
      }

      int32_t RegionAttributes::EntryIdleTimeout::get( )
      {
        return NativePtr->getEntryIdleTimeout( );
      }

      ExpirationAction RegionAttributes::EntryIdleTimeoutAction::get( )
      {
        return static_cast<ExpirationAction>( NativePtr->getEntryIdleTimeoutAction( ) );
      }

      ScopeType RegionAttributes::Scope::get( )
      {
        return static_cast<ScopeType>( NativePtr->getScope( ) );
      }

      bool RegionAttributes::CachingEnabled::get( )
      {
        return NativePtr->getCachingEnabled( );
      }

      bool RegionAttributes::CloningEnabled::get( )
      {
        return NativePtr->getCloningEnabled( );
      }

      int32_t RegionAttributes::InitialCapacity::get( )
      {
        return NativePtr->getInitialCapacity( );
      }

      Single RegionAttributes::LoadFactor::get( )
      {
        return NativePtr->getLoadFactor( );
      }

      int32_t RegionAttributes::ConcurrencyLevel::get( )
      {
        return NativePtr->getConcurrencyLevel( );
      }

      uint32_t RegionAttributes::LruEntriesLimit::get( )
      {
        return NativePtr->getLruEntriesLimit( );
      }

      DiskPolicyType RegionAttributes::DiskPolicy::get( )
      {
        return static_cast<DiskPolicyType>( NativePtr->getDiskPolicy( ) );
      }

      ExpirationAction RegionAttributes::LruEvictionAction::get( )
      {
        return static_cast<ExpirationAction>( NativePtr->getLruEvictionAction( ) );
      }

      String^ RegionAttributes::CacheLoaderLibrary::get( )
      {
        return ManagedString::Get( NativePtr->getCacheLoaderLibrary( ) );
      }

      String^ RegionAttributes::CacheLoaderFactory::get( )
      {
        return ManagedString::Get( NativePtr->getCacheLoaderFactory( ) );
      }

      String^ RegionAttributes::CacheListenerLibrary::get( )
      {
        return ManagedString::Get( NativePtr->getCacheListenerLibrary( ) );
      }

      String^ RegionAttributes::PartitionResolverLibrary::get( )
      {
        return ManagedString::Get( NativePtr->getPartitionResolverLibrary( ) );
      }

      String^ RegionAttributes::PartitionResolverFactory::get( )
      {
        return ManagedString::Get( NativePtr->getPartitionResolverFactory( ) );
      }

      String^ RegionAttributes::CacheListenerFactory::get( )
      {
        return ManagedString::Get( NativePtr->getCacheListenerFactory( ) );
      }

      String^ RegionAttributes::CacheWriterLibrary::get( )
      {
        return ManagedString::Get( NativePtr->getCacheWriterLibrary( ) );
      }

      String^ RegionAttributes::CacheWriterFactory::get( )
      {
        return ManagedString::Get( NativePtr->getCacheWriterFactory( ) );
      }

      bool RegionAttributes::Equals( RegionAttributes^ other )
      {
        gemfire::RegionAttributes* otherPtr =
          GetNativePtr<gemfire::RegionAttributes>( other );
        if (_NativePtr != nullptr && otherPtr != nullptr) {
          return NativePtr->operator==(*otherPtr);
        }
        return (_NativePtr == otherPtr);
      }

      bool RegionAttributes::Equals( Object^ other )
      {
        gemfire::RegionAttributes* otherPtr = GetNativePtr<gemfire::
          RegionAttributes>( dynamic_cast<RegionAttributes^>( other ) );
        if (_NativePtr != nullptr && otherPtr != nullptr) {
          return NativePtr->operator==(*otherPtr);
        }
        return (_NativePtr == otherPtr);
      }

      void RegionAttributes::ValidateSerializableAttributes( )
      {
        NativePtr->validateSerializableAttributes( );
      }

      String^ RegionAttributes::Endpoints::get( )
      {
        return ManagedString::Get( NativePtr->getEndpoints( ) );
      }

      String^ RegionAttributes::PoolName::get( )
      {
        return ManagedString::Get( NativePtr->getPoolName( ) );
      }

      Boolean RegionAttributes::ClientNotificationEnabled::get( )
      {
        return NativePtr->getClientNotificationEnabled( );
      }

      String^ RegionAttributes::PersistenceLibrary::get( )
      {
        return ManagedString::Get( NativePtr->getPersistenceLibrary( ) );
      }

      String^ RegionAttributes::PersistenceFactory::get( )
      {
        return ManagedString::Get( NativePtr->getPersistenceFactory( ) );
      }

      Properties^ RegionAttributes::PersistenceProperties::get( )
      {
        gemfire::PropertiesPtr& nativeptr(
          NativePtr->getPersistenceProperties( ) );
        return Properties::Create( nativeptr.ptr( ) );
      }

      bool RegionAttributes::ConcurrencyChecksEnabled::get( )
      {
        return NativePtr->getConcurrencyChecksEnabled( );
      }

      void RegionAttributes::ToData( DataOutput^ output )
      {
        gemfire::DataOutput* nativeOutput =
          GetNativePtr<gemfire::DataOutput>( output );
        if (nativeOutput != nullptr)
        {
          NativePtr->toData( *nativeOutput );
        }
      }

      IGFSerializable^ RegionAttributes::FromData( DataInput^ input )
      {
        gemfire::DataInput* nativeInput =
          GetNativePtr<gemfire::DataInput>( input );
        if (nativeInput != nullptr)
        {
          AssignPtr( static_cast<gemfire::RegionAttributes*>(
            NativePtr->fromData( *nativeInput ) ) );
        }
        return this;
      }

    }
  }
}
