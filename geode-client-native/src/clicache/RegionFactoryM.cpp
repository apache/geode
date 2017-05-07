/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_includes.hpp"
#include "RegionFactoryM.hpp"
#include "AttributesFactoryM.hpp"
#include "RegionM.hpp"
#include "impl/ManagedCacheLoader.hpp"
#include "impl/ManagedCacheWriter.hpp"
#include "impl/ManagedCacheListener.hpp"
#include "impl/ManagedPartitionResolver.hpp"
#include "impl/ManagedFixedPartitionResolver.hpp"
#include "RegionAttributesM.hpp"
#include "PropertiesM.hpp"
#include "ICacheLoader.hpp"
#include "ICacheWriter.hpp"
#include "ICacheListener.hpp"
#include "IPartitionResolver.hpp"
#include "IFixedPartitionResolver.hpp"
#include "impl/SafeConvert.hpp"

#include "com/vmware/impl/ManagedCacheLoaderN.hpp"
#include "com/vmware/impl/ManagedCacheWriterN.hpp"
#include "com/vmware/impl/ManagedCacheListenerN.hpp"
#include "com/vmware/impl/ManagedPartitionResolverN.hpp"
#include "com/vmware/impl/ManagedFixedPartitionResolverN.hpp"

#include "com/vmware/impl/CacheLoaderMN.hpp"
#include "com/vmware/impl/CacheWriterMN.hpp"
#include "com/vmware/impl/CacheListenerMN.hpp"
#include "com/vmware/impl/PartitionResolverMN.hpp"
#include "com/vmware/impl/FixedPartitionResolverMN.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      Region^ RegionFactory::Create(String^ regionName)
      {
        _GF_MG_EXCEPTION_TRY

          ManagedString mg_name( regionName );
          
        gemfire::RegionPtr& nativeptr( NativePtr->create(
            mg_name.CharPtr ) );
          return Region::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

        RegionFactory^ RegionFactory::SetCacheLoader( ICacheLoader^ cacheLoader )
      {
        gemfire::CacheLoaderPtr loaderptr;
        if ( cacheLoader != nullptr ) {
          loaderptr = new gemfire::ManagedCacheLoader( cacheLoader );
        }
        NativePtr->setCacheLoader( loaderptr );
        return this;
      }

      RegionFactory^ RegionFactory::SetCacheWriter( ICacheWriter^ cacheWriter )
      {
        gemfire::CacheWriterPtr writerptr;
        if ( cacheWriter != nullptr ) {
          writerptr = new gemfire::ManagedCacheWriter( cacheWriter );
        }
        NativePtr->setCacheWriter( writerptr );
        return this;
      }

      RegionFactory^ RegionFactory::SetCacheListener( ICacheListener^ cacheListener )
      {
        gemfire::CacheListenerPtr listenerptr;
        if ( cacheListener != nullptr ) {
          listenerptr = new gemfire::ManagedCacheListener( cacheListener );
        }
        NativePtr->setCacheListener( listenerptr );
        return this;
      }

      RegionFactory^ RegionFactory::SetPartitionResolver( IPartitionResolver^ partitionresolver )
      {
        gemfire::PartitionResolverPtr resolverptr;
        if ( partitionresolver != nullptr ) {
          IFixedPartitionResolver^ resolver = dynamic_cast<IFixedPartitionResolver^>(partitionresolver);
          if (resolver != nullptr) {
            resolverptr = new gemfire::ManagedFixedPartitionResolver( resolver );            
          }
          else {
            resolverptr = new gemfire::ManagedPartitionResolver( partitionresolver );
          }          
        }
        NativePtr->setPartitionResolver( resolverptr );
        return this;
      }

      RegionFactory^ RegionFactory::SetCacheLoader( String^ libPath, String^ factoryFunctionName )
      {        
        ManagedString mg_libpath( libPath );
        ManagedString mg_factoryFunctionName( factoryFunctionName );

        NativePtr->setCacheLoader( mg_libpath.CharPtr,
          mg_factoryFunctionName.CharPtr );
        return this;
      }

      RegionFactory^ RegionFactory::SetCacheWriter( String^ libPath, String^ factoryFunctionName )
      {        
        ManagedString mg_libpath( libPath );
        ManagedString mg_factoryFunctionName( factoryFunctionName );

        NativePtr->setCacheWriter( mg_libpath.CharPtr,
          mg_factoryFunctionName.CharPtr );
        return this;
      }

      RegionFactory^ RegionFactory::SetCacheListener( String^ libPath, String^ factoryFunctionName )
      {        
        ManagedString mg_libpath( libPath );
        ManagedString mg_factoryFunctionName( factoryFunctionName );

        NativePtr->setCacheListener( mg_libpath.CharPtr,
          mg_factoryFunctionName.CharPtr );
        return this;
      }

      RegionFactory^ RegionFactory::SetPartitionResolver( String^ libPath, String^ factoryFunctionName )
      {        
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
          static_cast<gemfire::ExpirationAction::Action>( action ), idleTimeout );
        return this;
      }

      RegionFactory^ RegionFactory::SetEntryTimeToLive( ExpirationAction action, uint32_t timeToLive )
      {
        NativePtr->setEntryTimeToLive(
          static_cast<gemfire::ExpirationAction::Action>( action ), timeToLive );
        return this;
      }

      RegionFactory^ RegionFactory::SetRegionIdleTimeout( ExpirationAction action, uint32_t idleTimeout )
      {
        NativePtr->setRegionIdleTimeout(
          static_cast<gemfire::ExpirationAction::Action>( action ), idleTimeout );
        return this;
      }

      RegionFactory^ RegionFactory::SetRegionTimeToLive( ExpirationAction action, uint32_t timeToLive )
      {
        NativePtr->setRegionTimeToLive(
          static_cast<gemfire::ExpirationAction::Action>( action ), timeToLive );
        return this;
      }

      // PERSISTENCE

      RegionFactory^ RegionFactory::SetPersistenceManager( String^ libPath,
        String^ factoryFunctionName )
      {        
        SetPersistenceManager( libPath, factoryFunctionName, nullptr );
        return this;
      }

      RegionFactory^ RegionFactory::SetPersistenceManager( String^ libPath,
        String^ factoryFunctionName, Properties^ config )
      {        
        ManagedString mg_libpath( libPath );
        ManagedString mg_factoryFunctionName( factoryFunctionName );
        gemfire::PropertiesPtr configptr(
          GetNativePtr<gemfire::Properties>( config ) );

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
        _GF_MG_EXCEPTION_TRY

          NativePtr->setInitialCapacity( initialCapacity );
          return this;

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      RegionFactory^ RegionFactory::SetLoadFactor( Single loadFactor )
      {
        _GF_MG_EXCEPTION_TRY

          NativePtr->setLoadFactor( loadFactor );
          return this;

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      RegionFactory^ RegionFactory::SetConcurrencyLevel( int32_t concurrencyLevel )
      {
        _GF_MG_EXCEPTION_TRY

          NativePtr->setConcurrencyLevel( concurrencyLevel );
          return this;

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      RegionFactory^ RegionFactory::SetLruEntriesLimit( uint32_t entriesLimit )
      {
        NativePtr->setLruEntriesLimit( entriesLimit );
        return this;
      }

      RegionFactory^ RegionFactory::SetDiskPolicy( DiskPolicyType diskPolicy )
      {
        NativePtr->setDiskPolicy(
          static_cast<gemfire::DiskPolicyType::PolicyType>( diskPolicy ) );
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
    }
  }
}
