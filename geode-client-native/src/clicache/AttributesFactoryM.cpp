/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
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


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      AttributesFactory::AttributesFactory( RegionAttributes^ regionAttributes )
        : UMWrap( )
      {
        gemfire::RegionAttributesPtr attribptr(
          GetNativePtr<gemfire::RegionAttributes>( regionAttributes ) );
        SetPtr( new gemfire::AttributesFactory( attribptr ), true );
      }

      // CALLBACKS

      void AttributesFactory::SetCacheLoader( ICacheLoader^ cacheLoader )
      {
        gemfire::CacheLoaderPtr loaderptr;
        if ( cacheLoader != nullptr ) {
          loaderptr = new gemfire::ManagedCacheLoader( cacheLoader );
        }
        NativePtr->setCacheLoader( loaderptr );
      }

      void AttributesFactory::SetCacheWriter( ICacheWriter^ cacheWriter )
      {
        gemfire::CacheWriterPtr writerptr;
        if ( cacheWriter != nullptr ) {
          writerptr = new gemfire::ManagedCacheWriter( cacheWriter );
        }
        NativePtr->setCacheWriter( writerptr );
      }

      void AttributesFactory::SetCacheListener( ICacheListener^ cacheListener )
      {
        gemfire::CacheListenerPtr listenerptr;
        if ( cacheListener != nullptr ) {
          listenerptr = new gemfire::ManagedCacheListener( cacheListener );
        }
        NativePtr->setCacheListener( listenerptr );
      }

      void AttributesFactory::SetPartitionResolver( IPartitionResolver^ partitionresolver )
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
      }

      void AttributesFactory::SetCacheLoader( String^ libPath, String^ factoryFunctionName )
      {
        ManagedString mg_libpath( libPath );
        ManagedString mg_factoryFunctionName( factoryFunctionName );

        NativePtr->setCacheLoader( mg_libpath.CharPtr,
          mg_factoryFunctionName.CharPtr );
      }

      void AttributesFactory::SetCacheWriter( String^ libPath, String^ factoryFunctionName )
      {
        ManagedString mg_libpath( libPath );
        ManagedString mg_factoryFunctionName( factoryFunctionName );

        NativePtr->setCacheWriter( mg_libpath.CharPtr,
          mg_factoryFunctionName.CharPtr );
      }

      void AttributesFactory::SetCacheListener( String^ libPath, String^ factoryFunctionName )
      {
        ManagedString mg_libpath( libPath );
        ManagedString mg_factoryFunctionName( factoryFunctionName );

        NativePtr->setCacheListener( mg_libpath.CharPtr,
          mg_factoryFunctionName.CharPtr );
      }

      void AttributesFactory::SetPartitionResolver( String^ libPath, String^ factoryFunctionName )
      {
        ManagedString mg_libpath( libPath );
        ManagedString mg_factoryFunctionName( factoryFunctionName );

        NativePtr->setPartitionResolver( mg_libpath.CharPtr,
          mg_factoryFunctionName.CharPtr );
      }

      // EXPIRATION ATTRIBUTES

      void AttributesFactory::SetEntryIdleTimeout( ExpirationAction action, uint32_t idleTimeout )
      {
        NativePtr->setEntryIdleTimeout(
          static_cast<gemfire::ExpirationAction::Action>( action ), idleTimeout );
      }

      void AttributesFactory::SetEntryTimeToLive( ExpirationAction action, uint32_t timeToLive )
      {
        NativePtr->setEntryTimeToLive(
          static_cast<gemfire::ExpirationAction::Action>( action ), timeToLive );
      }

      void AttributesFactory::SetRegionIdleTimeout( ExpirationAction action, uint32_t idleTimeout )
      {
        NativePtr->setRegionIdleTimeout(
          static_cast<gemfire::ExpirationAction::Action>( action ), idleTimeout );
      }

      void AttributesFactory::SetRegionTimeToLive( ExpirationAction action, uint32_t timeToLive )
      {
        NativePtr->setRegionTimeToLive(
          static_cast<gemfire::ExpirationAction::Action>( action ), timeToLive );
      }

      // PERSISTENCE

      void AttributesFactory::SetPersistenceManager( String^ libPath,
        String^ factoryFunctionName )
      {
        SetPersistenceManager( libPath, factoryFunctionName, nullptr );
      }

      void AttributesFactory::SetPersistenceManager( String^ libPath,
        String^ factoryFunctionName, Properties^ config )
      {
        ManagedString mg_libpath( libPath );
        ManagedString mg_factoryFunctionName( factoryFunctionName );
        gemfire::PropertiesPtr configptr(
          GetNativePtr<gemfire::Properties>( config ) );

        NativePtr->setPersistenceManager( mg_libpath.CharPtr,
          mg_factoryFunctionName.CharPtr, configptr );
      }

      // DISTRIBUTION ATTRIBUTES

      void AttributesFactory::SetScope( ScopeType scopeType )
      {
        NativePtr->setScope(
          static_cast<gemfire::ScopeType::Scope>( scopeType ) );
      }

      // STORAGE ATTRIBUTES

      void AttributesFactory::SetClientNotificationEnabled(
        bool clientNotificationEnabled )
      {
        NativePtr->setClientNotificationEnabled( clientNotificationEnabled );
      }

      void AttributesFactory::SetEndpoints( String^ endpoints )
      {
        ManagedString mg_endpoints( endpoints );

        NativePtr->setEndpoints( mg_endpoints.CharPtr );
      }

      void AttributesFactory::SetPoolName( String^ poolName )
      {
        ManagedString mg_poolName( poolName );

        NativePtr->setPoolName( mg_poolName.CharPtr );
      }

      // MAP ATTRIBUTES

      void AttributesFactory::SetInitialCapacity( int32_t initialCapacity )
      {
        _GF_MG_EXCEPTION_TRY

          NativePtr->setInitialCapacity( initialCapacity );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void AttributesFactory::SetLoadFactor( Single loadFactor )
      {
        _GF_MG_EXCEPTION_TRY

          NativePtr->setLoadFactor( loadFactor );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void AttributesFactory::SetConcurrencyLevel( int32_t concurrencyLevel )
      {
        _GF_MG_EXCEPTION_TRY

          NativePtr->setConcurrencyLevel( concurrencyLevel );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void AttributesFactory::SetLruEntriesLimit( uint32_t entriesLimit )
      {
        NativePtr->setLruEntriesLimit( entriesLimit );
      }

      void AttributesFactory::SetDiskPolicy( DiskPolicyType diskPolicy )
      {
        NativePtr->setDiskPolicy(
          static_cast<gemfire::DiskPolicyType::PolicyType>( diskPolicy ) );
      }

      void AttributesFactory::SetCachingEnabled( bool cachingEnabled )
      {
        NativePtr->setCachingEnabled( cachingEnabled );
      }

      void AttributesFactory::SetCloningEnabled( bool cloningEnabled )
      {
        NativePtr->setCloningEnabled( cloningEnabled );
      }
      
      void AttributesFactory::SetConcurrencyChecksEnabled( bool concurrencyChecksEnabled )
      {
        NativePtr->setConcurrencyChecksEnabled( concurrencyChecksEnabled );
      }

      // FACTORY METHOD 

      RegionAttributes^ AttributesFactory::CreateRegionAttributes( )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::RegionAttributesPtr& nativeptr (
            NativePtr->createRegionAttributes( ) );
          return RegionAttributes::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

    }
  }
}
