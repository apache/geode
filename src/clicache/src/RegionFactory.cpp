/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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


namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Generic
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

       generic <class TKey, class TValue>
      RegionFactory^ RegionFactory::SetPersistenceManager( Generic::IPersistenceManager<TKey, TValue>^ persistenceManager, 
          Properties<String^, String^>^ config)
      {
        gemfire::PersistenceManagerPtr persistenceManagerptr;
        if ( persistenceManager != nullptr ) {
          PersistenceManagerGeneric<TKey, TValue>^ clg = gcnew PersistenceManagerGeneric<TKey, TValue>();
          clg->SetPersistenceManager(persistenceManager);
          persistenceManagerptr = new gemfire::ManagedPersistenceManagerGeneric( /*clg,*/ persistenceManager);
          ((gemfire::ManagedPersistenceManagerGeneric*)persistenceManagerptr.ptr())->setptr(clg);
        }
        gemfire::PropertiesPtr configptr(GetNativePtr<gemfire::Properties>( config ) );
        NativePtr->setPersistenceManager( persistenceManagerptr, configptr );
        return this;
      }

      generic <class TKey, class TValue>
      RegionFactory^ RegionFactory::SetPersistenceManager( Generic::IPersistenceManager<TKey, TValue>^ persistenceManager )
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
      // NEW GENERIC APIs:

      generic <class TKey, class TValue>
      IRegion<TKey,TValue>^ RegionFactory::Create(String^ regionName)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

        ManagedString mg_name( regionName );
          
        gemfire::RegionPtr& nativeptr( NativePtr->create(
            mg_name.CharPtr ) );
          return Generic::Region<TKey,TValue>::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic <class TKey, class TValue>
      RegionFactory^ RegionFactory::SetCacheLoader( Generic::ICacheLoader<TKey, TValue>^ cacheLoader )
      {
        gemfire::CacheLoaderPtr loaderptr;
        if ( cacheLoader != nullptr ) {
          CacheLoaderGeneric<TKey, TValue>^ clg = gcnew CacheLoaderGeneric<TKey, TValue>();
          clg->SetCacheLoader(cacheLoader);
          loaderptr = new gemfire::ManagedCacheLoaderGeneric( /*clg,*/ cacheLoader );
          ((gemfire::ManagedCacheLoaderGeneric*)loaderptr.ptr())->setptr(clg);
        }
        NativePtr->setCacheLoader( loaderptr );
        return this;
      }

      generic <class TKey, class TValue>
      RegionFactory^ RegionFactory::SetCacheWriter( Generic::ICacheWriter<TKey, TValue>^ cacheWriter )
      {
        gemfire::CacheWriterPtr writerptr;
        if ( cacheWriter != nullptr ) {
          CacheWriterGeneric<TKey, TValue>^ cwg = gcnew CacheWriterGeneric<TKey, TValue>();
          cwg->SetCacheWriter(cacheWriter);
          writerptr = new gemfire::ManagedCacheWriterGeneric( /*cwg,*/ cacheWriter );
          ((gemfire::ManagedCacheWriterGeneric*)writerptr.ptr())->setptr(cwg);
        }
        NativePtr->setCacheWriter( writerptr );
        return this;
      }

      generic <class TKey, class TValue>
      RegionFactory^ RegionFactory::SetCacheListener( Generic::ICacheListener<TKey, TValue>^ cacheListener )
      {
        gemfire::CacheListenerPtr listenerptr;
        if ( cacheListener != nullptr ) {
          CacheListenerGeneric<TKey, TValue>^ clg = gcnew CacheListenerGeneric<TKey, TValue>();
          clg->SetCacheListener(cacheListener);
          listenerptr = new gemfire::ManagedCacheListenerGeneric( /*clg,*/ cacheListener );
          ((gemfire::ManagedCacheListenerGeneric*)listenerptr.ptr())->setptr(clg);
          /*
          listenerptr = new gemfire::ManagedCacheListenerGeneric(
            (Generic::ICacheListener<Object^, Object^>^)cacheListener);
            */
        }
        NativePtr->setCacheListener( listenerptr );
        return this;
      }

      generic <class TKey, class TValue>
      RegionFactory^ RegionFactory::SetPartitionResolver( Generic::IPartitionResolver<TKey, TValue>^ partitionresolver )
      {
        gemfire::PartitionResolverPtr resolverptr;
        if ( partitionresolver != nullptr ) {
          Generic::IFixedPartitionResolver<TKey, TValue>^ resolver = 
            dynamic_cast<Generic::IFixedPartitionResolver<TKey, TValue>^>(partitionresolver);
          if (resolver != nullptr) {            
            FixedPartitionResolverGeneric<TKey, TValue>^ prg = gcnew FixedPartitionResolverGeneric<TKey, TValue>();
            prg->SetPartitionResolver(resolver);
            resolverptr = new gemfire::ManagedFixedPartitionResolverGeneric( resolver ); 
            ((gemfire::ManagedFixedPartitionResolverGeneric*)resolverptr.ptr())->setptr(prg);
          }
          else {            
            PartitionResolverGeneric<TKey, TValue>^ prg = gcnew PartitionResolverGeneric<TKey, TValue>();
            prg->SetPartitionResolver(partitionresolver);
            resolverptr = new gemfire::ManagedPartitionResolverGeneric( partitionresolver );
            ((gemfire::ManagedPartitionResolverGeneric*)resolverptr.ptr())->setptr(prg);            
          }         
        }
        NativePtr->setPartitionResolver( resolverptr );
        return this;
      }
      } // end namespace Generic
    }
  }
}
