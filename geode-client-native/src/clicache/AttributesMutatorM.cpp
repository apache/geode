/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "AttributesMutatorM.hpp"
#include "RegionM.hpp"
#include "impl/ManagedCacheListener.hpp"
#include "impl/ManagedCacheLoader.hpp"
#include "impl/ManagedCacheWriter.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      int32_t AttributesMutator::SetEntryIdleTimeout( int32_t idleTimeout )
      {
        return NativePtr->setEntryIdleTimeout( idleTimeout );
      }

      ExpirationAction AttributesMutator::SetEntryIdleTimeoutAction(
        ExpirationAction action )
      {
        return static_cast<ExpirationAction>(
          NativePtr->setEntryIdleTimeoutAction(
          static_cast<gemfire::ExpirationAction::Action>( action ) ) );
      }

      int32_t AttributesMutator::SetEntryTimeToLive( int32_t timeToLive )
      {
        return NativePtr->setEntryTimeToLive( timeToLive );
      }

      ExpirationAction AttributesMutator::SetEntryTimeToLiveAction(
        ExpirationAction action )
      {
        return static_cast<ExpirationAction>(
          NativePtr->setEntryTimeToLiveAction(
          static_cast<gemfire::ExpirationAction::Action>( action ) ) );
      }

      int32_t AttributesMutator::SetRegionIdleTimeout( int32_t idleTimeout )
      {
        return NativePtr->setRegionIdleTimeout( idleTimeout );
      }

      ExpirationAction AttributesMutator::SetRegionIdleTimeoutAction(
        ExpirationAction action )
      {
        return static_cast<ExpirationAction>(
          NativePtr->setRegionIdleTimeoutAction(
          static_cast<gemfire::ExpirationAction::Action>( action ) ) );
      }

      int32_t AttributesMutator::SetRegionTimeToLive( int32_t timeToLive )
      {
        return NativePtr->setRegionTimeToLive( timeToLive );
      }

      ExpirationAction AttributesMutator::SetRegionTimeToLiveAction(
        ExpirationAction action )
      {
        return static_cast<ExpirationAction>(
          NativePtr->setRegionTimeToLiveAction(
          static_cast<gemfire::ExpirationAction::Action>( action ) ) );
      }

      uint32_t AttributesMutator::SetLruEntriesLimit( uint32_t entriesLimit )
      {
        return NativePtr->setLruEntriesLimit( entriesLimit );
      }

      void AttributesMutator::SetCacheListener( ICacheListener^ cacheListener )
      {
        gemfire::CacheListenerPtr listenerptr;
        if (cacheListener != nullptr)
        {
          listenerptr = new gemfire::ManagedCacheListener( cacheListener );
        }
        NativePtr->setCacheListener( listenerptr );
      }

      void AttributesMutator::SetCacheListener( String^ libPath,
        String^ factoryFunctionName )
      {
        ManagedString mg_libpath( libPath );
        ManagedString mg_factoryFunctionName( factoryFunctionName );

        NativePtr->setCacheListener( mg_libpath.CharPtr,
          mg_factoryFunctionName.CharPtr );
      }

      void AttributesMutator::SetCacheLoader( ICacheLoader^ cacheLoader )
      {
        gemfire::CacheLoaderPtr loaderptr;
        if (cacheLoader != nullptr)
        {
          loaderptr = new gemfire::ManagedCacheLoader( cacheLoader );
        }
        NativePtr->setCacheLoader( loaderptr );
      }

      void AttributesMutator::SetCacheLoader( String^ libPath,
        String^ factoryFunctionName )
      {
        ManagedString mg_libpath( libPath );
        ManagedString mg_factoryFunctionName( factoryFunctionName );

        NativePtr->setCacheLoader( mg_libpath.CharPtr,
          mg_factoryFunctionName.CharPtr );
      }

      void AttributesMutator::SetCacheWriter( ICacheWriter^ cacheWriter )
      {
        gemfire::CacheWriterPtr writerptr;
        if (cacheWriter != nullptr)
        {
          writerptr = new gemfire::ManagedCacheWriter( cacheWriter );
        }
        NativePtr->setCacheWriter( writerptr );
      }

      void AttributesMutator::SetCacheWriter( String^ libPath,
        String^ factoryFunctionName )
      {
        ManagedString mg_libpath( libPath );
        ManagedString mg_factoryFunctionName( factoryFunctionName );

        NativePtr->setCacheWriter( mg_libpath.CharPtr,
          mg_factoryFunctionName.CharPtr );
      }

    }
  }
}
