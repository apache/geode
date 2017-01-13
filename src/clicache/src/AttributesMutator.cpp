/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "gf_includes.hpp"
#include "AttributesMutator.hpp"
//#include "Region.hpp"
#include "impl/ManagedCacheListener.hpp"
#include "impl/ManagedCacheLoader.hpp"
#include "impl/ManagedCacheWriter.hpp"
#include "impl/CacheLoader.hpp"
#include "impl/CacheWriter.hpp"
#include "impl/CacheListener.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      generic<class TKey, class TValue>
      int32_t AttributesMutator<TKey, TValue>::SetEntryIdleTimeout( int32_t idleTimeout )
      {
        return NativePtr->setEntryIdleTimeout( idleTimeout );
      }

      generic<class TKey, class TValue>
      ExpirationAction AttributesMutator<TKey, TValue>::SetEntryIdleTimeoutAction(
        ExpirationAction action )
      {
        return static_cast<ExpirationAction>(
          NativePtr->setEntryIdleTimeoutAction(
          static_cast<gemfire::ExpirationAction::Action>( action ) ) );
      }

      generic<class TKey, class TValue>
      int32_t AttributesMutator<TKey, TValue>::SetEntryTimeToLive( int32_t timeToLive )
      {
        return NativePtr->setEntryTimeToLive( timeToLive );
      }

      generic<class TKey, class TValue>
      ExpirationAction AttributesMutator<TKey, TValue>::SetEntryTimeToLiveAction(
        ExpirationAction action )
      {
        return static_cast<ExpirationAction>(
          NativePtr->setEntryTimeToLiveAction(
          static_cast<gemfire::ExpirationAction::Action>( action ) ) );
      }

      generic<class TKey, class TValue>
      int32_t AttributesMutator<TKey, TValue>::SetRegionIdleTimeout( int32_t idleTimeout )
      {
        return NativePtr->setRegionIdleTimeout( idleTimeout );
      }

      generic<class TKey, class TValue>
      ExpirationAction AttributesMutator<TKey, TValue>::SetRegionIdleTimeoutAction(
        ExpirationAction action )
      {
        return static_cast<ExpirationAction>(
          NativePtr->setRegionIdleTimeoutAction(
          static_cast<gemfire::ExpirationAction::Action>( action ) ) );
      }

      generic<class TKey, class TValue>
      int32_t AttributesMutator<TKey, TValue>::SetRegionTimeToLive( int32_t timeToLive )
      {
        return NativePtr->setRegionTimeToLive( timeToLive );
      }

      generic<class TKey, class TValue>
      ExpirationAction AttributesMutator<TKey, TValue>::SetRegionTimeToLiveAction(
        ExpirationAction action )
      {
        return static_cast<ExpirationAction>(
          NativePtr->setRegionTimeToLiveAction(
          static_cast<gemfire::ExpirationAction::Action>( action ) ) );
      }

      generic<class TKey, class TValue>
      uint32_t AttributesMutator<TKey, TValue>::SetLruEntriesLimit( uint32_t entriesLimit )
      {
        return NativePtr->setLruEntriesLimit( entriesLimit );
      }

      generic<class TKey, class TValue>
      void AttributesMutator<TKey, TValue>::SetCacheListener( ICacheListener<TKey, TValue>^ cacheListener )
      {
        gemfire::CacheListenerPtr listenerptr;
        if (cacheListener != nullptr)
        {
          CacheListenerGeneric<TKey, TValue>^ clg = gcnew CacheListenerGeneric<TKey, TValue>();
          clg->SetCacheListener(cacheListener);
          listenerptr = new gemfire::ManagedCacheListenerGeneric( /*clg,*/ cacheListener );
          ((gemfire::ManagedCacheListenerGeneric*)listenerptr.ptr())->setptr(clg);
        }
        NativePtr->setCacheListener( listenerptr );
      }

      generic<class TKey, class TValue>
      void AttributesMutator<TKey, TValue>::SetCacheListener( String^ libPath,
        String^ factoryFunctionName )
      {
        throw gcnew System::NotSupportedException;
        ManagedString mg_libpath( libPath );
        ManagedString mg_factoryFunctionName( factoryFunctionName );

        NativePtr->setCacheListener( mg_libpath.CharPtr,
          mg_factoryFunctionName.CharPtr );
      }

      generic<class TKey, class TValue>
      void AttributesMutator<TKey, TValue>::SetCacheLoader( ICacheLoader<TKey, TValue>^ cacheLoader )
      {
        gemfire::CacheLoaderPtr loaderptr;
        if (cacheLoader != nullptr)
        {
          CacheLoaderGeneric<TKey, TValue>^ clg = gcnew CacheLoaderGeneric<TKey, TValue>();
          clg->SetCacheLoader(cacheLoader);
          loaderptr = new gemfire::ManagedCacheLoaderGeneric( /*clg,*/ cacheLoader );
          ((gemfire::ManagedCacheLoaderGeneric*)loaderptr.ptr())->setptr(clg);
        }
        NativePtr->setCacheLoader( loaderptr );
      }

      generic<class TKey, class TValue>
      void AttributesMutator<TKey, TValue>::SetCacheLoader( String^ libPath,
        String^ factoryFunctionName )
      {
        throw gcnew System::NotSupportedException;
        ManagedString mg_libpath( libPath );
        ManagedString mg_factoryFunctionName( factoryFunctionName );

        NativePtr->setCacheLoader( mg_libpath.CharPtr,
          mg_factoryFunctionName.CharPtr );
      }

      generic<class TKey, class TValue>
      void AttributesMutator<TKey, TValue>::SetCacheWriter( ICacheWriter<TKey, TValue>^ cacheWriter )
      {
        gemfire::CacheWriterPtr writerptr;
        if (cacheWriter != nullptr)
        {
          CacheWriterGeneric<TKey, TValue>^ cwg = gcnew CacheWriterGeneric<TKey, TValue>();
          cwg->SetCacheWriter(cacheWriter);
          writerptr = new gemfire::ManagedCacheWriterGeneric( /*cwg,*/ cacheWriter );
          ((gemfire::ManagedCacheWriterGeneric*)writerptr.ptr())->setptr(cwg);
        }
        NativePtr->setCacheWriter( writerptr );
      }

      generic<class TKey, class TValue>
      void AttributesMutator<TKey, TValue>::SetCacheWriter( String^ libPath,
        String^ factoryFunctionName )
      {
        throw gcnew System::NotSupportedException;
        ManagedString mg_libpath( libPath );
        ManagedString mg_factoryFunctionName( factoryFunctionName );

        NativePtr->setCacheWriter( mg_libpath.CharPtr,
          mg_factoryFunctionName.CharPtr );
      }

    }
  }
}
 } //namespace 
