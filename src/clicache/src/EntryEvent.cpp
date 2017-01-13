/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "gf_includes.hpp"
#include "EntryEvent.hpp"
#include "Region.hpp"
#include "impl/SafeConvert.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      generic<class TKey, class TValue>
      EntryEvent<TKey, TValue>::EntryEvent(IRegion<TKey, TValue>^ region,
        TKey key, TValue oldValue,
        TValue newValue, Object^ aCallbackArgument,
        bool remoteOrigin)
        : UMWrap( )
      {
        //TODO:: from where this gets called
        /*gemfire::RegionPtr regionptr( GetNativePtr<gemfire::Region>( region ) );
        gemfire::CacheableKeyPtr keyptr( SafeMKeyConvert( key ) );
        gemfire::CacheablePtr oldptr( SafeMSerializableConvert( oldValue ) );
        gemfire::CacheablePtr newptr( SafeMSerializableConvert( newValue ) );
        gemfire::UserDataPtr callbackptr(SafeMSerializableConvert(
            aCallbackArgument));

        SetPtr(new gemfire::EntryEvent(regionptr, keyptr,
          oldptr, newptr, callbackptr, remoteOrigin), true);*/
      }

      generic<class TKey, class TValue>
      IRegion<TKey, TValue>^ EntryEvent<TKey, TValue>::Region::get( )
      {
        gemfire::RegionPtr& regionptr( NativePtr->getRegion( ) );
        return Generic::Region<TKey, TValue>::Create( regionptr.ptr( ) );
      }

      generic<class TKey, class TValue>
      TKey EntryEvent<TKey, TValue>::Key::get( )
      {
        gemfire::CacheableKeyPtr& keyptr( NativePtr->getKey( ) );
        return Serializable::GetManagedValueGeneric<TKey>( keyptr );
      }

      generic<class TKey, class TValue>
      TValue EntryEvent<TKey, TValue>::OldValue::get( )
      {
        gemfire::CacheablePtr& valptr( NativePtr->getOldValue( ) );
        return Serializable::GetManagedValueGeneric<TValue>( valptr );
      }

      generic<class TKey, class TValue>
      TValue EntryEvent<TKey, TValue>::NewValue::get( )
      {
        gemfire::CacheablePtr& valptr( NativePtr->getNewValue( ) );
        return Serializable::GetManagedValueGeneric<TValue>( valptr );
      }

      generic<class TKey, class TValue>
      Object^ EntryEvent<TKey, TValue>::CallbackArgument::get()
      {
        gemfire::UserDataPtr& valptr(NativePtr->getCallbackArgument());
        return Serializable::GetManagedValueGeneric<Object^>( valptr );
      }

      generic<class TKey, class TValue>
      bool EntryEvent<TKey, TValue>::RemoteOrigin::get( )
      {
        return NativePtr->remoteOrigin( );
      }

    }
  }
}
 } //namespace 
