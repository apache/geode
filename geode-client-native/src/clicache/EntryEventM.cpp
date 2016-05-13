/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "EntryEventM.hpp"
#include "RegionM.hpp"
#include "impl/SafeConvert.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      EntryEvent::EntryEvent(GemStone::GemFire::Cache::Region^ region,
        GemStone::GemFire::Cache::ICacheableKey^ key, IGFSerializable^ oldValue,
        IGFSerializable^ newValue, IGFSerializable^ aCallbackArgument,
        bool remoteOrigin)
        : UMWrap( )
      {
        gemfire::RegionPtr regionptr( GetNativePtr<gemfire::Region>( region ) );
        gemfire::CacheableKeyPtr keyptr( SafeMKeyConvert( key ) );
        gemfire::CacheablePtr oldptr( SafeMSerializableConvert( oldValue ) );
        gemfire::CacheablePtr newptr( SafeMSerializableConvert( newValue ) );
        gemfire::UserDataPtr callbackptr(SafeMSerializableConvert(
            aCallbackArgument));

        SetPtr(new gemfire::EntryEvent(regionptr, keyptr,
          oldptr, newptr, callbackptr, remoteOrigin), true);
      }

      GemStone::GemFire::Cache::Region^ EntryEvent::Region::get( )
      {
        gemfire::RegionPtr& regionptr( NativePtr->getRegion( ) );
        return GemStone::GemFire::Cache::Region::Create( regionptr.ptr( ) );
      }

      GemStone::GemFire::Cache::ICacheableKey^ EntryEvent::Key::get( )
      {
        gemfire::CacheableKeyPtr& keyptr( NativePtr->getKey( ) );
        return SafeUMKeyConvert( keyptr.ptr( ) );
      }

      IGFSerializable^ EntryEvent::OldValue::get( )
      {
        gemfire::CacheablePtr& valptr( NativePtr->getOldValue( ) );
        return SafeUMSerializableConvert( valptr.ptr( ) );
      }

      IGFSerializable^ EntryEvent::NewValue::get( )
      {
        gemfire::CacheablePtr& valptr( NativePtr->getNewValue( ) );
        return SafeUMSerializableConvert( valptr.ptr( ) );
      }

      IGFSerializable^ EntryEvent::CallbackArgument::get()
      {
        gemfire::UserDataPtr& valptr(NativePtr->getCallbackArgument());
        return SafeUMSerializableConvert(valptr.ptr());
      }

      bool EntryEvent::RemoteOrigin::get( )
      {
        return NativePtr->remoteOrigin( );
      }

    }
  }
}
