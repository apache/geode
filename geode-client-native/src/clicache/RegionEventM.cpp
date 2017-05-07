/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "RegionEventM.hpp"
#include "RegionM.hpp"
#include "impl/SafeConvert.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      RegionEvent::RegionEvent(GemStone::GemFire::Cache::Region^ region,
        IGFSerializable^ aCallbackArgument, bool remoteOrigin)
        : UMWrap( )
      {
        if ( region == nullptr ) {
          throw gcnew IllegalArgumentException( "RegionEvent.ctor(): "
            "null region passed" );
        }

        gemfire::UserDataPtr callbackptr(SafeMSerializableConvert(
            aCallbackArgument));

        SetPtr(new gemfire::RegionEvent(gemfire::RegionPtr(region->_NativePtr),
          callbackptr, remoteOrigin), true);
      }

      GemStone::GemFire::Cache::Region^ RegionEvent::Region::get( )
      {
        gemfire::RegionPtr& regionptr( NativePtr->getRegion( ) );

        return GemStone::GemFire::Cache::Region::Create( regionptr.ptr( ) );
      }

      IGFSerializable^ RegionEvent::CallbackArgument::get()
      {
        gemfire::UserDataPtr& valptr(NativePtr->getCallbackArgument());
        return SafeUMSerializableConvert(valptr.ptr());
      }

      bool RegionEvent::RemoteOrigin::get( )
      {
        return NativePtr->remoteOrigin( );
      }

    }
  }
}
