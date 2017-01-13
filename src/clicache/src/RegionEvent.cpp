/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "gf_includes.hpp"
#include "RegionEvent.hpp"
#include "Region.hpp"
#include "IGFSerializable.hpp"
#include "impl/SafeConvert.hpp"
using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      generic<class TKey, class TValue>
      RegionEvent<TKey, TValue>::RegionEvent(Generic::IRegion<TKey, TValue>^ region,
        Object^ aCallbackArgument, bool remoteOrigin)
        : UMWrap( )
      {
        //TODO:: do we neeed this
        /*if ( region == nullptr ) {
          throw gcnew IllegalArgumentException( "RegionEvent.ctor(): "
            "null region passed" );
        }

        gemfire::UserDataPtr callbackptr(SafeMSerializableConvert(
            aCallbackArgument));

        SetPtr(new gemfire::RegionEvent(gemfire::RegionPtr(region->_NativePtr),
          callbackptr, remoteOrigin), true);*/
      }

      generic<class TKey, class TValue>
      IRegion<TKey, TValue>^ RegionEvent<TKey, TValue>::Region::get( )
      {
        gemfire::RegionPtr& regionptr( NativePtr->getRegion( ) );

        return Generic::Region<TKey, TValue>::Create( regionptr.ptr( ) );
      }

      generic<class TKey, class TValue>
      Object^ RegionEvent<TKey, TValue>::CallbackArgument::get()
      {
        gemfire::UserDataPtr& valptr(NativePtr->getCallbackArgument());
        return Serializable::GetManagedValueGeneric<Object^>( valptr );
      }

      generic<class TKey, class TValue>
      bool RegionEvent<TKey, TValue>::RemoteOrigin::get( )
      {
        return NativePtr->remoteOrigin( );
      }

    }
  }
}
 } //namespace 
