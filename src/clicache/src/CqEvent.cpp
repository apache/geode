/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "gf_includes.hpp"
#include "CqEvent.hpp"
#include "Log.hpp"
#include "impl/SafeConvert.hpp"
#include "CacheableBuiltins.hpp"
using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      generic<class TKey, class TResult>
      CqQuery<TKey, TResult>^ CqEvent<TKey, TResult>::getCq( )
      {
        gemfire::CqQueryPtr& cQueryptr( NativePtr->getCq( ) );
        return CqQuery<TKey, TResult>::Create( cQueryptr.ptr( ) );
      }

      generic<class TKey, class TResult>
      CqOperationType CqEvent<TKey, TResult>::getBaseOperation( )
      {
		  return CqOperation::ConvertFromNative(NativePtr->getBaseOperation());
      }

      generic<class TKey, class TResult>
      CqOperationType CqEvent<TKey, TResult>::getQueryOperation( )
      {
        return CqOperation::ConvertFromNative(NativePtr->getQueryOperation());
      }

      generic<class TKey, class TResult>
      TKey CqEvent<TKey, TResult>::getKey( )
      {
        gemfire::CacheableKeyPtr& keyptr( NativePtr->getKey( ) );
        return Serializable::GetManagedValueGeneric<TKey>(keyptr);
      }

      generic<class TKey, class TResult>
      TResult CqEvent<TKey, TResult>::getNewValue( )
      {
        gemfire::CacheablePtr& valptr( NativePtr->getNewValue( ) );
        return Serializable::GetManagedValueGeneric<TResult>(valptr);
      }

      generic<class TKey, class TResult>
      array< Byte >^ CqEvent<TKey, TResult>::getDeltaValue( )
      {
        gemfire::CacheableBytesPtr deltaBytes = NativePtr->getDeltaValue( );
        CacheableBytes^ managedDeltaBytes = ( CacheableBytes^ ) CacheableBytes::Create( deltaBytes.ptr( ) );
        return ( array< Byte >^ ) managedDeltaBytes;
      }


    }
  }
}
 } //namespace 
