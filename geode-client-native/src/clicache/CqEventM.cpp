/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "CqEventM.hpp"
#include "LogM.hpp"
#include "impl/SafeConvert.hpp"
#include "CacheableBuiltinsM.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      CqQuery^ CqEvent::getCq( )
      {
        gemfire::CqQueryPtr& cQueryptr( NativePtr->getCq( ) );
        return CqQuery::Create( cQueryptr.ptr( ) );
      }

      CqOperationType CqEvent::getBaseOperation( )
      {
		  return CqOperation::ConvertFromNative(NativePtr->getBaseOperation());
      }

      CqOperationType CqEvent::getQueryOperation( )
      {
        return CqOperation::ConvertFromNative(NativePtr->getQueryOperation());
      }

      GemStone::GemFire::Cache::ICacheableKey^ CqEvent::getKey( )
      {
        gemfire::CacheableKeyPtr& keyptr( NativePtr->getKey( ) );
        return SafeUMKeyConvert( keyptr.ptr( ) );
      }

      IGFSerializable^ CqEvent::getNewValue( )
      {
        gemfire::CacheablePtr& valptr( NativePtr->getNewValue( ) );
        return SafeUMSerializableConvert( valptr.ptr( ) );
      }

      array< Byte >^ CqEvent::getDeltaValue( )
      {
        gemfire::CacheableBytesPtr deltaBytes = NativePtr->getDeltaValue( );
        GemStone::GemFire::Cache::CacheableBytes^ managedDeltaBytes = ( GemStone::GemFire::Cache::CacheableBytes^ ) GemStone::GemFire::Cache::CacheableBytes::Create( deltaBytes.ptr( ) );
        return ( array< Byte >^ ) managedDeltaBytes;
      }


    }
  }
}
