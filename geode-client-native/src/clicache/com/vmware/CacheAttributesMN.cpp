/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */



//#include "gf_includesN.hpp"
#include "CacheAttributesMN.hpp"
#include "impl/ManagedStringN.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      int32_t CacheAttributes::RedundancyLevel::get( )
      {
        return NativePtr->getRedundancyLevel( );
      }

      String^ CacheAttributes::Endpoints::get( )
      {
        return ManagedString::Get( NativePtr->getEndpoints( ) );
      }

    }
  }
}
 } //namespace 

