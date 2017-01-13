/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */



//#include "gf_includes.hpp"
#include "CacheableUndefined.hpp"
#include "DataOutput.hpp"
#include "DataInput.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      // Region: IGFSerializable Members

      void CacheableUndefined::ToData(DataOutput^ output)
      {
      }

      IGFSerializable^ CacheableUndefined::FromData(DataInput^ input)
      {
        return this;
      }

      uint32_t CacheableUndefined::ObjectSize::get()
      {
        return static_cast<uint32_t> (sizeof(CacheableUndefined^));
      }

      // End Region: IGFSerializable Members
    }
  }
}
 } //namespace 
