/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "CacheableStackM.hpp"
#include "DataOutputM.hpp"
#include "DataInputM.hpp"
#include "cppcache/impl/GemfireTypeIdsImpl.hpp"
#include "GemFireClassIdsM.hpp"

using namespace System;
using namespace System::Collections::Generic;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      // Region: IGFSerializable Members

      void CacheableStack::ToData(DataOutput^ output)
      {
        output->WriteArrayLen((int32_t)Count);
        for each (IGFSerializable^ obj in this) {
          output->WriteObject(obj);
        }
      }

      IGFSerializable^ CacheableStack::FromData(DataInput^ input)
      {
        int len = input->ReadArrayLen();
        if (len > 0)
        {
          for( int i = 0; i < len; i++)
          {
            Push(input->ReadObject());
          }
        }
        return this;
      }

      uint32_t CacheableStack::ClassId::get()
      {
        return GemFireClassIds::CacheableStack;
      }

      uint32_t CacheableStack::ObjectSize::get()
      { 
        uint32_t size = static_cast<uint32_t> (sizeof(CacheableStack^));
        for each (IGFSerializable^ val in this) {
          if (val != nullptr) {
            size += val->ObjectSize;
          }
        }
        return size;
      }

      // End Region: IGFSerializable Members
    }
  }
}
