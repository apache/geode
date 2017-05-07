/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "CacheableVectorM.hpp"
#include "DataOutputM.hpp"
#include "DataInputM.hpp"


using namespace System;
using namespace System::Collections::Generic;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      // Region: IGFSerializable Members

      void CacheableVector::ToData(DataOutput^ output)
      {
        output->WriteArrayLen(this->Count);
        for each (IGFSerializable^ obj in this) {
          output->WriteObject(obj);
        }
      }

      IGFSerializable^ CacheableVector::FromData(DataInput^ input)
      {
        int len = input->ReadArrayLen();
        for( int i = 0; i < len; i++)
        {
          this->Add(input->ReadObject());
        }
        return this;
      }

      uint32_t CacheableVector::ObjectSize::get()
      { 
        uint32_t size = static_cast<uint32_t> (sizeof(CacheableVector^));
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
