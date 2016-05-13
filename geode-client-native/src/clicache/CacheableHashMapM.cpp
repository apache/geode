/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "CacheableHashMapM.hpp"
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

      void CacheableHashMap::ToData(DataOutput^ output)
      {
        output->WriteArrayLen(this->Count);
        for each (KeyValuePair<GemStone::GemFire::Cache::ICacheableKey^, IGFSerializable^> keyValPair in this) 
        {
          output->WriteObject(keyValPair.Key);
          output->WriteObject(keyValPair.Value);
        }        
      }

      IGFSerializable^ CacheableHashMap::FromData(DataInput^ input)
      {
        int len = input->ReadArrayLen();
        if (len > 0)
        {
          for ( int i = 0; i < len; i++)
          {
            GemStone::GemFire::Cache::ICacheableKey^ key =
              dynamic_cast<GemStone::GemFire::Cache::ICacheableKey^>(input->ReadObject());
            IGFSerializable^ value = input->ReadObject();
            this->Add(key, value);
          }
        }
        return this;
      }

      uint32_t CacheableHashMap::ObjectSize::get()
      {
        uint32_t size = 0;
        for each (KeyValuePair<GemStone::GemFire::Cache::ICacheableKey^, IGFSerializable^> keyValPair
          in this) {
          size += keyValPair.Key->ObjectSize;
          if (keyValPair.Value != nullptr) {
            size += keyValPair.Value->ObjectSize;
          }
        }
        return size;
      }

      // End Region: IGFSerializable Members
    }
  }
}
