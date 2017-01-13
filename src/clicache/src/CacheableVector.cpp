/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */



//#include "gf_includes.hpp"
#include "CacheableVector.hpp"
#include "DataOutput.hpp"
#include "DataInput.hpp"
#include "ExceptionTypes.hpp"
#include "impl/SafeConvert.hpp"

using namespace System;
using namespace System::Collections::Generic;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      // Region: IGFSerializable Members

      void CacheableVector::ToData(DataOutput^ output)
      {
        if(m_arrayList != nullptr)
        {
          output->WriteArrayLen(m_arrayList->Count);
          for each (Object^ obj in m_arrayList) {
						//TODO::split
            output->WriteObject(obj);
          }
        }
        else
          output->WriteByte(0xFF);
      }

      IGFSerializable^ CacheableVector::FromData(DataInput^ input)
      {
        int len = input->ReadArrayLen();
        for( int i = 0; i < len; i++)
        {
          m_arrayList->Add(input->ReadObject());
        }
        return this;
      }

      uint32_t CacheableVector::ObjectSize::get()
      { 
        //TODO::
        /*uint32_t size = static_cast<uint32_t> (sizeof(CacheableVector^));
        for each (IGFSerializable^ val in this) {
          if (val != nullptr) {
            size += val->ObjectSize;
          }
        }*/
        return m_arrayList->Count;
      }

      // End Region: IGFSerializable Members
    }
  }
}
 } //namespace 

