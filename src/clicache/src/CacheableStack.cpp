/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */



//#include "gf_includes.hpp"
#include "CacheableStack.hpp"
#include "DataOutput.hpp"
#include "DataInput.hpp"
#include <GemfireTypeIdsImpl.hpp>
#include "impl/SafeConvert.hpp"
#include "GemFireClassIds.hpp"

using namespace System;
using namespace System::Collections::Generic;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      // Region: IGFSerializable Members

      void CacheableStack::ToData(DataOutput^ output)
      {
        if(m_stack != nullptr)
        {
          output->WriteArrayLen((int32_t)m_stack->Count);
          for each (Object^ obj in m_stack) {
					  output->WriteObject(obj);
          }
        }
        else
        {
          output->WriteByte(0xFF);
        }
      }

      IGFSerializable^ CacheableStack::FromData(DataInput^ input)
      {
        int len = input->ReadArrayLen();
        if (len > 0)
        {
          System::Collections::Generic::Stack<Object^>^ stack = safe_cast<System::Collections::Generic::Stack<Object^>^>(m_stack);
          for( int i = 0; i < len; i++)
          {
            (stack)->Push(input->ReadObject());
//            Push(input->ReadObject());
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
        //TODO:
        /*uint32_t size = static_cast<uint32_t> (sizeof(CacheableStack^));
        for each (IGFSerializable^ val in this) {
          if (val != nullptr) {
            size += val->ObjectSize;
          }
        }*/
        return m_stack->Count;
      }

      // End Region: IGFSerializable Members
    }
  }
}
 } //namespace 

