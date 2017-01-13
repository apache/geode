/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */


#include "CacheableStringArray.hpp"
#include "CacheableString.hpp"
#include "DataInput.hpp"
#include "DataOutput.hpp"
#include "ExceptionTypes.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      CacheableStringArray::CacheableStringArray(array<String^>^ strings)
        : Serializable()
      {
        m_value = strings;
      }

      
      array<String^>^ CacheableStringArray::GetValues()
      {
        return m_value;
      }

      String^ CacheableStringArray::default::get(int32_t index)
      {
        return m_value[index];
      }

      void CacheableStringArray::ToData(DataOutput^ output) 
      {
        if (m_value == nullptr)
        {
          output->WriteArrayLen(-1);
        }
        else
        {
          output->WriteArrayLen(m_value->Length);
          if (m_value->Length > 0)
          {
            for(int i = 0; i < m_value->Length; i++)
            {
              output->WriteObject(m_value[i]);
            }
            GC::KeepAlive(this);
          }
		    }
      }
        
    
      IGFSerializable^ CacheableStringArray::FromData(DataInput^ input)
      {
        int len = input->ReadArrayLen();
        if ( len == -1)
        {
          m_value = nullptr;
          return nullptr;
        }
        else 
        {
          m_value = gcnew array<String^>(len);
          if (len > 0)
          {
            for( int i = 0; i < len; i++)
            {
              m_value[i] = dynamic_cast<String^>(input->ReadObject());
            }
          }
          return this;
        }
      }

    }
  }
}
 } //namespace 
