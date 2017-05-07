/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "CacheableStringArrayM.hpp"
#include "CacheableStringM.hpp"
#include "cppcache/DataInput.hpp"
#include "cppcache/DataOutput.hpp"
#include "GemFireClassIdsM.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      CacheableStringArray::CacheableStringArray(array<String^>^ strings)
        : GemStone::GemFire::Cache::Serializable()
      {
        if (strings != nullptr) 
        {
          m_value = gcnew array<CacheableString^>(strings->Length);

          for( int i = 0; i< strings->Length; i++)
          {
            m_value[i] = CacheableString::Create(strings[i]);
          }
        }
        else
          m_value = nullptr;
      }

      CacheableStringArray::CacheableStringArray(
        array<CacheableString^>^ strings) : GemStone::GemFire::Cache::Serializable()
      {
        m_value = strings;
      }

      array<CacheableString^>^ CacheableStringArray::GetValues()
      {
        return m_value;
      }

      String^ CacheableStringArray::default::get(int32_t index)
      {
        return m_value[index]->Value;
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
          m_value = gcnew array<CacheableString^>(len);
          if (len > 0)
          {
            for( int i = 0; i < len; i++)
            {
              m_value[i] = dynamic_cast<CacheableString^>(input->ReadObject());
            }
          }
          return this;
        }
      }
      
    }
  }
}
