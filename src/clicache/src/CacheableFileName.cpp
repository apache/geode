/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*=========================================================================
*/



//#include "gf_includes.hpp"
#include "CacheableFileName.hpp"
#include "DataOutput.hpp"
#include "DataInput.hpp"
#include "GemFireClassIds.hpp"
using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      void CacheableFileName::ToData(DataOutput^ output)
      {
        if (m_str->Length <= 0xFFFF) {
          output->WriteByte(gemfire::GemfireTypeIds::CacheableString);
          output->WriteUTF(m_str);
        }
        else {
          output->WriteByte(gemfire::GemfireTypeIds::CacheableStringHuge);
          output->WriteUTFHuge(m_str);
        }
      }

      IGFSerializable^ CacheableFileName::FromData(DataInput^ input)
      {
        unsigned char filetype = input->ReadByte();
        if (filetype == gemfire::GemfireTypeIds::CacheableString) {
          m_str = input->ReadUTF();
        }
        else {
          m_str = input->ReadUTFHuge();
        }
        return this;
      }

      uint32_t CacheableFileName::ClassId::get()
      {
        return GemFireClassIds::CacheableFileName;
      }

      uint32_t CacheableFileName::ObjectSize::get()
      {
        return (uint32_t)(m_str->Length * sizeof(char));
      }

      int32_t CacheableFileName::GetHashCode()
      {
        if (m_str->IsNullOrEmpty(m_str)) {
          return 0;
        }
        if (m_hashcode == 0) {
          int localHashcode = 0;          
          uint32_t prime = 31;

          pin_ptr<const wchar_t> pin_value = PtrToStringChars( m_str );
          for (int32_t i = 0; i < m_str->Length; i++) {
            localHashcode = prime*localHashcode + Char::ToLower(pin_value[i]);
          }    
          m_hashcode  = localHashcode ^ 1234321;
        }
        return m_hashcode;
      }

      bool CacheableFileName::Equals(ICacheableKey^ other)
      {
        if (other == nullptr ||
          other->ClassId != GemFireClassIds::CacheableFileName) {
          return false;
        }
        return (m_str == static_cast<CacheableFileName^>(other)->m_str);
      }

      bool CacheableFileName::Equals(Object^ obj)
      {
        CacheableFileName^ otherFileName =
          dynamic_cast<CacheableFileName^>(obj);

        if (otherFileName != nullptr) {
          return (m_str == otherFileName->m_str);
        }
        return false;
      }
    }
  }
}
 } //namespace 

