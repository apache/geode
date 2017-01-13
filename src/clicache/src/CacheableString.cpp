/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*=========================================================================
*/


#include "DataOutput.hpp"
#include "DataInput.hpp"

//#include "gf_includes.hpp"
#include "CacheableString.hpp"
#include "ExceptionTypes.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      void CacheableString::ToData(DataOutput^ output)
      {
        if (m_type == GemFireClassIds::CacheableASCIIString ||
            m_type == GemFireClassIds::CacheableString)
        {
          output->WriteUTF(m_value);
        }
        else if (m_type == GemFireClassIds::CacheableASCIIStringHuge)
        {
          output->WriteASCIIHuge(m_value);
        }
        else
        {
          output->WriteUTFHuge(m_value);
        }
      }

      IGFSerializable^ CacheableString::FromData(DataInput^ input) 
      {
        if (m_type == GemFireClassIds::CacheableASCIIString ||
            m_type == GemFireClassIds::CacheableString)
        {
          m_value = input->ReadUTF();
        }
        else if (m_type == GemFireClassIds::CacheableASCIIStringHuge)
        {
          m_value = input->ReadASCIIHuge();
        }
        else 
        {
          m_value = input->ReadUTFHuge();
        }

        return this;
      }


      inline void CacheableString::GetCacheableString(String^ value,
        gemfire::CacheableStringPtr& cStr)
      {
        size_t len;
        if (value != nullptr && (len = value->Length) > 0) {
          pin_ptr<const wchar_t> pin_value = PtrToStringChars(value);
          cStr = gemfire::CacheableString::create(pin_value, (int32_t)len);
        }
        else {
          cStr = (gemfire::CacheableString*)
            gemfire::CacheableString::createDeserializable();
        }
      }

      inline void CacheableString::GetCacheableString(array<Char>^ value,
        gemfire::CacheableStringPtr& cStr)
      {
        size_t len;
        if (value != nullptr && (len = value->Length) > 0) {
          pin_ptr<const Char> pin_value = &value[0];
          cStr = gemfire::CacheableString::create(
            (const wchar_t*)pin_value, (int32_t)len);
        }
        else {
          cStr = (gemfire::CacheableString*)
            gemfire::CacheableString::createDeserializable();
        }
      }

      CacheableString::CacheableString(String^ value)
        : CacheableKey()
      {
        if (value == nullptr ) {
          throw gcnew IllegalArgumentException("CacheableString: null or " +
            "zero-length string provided to the constructor.");
        }
        m_value = value;

        this->SetStringType();
      }

      CacheableString::CacheableString(array<Char>^ value)
        : CacheableKey()
      {
        if (value == nullptr ) {
          throw gcnew IllegalArgumentException("CacheableString: null or " +
            "zero-length character array provided to the constructor.");
        }
        m_value = gcnew String(value);
       
        this->SetStringType();
      }

      CacheableString::CacheableString(String^ value, bool noParamCheck)
        : CacheableKey()
      {
        m_value = value;
        this->SetStringType();
      }

      CacheableString::CacheableString(array<Char>^ value, bool noParamCheck)
        : CacheableKey()
      {
        m_value = gcnew String(value);
        this->SetStringType();
      }

      uint32_t CacheableString::ObjectSize::get()
      {
        return (m_value->Length * sizeof(char));
      }

      bool CacheableString::Equals(GemStone::GemFire::Cache::Generic::ICacheableKey^ other)
      {
        if (other == nullptr || other->ClassId != ClassId) {
          return false;
        }

        CacheableString^ otherStr =
          dynamic_cast<CacheableString^>(other);

        if (otherStr == nullptr)
          return false;

        return m_value->Equals(otherStr->Value);//TODO::
      }

      bool CacheableString::Equals(Object^ obj)
      {
        CacheableString^ otherStr =
          dynamic_cast<CacheableString^>(obj);

        if (otherStr != nullptr) {
          return m_value->Equals(otherStr->Value);
        }
        return false;
      }

      int32_t CacheableString::GetHashCode()
      {
        if (String::IsNullOrEmpty(m_value)) {
          return 0;
        }
        //TODO: need to need java hashcode
        //return m_value->GetHashCode();
        if(m_hashcode == 0) 
        {
          int32_t prime = 31;
          int32_t localHash = 0;
          for (int32_t i = 0; i < m_value->Length; i++) 
            localHash = prime*localHash +  m_value[i];
          m_hashcode = localHash;
        }
        return m_hashcode;
      }

      void CacheableString::SetStringType()
      {
        int len = DataOutput::getEncodedLength(m_value);
        
        if (len == m_value->Length)//ASCII string
        {
          if (len > 0xFFFF)
            m_type = GemFireClassIds::CacheableASCIIStringHuge;
          else
            m_type = GemFireClassIds::CacheableASCIIString;
        }
        else
        {
          if (len > 0xFFFF)
            m_type = GemFireClassIds::CacheableStringHuge;
          else
            m_type = GemFireClassIds::CacheableString;  
        }
      }
    }
  }
}
 } //namespace 

