/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */




//#include "gf_includes.hpp"
#include "CacheableFileName.hpp"
#include "DataOutput.hpp"
#include "DataInput.hpp"
#include "GeodeClassIds.hpp"
using namespace System;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      void CacheableFileName::ToData(DataOutput^ output)
      {
        if (m_str->Length <= 0xFFFF) {
          output->WriteByte(apache::geode::client::GeodeTypeIds::CacheableString);
          output->WriteUTF(m_str);
        }
        else {
          output->WriteByte(apache::geode::client::GeodeTypeIds::CacheableStringHuge);
          output->WriteUTFHuge(m_str);
        }
      }

      IGFSerializable^ CacheableFileName::FromData(DataInput^ input)
      {
        unsigned char filetype = input->ReadByte();
        if (filetype == apache::geode::client::GeodeTypeIds::CacheableString) {
          m_str = input->ReadUTF();
        }
        else {
          m_str = input->ReadUTFHuge();
        }
        return this;
      }

      uint32_t CacheableFileName::ClassId::get()
      {
        return GeodeClassIds::CacheableFileName;
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

          pin_ptr<const wchar_t> pin_value = PtrToStringChars(m_str);
          for (int32_t i = 0; i < m_str->Length; i++) {
            localHashcode = prime*localHashcode + Char::ToLower(pin_value[i]);
          }
          m_hashcode = localHashcode ^ 1234321;
        }
        return m_hashcode;
      }

      bool CacheableFileName::Equals(ICacheableKey^ other)
      {
        if (other == nullptr ||
            other->ClassId != GeodeClassIds::CacheableFileName) {
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
      }  // namespace Client
    }  // namespace Geode
  }  // namespace Apache

} //namespace 

