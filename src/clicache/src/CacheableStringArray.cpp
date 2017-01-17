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
