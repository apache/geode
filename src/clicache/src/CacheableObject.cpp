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
#include "CacheableObject.hpp"
#include "DataInput.hpp"
#include "DataOutput.hpp"
#include "impl/GFNullStream.hpp"
#include "impl/GFDataInputStream.hpp"
#include "impl/GFDataOutputStream.hpp"

using namespace System;
using namespace System::IO;
using namespace System::Runtime::Serialization::Formatters::Binary;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {
namespace Generic
    {
      void CacheableObject::ToData(DataOutput^ output)
      {
        if(m_obj != nullptr)
        {
          output->AdvanceCursor(4); // placeholder for object size bytes needed while reading back.

          GFDataOutputStream dos(output);
          BinaryFormatter bf;
          int64_t checkpoint = dos.Length;
          bf.Serialize(%dos, m_obj);
          m_objectSize = (uint32_t) (dos.Length - checkpoint);

          output->RewindCursor(m_objectSize + 4);
          output->WriteInt32(m_objectSize);
          output->AdvanceCursor(m_objectSize);
        }
      }

      IGFSerializable^ CacheableObject::FromData(DataInput^ input)
      {
        int maxSize = input->ReadInt32();
        GFDataInputStream dis(input, maxSize);
        uint32_t checkpoint = dis.BytesRead;
        BinaryFormatter bf;
        m_obj = bf.Deserialize(%dis);
        m_objectSize = dis.BytesRead - checkpoint;
        return this;
      }

      uint32_t CacheableObject::ObjectSize::get()
      { 
        if (m_objectSize == 0) {
          GFNullStream ns;
          BinaryFormatter bf;
          bf.Serialize(%ns, m_obj);

          m_objectSize = (uint32_t)sizeof(CacheableObject^) + (uint32_t)ns.Length;
        }
        return m_objectSize;
      }
    }
  }
}
 } //namespace 

