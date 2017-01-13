/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
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

