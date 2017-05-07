/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "CacheableObject.hpp"
#include "DataInputM.hpp"
#include "DataOutputM.hpp"
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
    namespace Cache
    {
      void CacheableObject::ToData(DataOutput^ output)
      {
        if(m_obj != nullptr)
        {
          GFDataOutputStream dos(output);
          BinaryFormatter bf;
          int64_t checkpoint = dos.Length;
          bf.Serialize(%dos, m_obj);
          m_objectSize = (uint32_t) (dos.Length - checkpoint);
        }
      }

      IGFSerializable^ CacheableObject::FromData(DataInput^ input)
      {
        GFDataInputStream dis(input);
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
