/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */



//#include "gf_includes.hpp"
#include "CacheableObjectXml.hpp"
#include "DataInput.hpp"
#include "DataOutput.hpp"
#include "Log.hpp"
#include "impl/GFNullStream.hpp"
#include "impl/GFDataInputStream.hpp"
#include "impl/GFDataOutputStream.hpp"

using namespace System;
using namespace System::IO;
using namespace System::Xml::Serialization;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      void CacheableObjectXml::ToData(DataOutput^ output)
      {
        if (m_obj == nullptr) {
          output->WriteByte((Byte)1);
        }
        else
        {
          output->WriteByte((Byte)0);
          Type^ objType = m_obj->GetType();

          output->WriteUTF(objType->AssemblyQualifiedName);

          output->AdvanceCursor(4); // placeholder for object size bytes needed while reading back.

          XmlSerializer xs(objType);
          GFDataOutputStream dos(output);
          int64_t checkpoint = dos.Length;
          xs.Serialize(%dos, m_obj);
          m_objectSize = (uint32_t) (dos.Length - checkpoint);

          output->RewindCursor(m_objectSize + 4);
          output->WriteInt32(m_objectSize);
          output->AdvanceCursor(m_objectSize);
        }
      }

      IGFSerializable^ CacheableObjectXml::FromData(DataInput^ input)
      {
        Byte isNull = input->ReadByte();
        if (isNull) {
          m_obj = nullptr;
        }
        else {
          String^ typeName = input->ReadUTF();
          Type^ objType = Type::GetType(typeName);
          if (objType == nullptr)
          {
            Log::Error("CacheableObjectXml.FromData(): Cannot find type '" +
              typeName + "'.");
            m_obj = nullptr;
          }
          else {
            int maxSize = input->ReadInt32();
            GFDataInputStream dis(input, maxSize);
            XmlSerializer xs(objType);
            uint32_t checkpoint = dis.BytesRead;
            m_obj = xs.Deserialize(%dis);
            m_objectSize = dis.BytesRead - checkpoint;
          }
        }
        return this;
      }

      uint32_t CacheableObjectXml::ObjectSize::get()
      { 
        if (m_objectSize == 0) {
          if (m_obj != nullptr) {
            Type^ objType = m_obj->GetType();
            XmlSerializer xs(objType);
            GFNullStream ns;
            xs.Serialize(%ns, m_obj);

            m_objectSize = (uint32_t)sizeof(CacheableObjectXml^) + (uint32_t)ns.Length;
          }
        }
        return m_objectSize;
      }
    }
  }
}
 } //namespace 

