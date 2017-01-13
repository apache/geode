/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "../IPdxWriter.hpp"
#include "../IPdxReader.hpp"
#include "../IPdxSerializer.hpp"
#include "../Serializable.hpp"
namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
			namespace Generic
			{  
        ref class PdxWrapper : IPdxSerializable
        {
        private:
          Object^ m_object;
        public:
          PdxWrapper(Object^ object)
          {
            m_object = object;
          }
          
          Object^ GetObject()
          {
            return m_object;
          }

          virtual void ToData( IPdxWriter^ writer )
          {
            if(!Serializable::GetPdxSerializer()->ToData(m_object, writer))
              throw gcnew IllegalStateException("PdxSerilizer unable serialize data for type " + m_object->GetType());
          }
          
          virtual void FromData( IPdxReader^ reader )
          {
            String^ className = dynamic_cast<String^>(m_object);

            if(className != nullptr)
              m_object = Serializable::GetPdxSerializer()->FromData((String^)m_object, reader);
            else
              m_object = Serializable::GetPdxSerializer()->FromData(m_object->GetType()->FullName, reader);
            if(m_object == nullptr)
              throw gcnew IllegalStateException("PdxSerilizer unable de-serialize data for type " + m_object->GetType());           
          }

          virtual int GetHashCode()override 
          {
            return m_object->GetHashCode();
          }

          virtual  bool Equals(Object^ obj)override
          {
            if(obj != nullptr)
            {
              PdxWrapper^ pdxWrapper = dynamic_cast<PdxWrapper^>(obj);
              if(pdxWrapper != nullptr)
                return m_object->Equals(pdxWrapper->m_object);
              return m_object->Equals(obj);
            }
            return false;
          }
        };
      }
    }
  }
}