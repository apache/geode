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