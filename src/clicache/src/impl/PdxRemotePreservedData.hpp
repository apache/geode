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
#include "../IPdxUnreadFields.hpp"

using namespace System;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      namespace Internal
      {
        public ref class PdxRemotePreservedData : public IPdxUnreadFields
        {
        private:
          array<array<Byte>^>^ m_preservedData;
          Int32               m_typeId;
          Int32               m_mergedTypeId;
          Int32               m_currentIndex;
          Object^             m_owner;
        public:

					PdxRemotePreservedData()
					{
					
					}

          //PdxRemotePreservedData(Int32 typeId, Int32 mergedTypeId, Int32 numberOfFields)
          //{
          //  m_typeId = typeId;
          //  m_mergedTypeId = mergedTypeId;
          //  m_preservedData = gcnew array<array<Byte>^>(numberOfFields);
          //  m_currentIndex = 0;
          //  //TODO:how to get merge typeid
          //}

          PdxRemotePreservedData(Int32 typeId, Int32 mergedTypeId, Int32 numberOfFields, Object^ owner)
          {
            m_typeId = typeId;
            m_mergedTypeId = mergedTypeId;
            m_preservedData = gcnew array<array<Byte>^>(numberOfFields);
            m_currentIndex = 0;
            m_owner = owner;
            //TODO:how to get merge typeid
          }

					void Initialize(Int32 typeId, Int32 mergedTypeId, Int32 numberOfFields, Object^ owner)
					{
						m_typeId = typeId;
            m_mergedTypeId = mergedTypeId;
            m_preservedData = gcnew array<array<Byte>^>(numberOfFields);
            m_currentIndex = 0;
            m_owner = owner;
					}

          property Int32 MergedTypeId
          {
            Int32 get() {return m_mergedTypeId;}
          }

          property Object^ Owner
          {
            Object^ get() {return m_owner;}
            void set(Object^ val) {m_owner = val;}
          }
          property array<Byte>^ default[Int32]
          {
            array<Byte>^ get(Int32 idx)
            {
              return m_preservedData[idx];
            }
            void set(Int32 idx, array<Byte>^ val)
            {
              m_preservedData[m_currentIndex++] = val;
            }
          }

          virtual bool Equals(Object^ otherObject) override
          {
            if(otherObject == nullptr)
              return false;

            if(m_owner == nullptr)
              return false;

            return m_owner->Equals(otherObject);
          }

          virtual int GetHashCode() override
          {
            if(m_owner != nullptr)
              return m_owner->GetHashCode();
            return 0;
          }
        };
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache

}