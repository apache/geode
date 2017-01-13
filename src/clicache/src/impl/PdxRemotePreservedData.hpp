/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once
#include "../IPdxUnreadFields.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Generic
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
      }
			}
    }
  }
}