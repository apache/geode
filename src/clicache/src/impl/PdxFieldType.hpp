/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

using namespace System;
#include "../DataOutput.hpp"
#include "../DataInput.hpp"
#include "../GemFireClassIds.hpp"

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
        public ref class PdxFieldType : IGFSerializable
        {
        private:
          String^ m_fieldName;
          String^ m_className;
          Byte    m_typeId;
          Int32   m_sequenceId;
          bool    m_isVariableLengthType;
          bool    m_isIdentityField;
          Int32   m_fixedSize;
          Int32   m_varLenFieldIdx;

          Int32   m_vlOffsetIndex;
          Int32   m_relativeOffset;
          Int32   m_cachedHashcode;
          Int32 getFixedTypeSize();
        public:
          PdxFieldType(String^ fieldName,
                        String^ className,
                        Byte typeId,
                        Int32 sequenceId,
                        bool isVariableLengthType,
                        Int32 fixedSize,
                        Int32 varLenFieldIdx)
          {
            m_cachedHashcode = 0;
            m_fieldName = fieldName;
            m_className = className;
            m_typeId = typeId;
            m_sequenceId = sequenceId;//start with 0
            m_isVariableLengthType = isVariableLengthType;
            m_fixedSize = fixedSize;
            m_varLenFieldIdx = varLenFieldIdx;//start with 0
            m_isIdentityField = false;
          }

          PdxFieldType()
          {
            m_cachedHashcode = 0;
          }

          property Int32 SequenceId
          {
            Int32 get();
          }

          property String^ FieldName
          {
            String^ get();
          }

          property String^ ClassName
          {
            String^ get();
          }

          property Byte TypeId
          {
            Byte get();
          }

          property bool IsVariableLengthType
          {
            bool get();
          }

          property bool IdentityField
          {
            bool get() {return m_isIdentityField;}
            void set(bool value) {m_isIdentityField = value;}
          }

          property Int32 Size
          {
            Int32 get();
          }

          property Int32 VarLenFieldIdx
          {
            Int32 get();
          }

          property Int32 VarLenOffsetIndex
          {
            Int32 get();
            void set(Int32 Value);
          }

          property Int32 RelativeOffset
          {
            Int32 get();
            void set(Int32 Value);
          }

           virtual bool Equals(Object^ otherObj) override;
           virtual Int32 GetHashCode() override;

           virtual void ToData( DataOutput^ output );
           virtual IGFSerializable^ FromData( DataInput^ input );
           virtual property uint32_t ObjectSize
           {
             uint32_t get( ){return 0;}
           }
           virtual property uint32_t ClassId
           {
             uint32_t get( ){return m_typeId;}
           }
           virtual String^ ToString( ) override
           {
             return "PdxFieldName:" + m_fieldName + ", TypeId: " + m_typeId + ", VarLenFieldIdx:" + m_varLenFieldIdx + ", sequenceid:" + m_sequenceId;
           }
        };
      }
			}
    }
  }
}