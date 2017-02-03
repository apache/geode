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
#include "PdxFieldType.hpp"
#include <gfcpp/GeodeTypeIds.hpp>

using namespace System;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      namespace Internal
      {
        Int32 PdxFieldType::SequenceId::get()
        {
          return m_sequenceId;
        }

        String^ PdxFieldType::FieldName::get()
        {
          return m_fieldName;
        }

        String^ PdxFieldType::ClassName::get()
        {
          return m_className;
        }

        Byte PdxFieldType::TypeId::get()
        {
          return m_typeId;
        }

        bool PdxFieldType::IsVariableLengthType::get()
        {
          return m_isVariableLengthType;
        }

        Int32 PdxFieldType::Size::get()
        {
          return m_fixedSize;
        }

        Int32 PdxFieldType::VarLenFieldIdx::get()
        {
          return m_varLenFieldIdx;
        }

        Int32 PdxFieldType::VarLenOffsetIndex::get()
        {
          return m_vlOffsetIndex;
        }

        void PdxFieldType::VarLenOffsetIndex::set(Int32 val)
        {
          m_vlOffsetIndex = val;
        }

        Int32 PdxFieldType::RelativeOffset::get()
        {
          return m_relativeOffset;
        }

        void PdxFieldType::RelativeOffset::set(Int32 val)
        {
          m_relativeOffset = val;
        }

        //it compares fieldname and type-id
        bool PdxFieldType::Equals(Object^ otherObj)
        {
          if (otherObj == nullptr)
            return false;

          PdxFieldType^ otherFieldType = dynamic_cast<PdxFieldType^>(otherObj);

          if (otherFieldType == nullptr)
            return false;

          if (otherFieldType == this)
            return true;

          if (otherFieldType->m_fieldName == m_fieldName && otherFieldType->m_typeId == m_typeId)
            return true;

          return false;
        }

        Int32 PdxFieldType::GetHashCode()
        {
          int hash = m_cachedHashcode;
          if (hash == 0)
          {
            if (m_fieldName != nullptr)
            {
              hash = hash * 31 + m_fieldName->GetHashCode();
            }

            hash = hash * 31 + m_typeId;
            if (hash == 0)
              hash = 1;
            m_cachedHashcode = hash;
          }

          return m_cachedHashcode;
        }

        void PdxFieldType::ToData(DataOutput^ output)
        {
          output->WriteString(m_fieldName);
          output->WriteInt32(m_sequenceId);
          output->WriteInt32(m_varLenFieldIdx);
          output->WriteByte(m_typeId);

          output->WriteInt32(m_relativeOffset);
          output->WriteInt32(m_vlOffsetIndex);
          output->WriteBoolean(m_isIdentityField);
        }

        IGFSerializable^ PdxFieldType::FromData(DataInput^ input)
        {
          m_fieldName = input->ReadString();
          m_sequenceId = input->ReadInt32();
          m_varLenFieldIdx = input->ReadInt32();
          m_typeId = input->ReadByte();

          m_relativeOffset = input->ReadInt32();
          m_vlOffsetIndex = input->ReadInt32();
          m_isIdentityField = input->ReadBoolean();

          m_fixedSize = getFixedTypeSize();

          if (m_fixedSize != -1)
            m_isVariableLengthType = false;
          else
            m_isVariableLengthType = true;

          return this;
        }

        Int32 PdxFieldType::getFixedTypeSize()
        {
          switch (m_typeId)
          {
          case PdxTypes::BYTE:
          case PdxTypes::BOOLEAN:
            return GeodeClassIds::BOOLEAN_SIZE;

          case PdxTypes::SHORT:
          case PdxTypes::CHAR:
            //case apache::geode::client::GeodeTypeIds::CacheableChar: //TODO
            return GeodeClassIds::CHAR_SIZE;

          case PdxTypes::INT:
          case PdxTypes::FLOAT:
            //case DSCODE.ENUM:
            return GeodeClassIds::INTEGER_SIZE;

          case PdxTypes::LONG:
          case PdxTypes::DOUBLE:
          case PdxTypes::DATE:
            return GeodeClassIds::LONG_SIZE;

          default:
            return -1;
          }  // namespace Client
        }  // namespace Geode
      }  // namespace Apache

    }
  }
}