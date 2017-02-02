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
#include "DataOutput.hpp"
#include <GeodeTypeIdsImpl.hpp>
#include <vcclr.h>

#include "IGFSerializable.hpp"
#include "CacheableObjectArray.hpp"
#include "impl/PdxHelper.hpp"
#include "impl/PdxWrapper.hpp"
using namespace System;
using namespace System::Runtime::InteropServices;
using namespace apache::geode::client;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      void DataOutput::WriteByte(Byte value)
      {
        EnsureCapacity(1);
        m_bytes[m_cursor++] = value;
      }

      void DataOutput::WriteSByte(SByte value)
      {
        EnsureCapacity(1);
        m_bytes[m_cursor++] = value;
      }

      void DataOutput::WriteBoolean(bool value)
      {
        EnsureCapacity(1);
        if (value)
          m_bytes[m_cursor++] = 0x01;
        else
          m_bytes[m_cursor++] = 0x00;
      }

      void DataOutput::WriteChar(Char value)
      {
        EnsureCapacity(2);
        m_bytes[m_cursor++] = (uint8_t)(value >> 8);
        m_bytes[m_cursor++] = (uint8_t)value;
      }

      void DataOutput::WriteBytes(array<Byte>^ bytes, int32_t len)
      {
        if (bytes != nullptr && bytes->Length >= 0)
        {
          if (len >= 0 && len <= bytes->Length)
          {
            WriteArrayLen(len);
            EnsureCapacity(len);
            for (int i = 0; i < len; i++)
              m_bytes[m_cursor++] = bytes[i];
          }
          else
          {
            throw gcnew IllegalArgumentException("DataOutput::WriteBytes argument len is not in byte array range.");
          }
        }
        else
        {
          WriteByte(0xFF);
        }
      }

      void DataOutput::WriteArrayLen(int32_t len)
      {
        if (len == -1) {//0xff
          WriteByte(0xFF);
        }
        else if (len <= 252) { // 252 is java's ((byte)-4 && 0xFF) or 0xfc
          WriteByte((Byte)(len));
        }
        else if (len <= 0xFFFF) {
          WriteByte(0xFE);//0xfe
          WriteUInt16((len));
        }
        else {
          WriteByte((0xFD));//0xfd
          WriteUInt32(len);
        }
      }

      void DataOutput::WriteSBytes(array<SByte>^ bytes, int32_t len)
      {
        if (bytes != nullptr && bytes->Length >= 0)
        {
          if (len >= 0 && len <= bytes->Length)
          {
            WriteArrayLen(len);
            EnsureCapacity(len);
            for (int i = 0; i < len; i++)
              m_bytes[m_cursor++] = bytes[i];
          }
          else
          {
            throw gcnew IllegalArgumentException("DataOutput::WriteSBytes argument len is not in SByte array range.");
          }
        }
        else
        {
          WriteByte(0xFF);
        }
      }

      void DataOutput::WriteBytesOnly(array<Byte>^ bytes, uint32_t len)
      {
        WriteBytesOnly(bytes, len, 0);
      }

      void DataOutput::WriteBytesOnly(array<Byte>^ bytes, uint32_t len, uint32_t offset)
      {
        if (bytes != nullptr)
        {
          if (len >= 0 && len <= ((uint32_t)bytes->Length - offset))
          {
            EnsureCapacity(len);
            for (uint32_t i = 0; i < len; i++)
              m_bytes[m_cursor++] = bytes[offset + i];
          }
          else
          {
            throw gcnew IllegalArgumentException("DataOutput::WriteBytesOnly argument len is not in Byte array range.");
          }
        }
      }

      void DataOutput::WriteSBytesOnly(array<SByte>^ bytes, uint32_t len)
      {
        if (bytes != nullptr)
        {
          if (len >= 0 && len <= (uint32_t)bytes->Length)
          {
            EnsureCapacity(len);
            for (uint32_t i = 0; i < len; i++)
              m_bytes[m_cursor++] = bytes[i];
          }
          else
          {
            throw gcnew IllegalArgumentException("DataOutput::WriteSBytesOnly argument len is not in SByte array range.");
          }
        }
      }

      void DataOutput::WriteUInt16(uint16_t value)
      {
        EnsureCapacity(2);
        m_bytes[m_cursor++] = (uint8_t)(value >> 8);
        m_bytes[m_cursor++] = (uint8_t)value;
      }

      void DataOutput::WriteUInt32(uint32_t value)
      {
        EnsureCapacity(4);
        m_bytes[m_cursor++] = (uint8_t)(value >> 24);
        m_bytes[m_cursor++] = (uint8_t)(value >> 16);
        m_bytes[m_cursor++] = (uint8_t)(value >> 8);
        m_bytes[m_cursor++] = (uint8_t)value;
      }

      void DataOutput::WriteUInt64(uint64_t value)
      {
        EnsureCapacity(8);
        m_bytes[m_cursor++] = (uint8_t)(value >> 56);
        m_bytes[m_cursor++] = (uint8_t)(value >> 48);
        m_bytes[m_cursor++] = (uint8_t)(value >> 40);
        m_bytes[m_cursor++] = (uint8_t)(value >> 32);
        m_bytes[m_cursor++] = (uint8_t)(value >> 24);
        m_bytes[m_cursor++] = (uint8_t)(value >> 16);
        m_bytes[m_cursor++] = (uint8_t)(value >> 8);
        m_bytes[m_cursor++] = (uint8_t)value;
      }

      void DataOutput::WriteInt16(int16_t value)
      {
        WriteUInt16(value);
      }

      void DataOutput::WriteInt32(int32_t value)
      {
        WriteUInt32(value);
      }

      void DataOutput::WriteInt64(int64_t value)
      {
        WriteUInt64(value);
      }

      void DataOutput::WriteFloat(float value)
      {
        array<Byte>^ bytes = BitConverter::GetBytes(value);
        EnsureCapacity(4);
        for (int i = 4 - 1; i >= 0; i--)
          m_bytes[m_cursor++] = bytes[i];
      }

      void DataOutput::WriteDouble(double value)
      {
        array<Byte>^ bytes = BitConverter::GetBytes(value);
        EnsureCapacity(8);
        for (int i = 8 - 1; i >= 0; i--)
          m_bytes[m_cursor++] = bytes[i];
      }

      void DataOutput::WriteDictionary(System::Collections::IDictionary^ dict)
      {
        if (dict != nullptr)
        {
          this->WriteArrayLen(dict->Count);
          for each(System::Collections::DictionaryEntry^ entry in dict)
          {
            this->WriteObject(entry->Key);
            this->WriteObject(entry->Value);
          }
        }
        else
        {
          WriteByte((int8_t)-1);
        }
      }

      void DataOutput::WriteCollection(System::Collections::IList^ collection)
      {
        if (collection != nullptr)
        {
          this->WriteArrayLen(collection->Count);
          for each (Object^ obj in collection) {
            this->WriteObject(obj);
          }
        }
        else
          this->WriteByte((int8_t)-1);
      }

      void DataOutput::WriteDate(System::DateTime date)
      {
        if (date.Ticks != 0L)
        {
          CacheableDate^ cd = gcnew CacheableDate(date);
          cd->ToData(this);
        }
        else
          this->WriteInt64(-1L);
      }

      void DataOutput::WriteCharArray(array<Char>^ charArray)
      {
        if (charArray != nullptr)
        {
          this->WriteArrayLen(charArray->Length);
          for (int i = 0; i < charArray->Length; i++) {
            this->WriteObject(charArray[i]);
          }
        }
        else
          this->WriteByte((int8_t)-1);
      }

      void DataOutput::WriteObjectArray(List<Object^>^ objectArray)
      {
        if (objectArray != nullptr)
        {
          CacheableObjectArray^ coa = CacheableObjectArray::Create(objectArray);
          coa->ToData(this);
        }
        else
          this->WriteByte((int8_t)-1);
      }

      void DataOutput::WriteDotNetObjectArray(Object^ objectArray)
      {
        System::Collections::IList^ list = (System::Collections::IList^)objectArray;
        this->WriteArrayLen(list->Count);
        WriteByte((int8_t)apache::geode::client::GeodeTypeIdsImpl::Class);
        String^ pdxDomainClassname = Serializable::GetPdxTypeName(objectArray->GetType()->GetElementType()->FullName);
        WriteByte((int8_t)apache::geode::client::GeodeTypeIds::CacheableASCIIString);
        WriteUTF(pdxDomainClassname);
        for each(Object^ o in list)
          WriteObject(o);
      }

      void DataOutput::WriteArrayOfByteArrays(array<array<Byte>^>^ byteArrays)
      {
        if (byteArrays != nullptr)
        {
          int fdLen = byteArrays->Length;
          this->WriteArrayLen(byteArrays->Length);
          for (int i = 0; i < fdLen; i++) {
            this->WriteBytes(byteArrays[i]);
          }
        }
        else
          this->WriteByte((int8_t)-1);
      }

      void DataOutput::WriteUTF(String^ value)
      {
        if (value != nullptr) {
          int len = getEncodedLength(value);

          if (len > 0xffff)
            len = 0xffff;

          WriteUInt16(len);
          EnsureCapacity(len);
          EncodeUTF8String(value, len);
        }
        else {
          WriteUInt16(0);
        }
      }

      void DataOutput::WriteStringWithType(String^ value)
      {
        //value will not be null
        int len = getEncodedLength(value);

        if (len > 0xffff)
        {
          if (len == value->Length)//huge ascii
          {
            WriteByte(GeodeTypeIds::CacheableASCIIStringHuge);
            WriteASCIIHuge(value);
          }
          else//huge utf
          {
            WriteByte(GeodeTypeIds::CacheableStringHuge);
            WriteUTFHuge(value);
          }
          return;
        }

        if (len == value->Length)
        {
          WriteByte(GeodeTypeIds::CacheableASCIIString);//ascii string
        }
        else
        {
          WriteByte(GeodeTypeIds::CacheableString);//utf string
        }
        WriteUInt16(len);
        EnsureCapacity(len);
        EncodeUTF8String(value, len);

      }

      void DataOutput::WriteASCIIHuge(String^ value)
      {
        if (value != nullptr) {
          const int strLength = value->Length;
          WriteUInt32(strLength);
          EnsureCapacity(strLength);
          for (int i = 0; i < strLength; i++) {
            m_bytes[m_cursor++] = (uint8_t)value[i];
          }
        }
        else {
          WriteUInt32(0);
        }
      }

      /* Write UTF-16 */
      void DataOutput::WriteUTFHuge(String^ value)
      {
        if (value != nullptr) {
          WriteUInt32(value->Length);
          EnsureCapacity(value->Length * 2);
          for (int i = 0; i < value->Length; i++)
          {
            Char ch = value[i];
            m_bytes[m_cursor++] = (Byte)((ch & 0xff00) >> 8);
            m_bytes[m_cursor++] = (Byte)(ch & 0xff);
          }
        }
        else {
          WriteUInt32(0);
        }
      }

      /*void DataOutput::WriteObject( Object^ obj )
      {
      WriteObjectInternal((IGFSerializable^)obj);
      }*/

      /*void DataOutput::WriteObject( Object^ obj )
      {
      WriteObject( (IGFSerializable^)obj );
      }*/

      int8_t DataOutput::GetTypeId(uint32_t classId)
      {
        if (classId >= 0x80000000) {
          return (int8_t)((classId - 0x80000000) % 0x20000000);
        }
        else if (classId <= 0x7F) {
          return (int8_t)GeodeTypeIdsImpl::CacheableUserData;
        }
        else if (classId <= 0x7FFF) {
          return (int8_t)GeodeTypeIdsImpl::CacheableUserData2;
        }
        else {
          return (int8_t)GeodeTypeIdsImpl::CacheableUserData4;
        }
      }

      int8_t DataOutput::DSFID(uint32_t classId)
      {
        // convention that [0x8000000, 0xa0000000) is for FixedIDDefault,
        // [0xa000000, 0xc0000000) is for FixedIDByte,
        // [0xc0000000, 0xe0000000) is for FixedIDShort
        // and [0xe0000000, 0xffffffff] is for FixedIDInt
        // Note: depends on fact that FixedIDByte is 1, FixedIDShort is 2
        // and FixedIDInt is 3; if this changes then correct this accordingly
        if (classId >= 0x80000000) {
          return (int8_t)((classId - 0x80000000) / 0x20000000);
        }
        return 0;
      }

      void DataOutput::WriteObject(Object^ obj)
      {

        if (obj == nullptr)
        {
          WriteByte((int8_t)GeodeTypeIds::NullObj);
          return;
        }

        if (m_ispdxSerialization && obj->GetType()->IsEnum)
        {
          //need to set             
          int enumVal = Internal::PdxHelper::GetEnumValue(obj->GetType()->FullName, Enum::GetName(obj->GetType(), obj), obj->GetHashCode());
          WriteByte(GeodeClassIds::PDX_ENUM);
          WriteByte(enumVal >> 24);
          WriteArrayLen(enumVal & 0xFFFFFF);
          return;
        }

        //Apache::Geode::Client::Log::Debug("DataOutput::WriteObject " + obj);

        Byte typeId = Apache::Geode::Client::Serializable::GetManagedTypeMappingGeneric(obj->GetType());

        switch (typeId)
        {
        case apache::geode::client::GeodeTypeIds::CacheableByte:
        {
          WriteByte(typeId);
          WriteSByte((SByte)obj);
          return;
        }
        case apache::geode::client::GeodeTypeIds::CacheableBoolean:
        {
          WriteByte(typeId);
          WriteBoolean((bool)obj);
          return;
        }
        case apache::geode::client::GeodeTypeIds::CacheableWideChar:
        {
          WriteByte(typeId);
          WriteObject((Char)obj);
          return;
        }
        case apache::geode::client::GeodeTypeIds::CacheableDouble:
        {
          WriteByte(typeId);
          WriteDouble((Double)obj);
          return;
        }
        case apache::geode::client::GeodeTypeIds::CacheableASCIIString:
        {
          //CacheableString^ cStr = CacheableString::Create((String^)obj);
          ////  TODO: igfser mapping between generic and non generic
          //WriteObjectInternal(cStr);
          WriteStringWithType((String^)obj);
          return;
        }
        case apache::geode::client::GeodeTypeIds::CacheableFloat:
        {
          WriteByte(typeId);
          WriteFloat((float)obj);
          return;
        }
        case apache::geode::client::GeodeTypeIds::CacheableInt16:
        {
          WriteByte(typeId);
          WriteInt16((Int16)obj);
          return;
        }
        case apache::geode::client::GeodeTypeIds::CacheableInt32:
        {
          WriteByte(typeId);
          WriteInt32((Int32)obj);
          return;
        }
        case apache::geode::client::GeodeTypeIds::CacheableInt64:
        {
          WriteByte(typeId);
          WriteInt64((Int64)obj);
          return;
        }
        case apache::geode::client::GeodeTypeIds::CacheableDate:
        {
          //CacheableDate^ cd = gcnew CacheableDate((DateTime)obj);
          //  TODO: igfser mapping between generic and non generic
          //WriteObjectInternal(cd);
          WriteByte(typeId);
          WriteDate((DateTime)obj);
          return;
        }
        case apache::geode::client::GeodeTypeIds::CacheableBytes:
        {
          WriteByte(typeId);
          WriteBytes((array<Byte>^)obj);
          return;
        }
        case apache::geode::client::GeodeTypeIds::CacheableDoubleArray:
        {
          WriteByte(typeId);
          WriteObject((array<Double>^)obj);
          return;
        }
        case apache::geode::client::GeodeTypeIds::CacheableFloatArray:
        {
          WriteByte(typeId);
          WriteObject((array<float>^)obj);
          return;
        }
        case apache::geode::client::GeodeTypeIds::CacheableInt16Array:
        {
          WriteByte(typeId);
          WriteObject((array<Int16>^)obj);
          return;
        }
        case apache::geode::client::GeodeTypeIds::CacheableInt32Array:
        {
          WriteByte(typeId);
          WriteObject((array<Int32>^)obj);
          return;
        }
        case apache::geode::client::GeodeTypeIds::CacheableInt64Array:
        {
          WriteByte(typeId);
          WriteObject((array<Int64>^)obj);
          return;
        }
        case apache::geode::client::GeodeTypeIds::BooleanArray:
        {
          WriteByte(typeId);
          WriteObject((array<bool>^)obj);
          return;
        }
        case apache::geode::client::GeodeTypeIds::CharArray:
        {
          WriteByte(typeId);
          WriteObject((array<char>^)obj);
          return;
        }
        case apache::geode::client::GeodeTypeIds::CacheableStringArray:
        {
          WriteByte(typeId);
          WriteObject((array<String^>^)obj);
          return;
        }
        case apache::geode::client::GeodeTypeIds::CacheableHashTable:
        case apache::geode::client::GeodeTypeIds::CacheableHashMap:
        case apache::geode::client::GeodeTypeIds::CacheableIdentityHashMap:
        {
          WriteByte(typeId);
          WriteDictionary((System::Collections::IDictionary^)obj);
          return;
        }
        case apache::geode::client::GeodeTypeIds::CacheableVector:
        {
          //CacheableVector^ cv = gcnew CacheableVector((System::Collections::IList^)obj);
          ////  TODO: igfser mapping between generic and non generic
          //WriteObjectInternal(cv);
          WriteByte(apache::geode::client::GeodeTypeIds::CacheableVector);
          WriteList((System::Collections::IList^)obj);
          return;
        }
        case apache::geode::client::GeodeTypeIds::CacheableLinkedList:
        {
          //CacheableArrayList^ cal = gcnew CacheableArrayList((System::Collections::IList^)obj);
          ////  TODO: igfser mapping between generic and non generic
          //WriteObjectInternal(cal);
          WriteByte(apache::geode::client::GeodeTypeIds::CacheableLinkedList);
          System::Collections::ICollection^ linkedList = (System::Collections::ICollection^)obj;
          this->WriteArrayLen(linkedList->Count);
          for each (Object^ o in linkedList)
            this->WriteObject(o);
          return;
        }
        case apache::geode::client::GeodeTypeIds::CacheableArrayList:
        {
          //CacheableArrayList^ cal = gcnew CacheableArrayList((System::Collections::IList^)obj);
          ////  TODO: igfser mapping between generic and non generic
          //WriteObjectInternal(cal);
          WriteByte(apache::geode::client::GeodeTypeIds::CacheableArrayList);
          WriteList((System::Collections::IList^)obj);
          return;
        }
        case apache::geode::client::GeodeTypeIds::CacheableStack:
        {
          CacheableStack^ cs = gcnew CacheableStack((System::Collections::ICollection^)obj);
          //  TODO: igfser mapping between generic and non generic
          WriteObjectInternal(cs);
          return;
        }
        default:
        {
          IPdxSerializable^ pdxObj = dynamic_cast<IPdxSerializable^>(obj);
          if (pdxObj != nullptr)
          {
            WriteByte(GeodeClassIds::PDX);
            Internal::PdxHelper::SerializePdx(this, pdxObj);
            return;
          }
          else
          {
            //pdx serialization and is array of object
            if (m_ispdxSerialization && obj->GetType()->IsArray)
            {
              WriteByte(apache::geode::client::GeodeTypeIds::CacheableObjectArray);
              WriteDotNetObjectArray(obj);
              return;
            }

            IGFSerializable^ ct = dynamic_cast<IGFSerializable^>(obj);
            if (ct != nullptr) {
              WriteObjectInternal(ct);
              return;
            }

            if (Serializable::IsObjectAndPdxSerializerRegistered(nullptr))
            {
              pdxObj = gcnew PdxWrapper(obj);
              WriteByte(GeodeClassIds::PDX);
              Internal::PdxHelper::SerializePdx(this, pdxObj);
              return;
            }
          }

          throw gcnew System::Exception("DataOutput not found appropriate type to write it for object: " + obj->GetType());
        }
        }
      }

      void DataOutput::WriteStringArray(array<String^>^ strArray)
      {
        if (strArray != nullptr)
        {
          this->WriteArrayLen(strArray->Length);
          for (int i = 0; i < strArray->Length; i++)
          {
            // this->WriteUTF(strArray[i]);
            WriteObject(strArray[i]);
          }
        }
        else
          WriteByte(-1);
      }

      void DataOutput::WriteObjectInternal(IGFSerializable^ obj)
      {
        //CacheableKey^ key = gcnew CacheableKey();
        if (obj == nullptr) {
          WriteByte((int8_t)GeodeTypeIds::NullObj);
        }
        else {
          int8_t typeId = DataOutput::GetTypeId(obj->ClassId);
          switch (DataOutput::DSFID(obj->ClassId)) {
          case GeodeTypeIdsImpl::FixedIDByte:
            WriteByte((int8_t)GeodeTypeIdsImpl::FixedIDByte);
            WriteByte(typeId); // write the type ID.
            break;
          case GeodeTypeIdsImpl::FixedIDShort:
            WriteByte((int8_t)GeodeTypeIdsImpl::FixedIDShort);
            WriteInt16((int16_t)typeId); // write the type ID.
            break;
          case GeodeTypeIdsImpl::FixedIDInt:
            WriteByte((int8_t)GeodeTypeIdsImpl::FixedIDInt);
            WriteInt32((int32_t)typeId); // write the type ID.
            break;
          default:
            WriteByte(typeId); // write the type ID.
            break;
          }

          if ((int32_t)typeId == GeodeTypeIdsImpl::CacheableUserData) {
            WriteByte((int8_t)obj->ClassId);
          }
          else if ((int32_t)typeId == GeodeTypeIdsImpl::CacheableUserData2) {
            WriteInt16((int16_t)obj->ClassId);
          }
          else if ((int32_t)typeId == GeodeTypeIdsImpl::CacheableUserData4) {
            WriteInt32((int32_t)obj->ClassId);
          }
          obj->ToData(this); // let the obj serialize itself.
        }
      }

      void DataOutput::AdvanceCursor(uint32_t offset)
      {
        EnsureCapacity(offset);
        m_cursor += offset;
      }

      void DataOutput::RewindCursor(uint32_t offset)
      {
        //first set native one
        WriteBytesToUMDataOutput();
        NativePtr->rewindCursor(offset);
        SetBuffer();
      }

      array<Byte>^ DataOutput::GetBuffer()
      {
        WriteBytesToUMDataOutput();
        SetBuffer();

        int buffLen = NativePtr->getBufferLength();
        array<Byte>^ buffer = gcnew array<Byte>(buffLen);

        if (buffLen > 0) {
          pin_ptr<Byte> pin_buffer = &buffer[0];
          memcpy((void*)pin_buffer, NativePtr->getBuffer(), buffLen);
        }
        return buffer;
      }

      uint32_t DataOutput::BufferLength::get()
      {
        //first set native one
        WriteBytesToUMDataOutput();
        SetBuffer();

        return NativePtr->getBufferLength();
      }

      void DataOutput::Reset()
      {
        WriteBytesToUMDataOutput();
        NativePtr->reset();
        SetBuffer();
      }

      void DataOutput::WriteString(String^ value)
      {
        if (value == nullptr)
        {
          this->WriteByte(GeodeTypeIds::CacheableNullString);
        }
        else
        {
          WriteObject(value);
          /*CacheableString^ cs = gcnew CacheableString(value);

          this->WriteByte( (Byte)(cs->ClassId - 0x80000000));
          cs->ToData(this);*/
        }
      }

      void DataOutput::WriteBytesToUMDataOutput()
      {
        NativePtr->advanceCursor(m_cursor);
        m_cursor = 0;
        m_remainingBufferLength = 0;
        m_bytes = nullptr;
      }

      void DataOutput::WriteObject(bool% obj)
      {
        WriteBoolean(obj);
      }

      void DataOutput::WriteObject(Byte% obj)
      {
        WriteByte(obj);
      }

      void DataOutput::WriteObject(Char% obj)
      {
        unsigned short us = (unsigned short)obj;
        EnsureCapacity(2);
        m_bytes[m_cursor++] = us >> 8;
        m_bytes[m_cursor++] = (Byte)us;
      }

      void DataOutput::WriteObject(Double% obj)
      {
        WriteDouble(obj);
      }

      void DataOutput::WriteObject(Single% obj)
      {
        WriteFloat(obj);
      }

      void DataOutput::WriteObject(int16_t% obj)
      {
        WriteInt16(obj);
      }

      void DataOutput::WriteObject(int32_t% obj)
      {
        WriteInt32(obj);
      }

      void DataOutput::WriteObject(int64_t% obj)
      {
        WriteInt64(obj);
      }

      void DataOutput::WriteObject(UInt16% obj)
      {
        WriteUInt16(obj);
      }

      void DataOutput::WriteObject(UInt32% obj)
      {
        WriteUInt32(obj);
      }

      void DataOutput::WriteObject(UInt64% obj)
      {
        WriteUInt64(obj);
      }

      void DataOutput::WriteBooleanArray(array<bool>^ boolArray)
      {
        WriteObject<bool>(boolArray);
      }

      void DataOutput::WriteShortArray(array<Int16>^ shortArray)
      {
        WriteObject<Int16>(shortArray);
      }

      void DataOutput::WriteIntArray(array<Int32>^ intArray)
      {
        WriteObject<Int32>(intArray);
      }

      void DataOutput::WriteLongArray(array<Int64>^ longArray)
      {
        WriteObject<Int64>(longArray);
      }

      void DataOutput::WriteFloatArray(array<float>^ floatArray)
      {
        WriteObject<float>(floatArray);
      }

      void DataOutput::WriteDoubleArray(array<double>^ doubleArray)
      {
        WriteObject<double>(doubleArray);
      }  // namespace Client
    }  // namespace Geode
  }  // namespace Apache

}
