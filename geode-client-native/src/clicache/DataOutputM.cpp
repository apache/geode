/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "DataOutputM.hpp"
#include <cppcache/impl/GemfireTypeIdsImpl.hpp>
#include <vcclr.h>

#include "IGFSerializable.hpp"

using namespace System;
using namespace System::Runtime::InteropServices;
using namespace gemfire;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      void DataOutput::WriteByte( Byte value )
      {
        EnsureCapacity(1);
        m_bytes[m_cursor++] = value;
      }

      void DataOutput::WriteSByte( SByte value )
      {
        EnsureCapacity(1);
        m_bytes[m_cursor++] = value;
      }

      void DataOutput::WriteBoolean( bool value )
      {
        EnsureCapacity(1);
        if (value)
          m_bytes[m_cursor++] = 0x01;
        else
          m_bytes[m_cursor++] = 0x00;
      }

      void DataOutput::WriteBytes( array<Byte>^ bytes, int32_t len )
      {
        //TODO::hitesh
        if (bytes != nullptr && bytes->Length >= 0)
        {
          if ( len >= 0 && len <= bytes->Length )
          {
            WriteArrayLen(len);
            EnsureCapacity(len);
            for( int i = 0; i < len; i++ )
              m_bytes[m_cursor++] = bytes[i];
          }
          else
          {
            throw gcnew GemFire::Cache::IllegalArgumentException("DataOutput::WriteBytes argument len is not in byte array range." );
          }
        }
        else
        {
          WriteByte(0xFF);
        }
      }

     /* void DataOutput::WriteArrayLen( int32_t len )
      {
        if (len == -1) {
          WriteByte(0xFF);
        } else if (len <= 252) { // 252 is java's ((byte)-4 && 0xFF)
          WriteByte(Convert::ToByte(len));
        } else if (len <= 0xFFFF) {
          WriteByte(0xFE);
          WriteInt16(Convert::ToInt16(len));
        } else {
          WriteByte(Convert::ToByte(-0xFD));
          WriteInt32(len);
        }
      }*/

			void DataOutput::WriteArrayLen( int32_t len )
			{
				if (len == -1) {//0xff
					WriteByte(0xFF);
				} else if (len <= 252) { // 252 is java's ((byte)-4 && 0xFF) or 0xfc
					WriteByte(Convert::ToByte(len));
				} else if (len <= 0xFFFF) {
					WriteByte(0xFE);//0xfe
					WriteUInt16((len));
				} else {
					WriteByte((0xFD));//0xfd
					WriteUInt32(len);
				}
			}

      void DataOutput::WriteSBytes( array<SByte>^ bytes, int32_t len )
      {
        //TODO::hitesh
        if (bytes != nullptr && bytes->Length >= 0)
        {
          if ( len >= 0 && len <= bytes->Length )
          {
            WriteArrayLen(len);
            EnsureCapacity(len);
            for( int i = 0; i < len; i++ )
              m_bytes[m_cursor++] = bytes[i];
          }
          else
          {
            throw gcnew GemFire::Cache::IllegalArgumentException("DataOutput::WriteSBytes argument len is not in SByte array range." );
          }
        }
        else
        {
          WriteByte(0xFF);
        }
      }

      void DataOutput::WriteBytesOnly( array<Byte>^ bytes, uint32_t len )
      {
        WriteBytesOnly(bytes, len, 0);
      }

      void DataOutput::WriteBytesOnly( array<Byte>^ bytes, uint32_t len, uint32_t offset )
      {
        //TODO::hitesh
        if (bytes != nullptr)
        {
          if ( len >= 0 && len <= ((uint32_t)bytes->Length - offset) )
          {
            EnsureCapacity(len);            
            for( uint32_t i = 0; i < len; i++ )
              m_bytes[m_cursor++] = bytes[offset + i];
          }
          else
          {
            throw gcnew GemFire::Cache::IllegalArgumentException("DataOutput::WriteBytesOnly argument len is not in Byte array range." );
          }
        }
      }

      void DataOutput::WriteSBytesOnly( array<SByte>^ bytes, uint32_t len )
      {
        //TODO::hitesh
        if (bytes != nullptr)
        {
          if ( len >= 0 && len <= (uint32_t)bytes->Length )
          {
            EnsureCapacity(len);
            for( uint32_t i = 0; i < len; i++ )
              m_bytes[m_cursor++] = bytes[i];
          }
          else
          {
            throw gcnew GemFire::Cache::IllegalArgumentException("DataOutput::WriteSBytesOnly argument len is not in SByte array range." );
          }
        }
      }

      void DataOutput::WriteUInt16( uint16_t value )
      {
        EnsureCapacity(2);
        m_bytes[m_cursor++] = (uint8_t)(value >> 8);
        m_bytes[m_cursor++] = (uint8_t)value;
      }

      void DataOutput::WriteUInt32( uint32_t value )
      {
        EnsureCapacity(4);
        m_bytes[m_cursor++] = (uint8_t)(value >> 24);
        m_bytes[m_cursor++] = (uint8_t)(value >> 16);
        m_bytes[m_cursor++] = (uint8_t)(value >> 8);
        m_bytes[m_cursor++] = (uint8_t)value;
      }

      void DataOutput::WriteUInt64( uint64_t value )
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

      void DataOutput::WriteInt16( int16_t value )
      {
        WriteUInt16(value);
      }

      void DataOutput::WriteInt32( int32_t value )
      {
        WriteUInt32(value);
      }

      void DataOutput::WriteInt64( int64_t value )
      {
        WriteUInt64(value);
      }

      void DataOutput::WriteFloat( float value )
      {
        array<Byte>^ bytes = BitConverter::GetBytes(value);
        EnsureCapacity(4);
        for(int i = bytes->Length - 1; i >=0 ; i--)
          m_bytes[m_cursor++] = bytes[i];
      }

      void DataOutput::WriteDouble( double value )
      {
        array<Byte>^ bytes = BitConverter::GetBytes(value);
        EnsureCapacity(8);
        for(int i = bytes->Length - 1; i >=0 ; i--)
          m_bytes[m_cursor++] = bytes[i];
      }

       void DataOutput::WriteUTF( String^ value )
      {
        if (value != nullptr) {
          int len = value->Length*3;
          
          if (len > 8192) //approx check
            len = getEncodedLength(value);
          
          if (len > 0xffff)
            len = 0xffff;

          EnsureCapacity(len);

          m_cursor += 2;          
          int strLen = EncodeString(value, len);
          m_cursor -= (strLen + 2);
          WriteUInt16(strLen);
          m_cursor += strLen;
        }
        else {
          WriteUInt16(0);
        }
      }

      void DataOutput::WriteASCIIHuge( String^ value )
      {
        //TODO::hitesh
        if (value != nullptr) {
          EnsureCapacity(value->Length);
          m_cursor += 4;          
          int strLen = EncodeString(value, value->Length);
          m_cursor -= (strLen + 4);
          WriteUInt32(strLen);
          m_cursor += strLen;
        }
        else {
          WriteUInt32(0);
        }
      }

      void DataOutput::WriteUTFHuge( String^ value )
      {
        if (value != nullptr) {
          WriteUInt32(value->Length);
          EnsureCapacity(value->Length * 2);
          for( int i = 0; i < value->Length; i++)
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

      void DataOutput::WriteObject( IGFSerializable^ obj )
      {
         WriteObjectInternal(obj);
      }

      void DataOutput::WriteObject( Serializable^ obj )
      {
        WriteObject( (IGFSerializable^)obj );
      }

      int8_t DataOutput::GetTypeId(uint32_t classId )
      {
          if (classId >= 0x80000000) {
            return (int8_t)((classId - 0x80000000) % 0x20000000);
          }
          else if (classId <= 0x7F) {
            return (int8_t)GemfireTypeIdsImpl::CacheableUserData;
          }
          else if (classId <= 0x7FFF) {
            return (int8_t)GemfireTypeIdsImpl::CacheableUserData2;
          }
          else {
            return (int8_t)GemfireTypeIdsImpl::CacheableUserData4;
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

      /*
      void DataOutput::WriteGenericObject(Object^ obj)
      {
        if ( obj == nullptr ) 
        {
          WriteByte( (int8_t) GemfireTypeIds::NullObj );
          return;
        } 

        Byte typeId = GemStone::GemFire::Cache::Generic::Serializable::GetManagedTypeMappingGeneric(obj->GetType());

        switch(typeId)
        {
        case gemfire::GemfireTypeIds::CacheableByte:
          {
            WriteByte(typeId);
            WriteByte((Byte)obj);
            return;
          }
        case gemfire::GemfireTypeIds::CacheableBoolean:
          {
            WriteByte(typeId);
            WriteBoolean((bool)obj);
            return;
          }
        case gemfire::GemfireTypeIds::CacheableWideChar:
          {
            WriteByte(typeId);
            WriteObject((Char)obj);
            return;
          }
        case gemfire::GemfireTypeIds::CacheableDouble:
          {
            WriteByte(typeId);
            WriteDouble((Double)obj);
            return;
          }
        case gemfire::GemfireTypeIds::CacheableASCIIString:
          {
            GemStone::GemFire::Cache::CacheableString^ cStr = GemStone::GemFire::Cache::CacheableString::Create((String^)obj);
            WriteObjectInternal(cStr);
            return;
          }
        case gemfire::GemfireTypeIds::CacheableFloat:
          {
            WriteByte(typeId);
            WriteFloat((float)obj);
            return;
          }
        case gemfire::GemfireTypeIds::CacheableInt16:
          {
            WriteByte(typeId);
            WriteInt16((Int16)obj);
            return;
          }
        case gemfire::GemfireTypeIds::CacheableInt32:
          {
            WriteByte(typeId);
            WriteInt32((Int32)obj);
            return;
          }
        case gemfire::GemfireTypeIds::CacheableInt64:
          {
            WriteByte(typeId);
            WriteInt64((Int64)obj);
            return;
          }
        case gemfire::GemfireTypeIds::CacheableDate:
          {
            GemStone::GemFire::Cache::CacheableDate^ cd = gcnew GemStone::GemFire::Cache::CacheableDate((DateTime)obj);
            WriteObjectInternal(cd);
            return;
          }
        case gemfire::GemfireTypeIds::CacheableBytes:
          {
            WriteByte(typeId);
            WriteBytes((array<Byte>^)obj);
            return;
          }
        case gemfire::GemfireTypeIds::CacheableDoubleArray:
          {
            WriteByte(typeId);
            WriteObject((array<Double>^)obj);
            return;
          }
        case gemfire::GemfireTypeIds::CacheableFloatArray:
        {
            WriteByte(typeId);
            WriteObject((array<float>^)obj);
            return;
          }
        case gemfire::GemfireTypeIds::CacheableInt16Array:
        {
            WriteByte(typeId);
            WriteObject((array<Int16>^)obj);
            return;
        }
        case gemfire::GemfireTypeIds::CacheableInt32Array:
        {
            WriteByte(typeId);
            WriteObject((array<Int32>^)obj);
            return;
        }
        case gemfire::GemfireTypeIds::CacheableInt64Array:
        {
            WriteByte(typeId);
            WriteObject((array<Int64>^)obj);
            return;
        }
        case gemfire::GemfireTypeIds::BooleanArray:
        {
            WriteByte(typeId);
            WriteObject((array<bool>^)obj);
            return;
        }
        case gemfire::GemfireTypeIds::CharArray:
        {
            WriteByte(typeId);
            WriteObject((array<char>^)obj);
            return;
        }
        case gemfire::GemfireTypeIds::CacheableStringArray:
        {
            WriteByte(typeId);
            WriteObject((array<String^>^)obj);
            return;
        }
        case gemfire::GemfireTypeIds::CacheableHashTable:
        case gemfire::GemfireTypeIds::CacheableHashMap:
        case gemfire::GemfireTypeIds::CacheableIdentityHashMap:
        {
            WriteByte(typeId);
            WriteDictionary((System::Collections::IDictionary^)obj);
            return;
        }
        case gemfire::GemfireTypeIds::CacheableVector:
        {
          GemStone::GemFire::Cache::CacheableVector^ cv = gcnew GemStone::GemFire::Cache::CacheableVector((System::Collections::IList^)obj);
          WriteObjectInternal(cv);
          return;
        }
        case gemfire::GemfireTypeIds::CacheableArrayList:
        {
          GemStone::GemFire::Cache::CacheableArrayList^ cal = gcnew GemStone::GemFire::Cache::CacheableArrayList((System::Collections::IList^)obj);
          WriteObjectInternal(cal);
          return;
        }
        case gemfire::GemfireTypeIds::CacheableStack:
        {
          GemStone::GemFire::Cache::CacheableStack^ cs = gcnew GemStone::GemFire::Cache::CacheableStack((System::Collections::ICollection^)obj);
          WriteObjectInternal(cs);
          return;
        }
        default:
          {
            IGFSerializable^ ct = safe_cast<IGFSerializable^>(obj);
            if(ct != nullptr) {
              WriteObjectInternal(ct);
              return ;
            }
            throw gcnew System::Exception("DataOutput not found appropriate type to write it.");
          }
        }
      }
      */

      /*
      void DataOutput::WriteStringArray(array<String^>^ strArray)
      {
        this->WriteArrayLen(strArray->Length);
        for(int i = 0; i < strArray->Length; i++)
        {
          this->WriteUTF(strArray[i]);
        }
      }
      */

      void DataOutput::WriteObjectInternal( IGFSerializable^ obj )
      {
        //CacheableKey^ key = gcnew CacheableKey();
        if ( obj == nullptr ) {
          WriteByte( (int8_t) GemfireTypeIds::NullObj );
        } else {
          int8_t typeId = DataOutput::GetTypeId( obj->ClassId);
          switch (DataOutput::DSFID(obj->ClassId)) {
            case GemfireTypeIdsImpl::FixedIDByte:
              WriteByte((int8_t)GemfireTypeIdsImpl::FixedIDByte);
              WriteByte( typeId ); // write the type ID.
              break;
            case GemfireTypeIdsImpl::FixedIDShort:
              WriteByte((int8_t)GemfireTypeIdsImpl::FixedIDShort);
              WriteInt16( (int16_t)typeId ); // write the type ID.
              break;
            case GemfireTypeIdsImpl::FixedIDInt:
              WriteByte((int8_t)GemfireTypeIdsImpl::FixedIDInt);
              WriteInt32( (int32_t)typeId ); // write the type ID.
              break;
            default:
              WriteByte( typeId ); // write the type ID.
              break;
          }
          
          if ( (int32_t)typeId == GemfireTypeIdsImpl::CacheableUserData ) {
            WriteByte( (int8_t) obj->ClassId );
          } else if ( (int32_t)typeId == GemfireTypeIdsImpl::CacheableUserData2 ) {
            WriteInt16( (int16_t) obj->ClassId );
          } else if ( (int32_t)typeId == GemfireTypeIdsImpl::CacheableUserData4 ) {
            WriteInt32( (int32_t) obj->ClassId );
          }
          obj->ToData( this ); // let the obj serialize itself.
        }
      }

      void DataOutput::AdvanceCursor( uint32_t offset )
      {
        m_cursor += offset;
      }

      void DataOutput::RewindCursor( uint32_t offset )
      {
        //first set native one
        WriteBytesToUMDataOutput();
        NativePtr->rewindCursor(offset);
        SetBuffer();
        //m_cursor -= offset;
      }

      array<Byte>^ DataOutput::GetBuffer( )
      {
        //TODO::hitesh
       // return m_bytes;
        //first set native one
        WriteBytesToUMDataOutput();
        SetBuffer();
        
        int buffLen = NativePtr->getBufferLength();
        array<Byte>^ buffer = gcnew array<Byte>( buffLen );

        if ( buffLen > 0 ) {
          pin_ptr<Byte> pin_buffer = &buffer[ 0 ];
          memcpy( (void*)pin_buffer, NativePtr->getBuffer( ), buffLen );
        }
        return buffer;
      }

      uint32_t DataOutput::BufferLength::get( )
      {
        //first set native one
        WriteBytesToUMDataOutput();
        SetBuffer();

        return NativePtr->getBufferLength();
      }

      void DataOutput::Reset( )
      {
        //first set native one
        WriteBytesToUMDataOutput();
        NativePtr->reset();
        SetBuffer();
      }

    }
  }
}
