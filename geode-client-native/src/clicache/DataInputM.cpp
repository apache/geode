/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "DataInputM.hpp"
#include <cppcache/Cache.hpp>
#include "CacheFactoryM.hpp"
#include "CacheM.hpp"
#include <vcclr.h>
//#include <cppcache/GemfireTypeIds.hpp>
#include <cppcache/impl/GemfireTypeIdsImpl.hpp>
#include "CacheableStringM.hpp"
#include "CacheableHashMapM.hpp"
#include "CacheableStackM.hpp"
#include "CacheableVectorM.hpp"
#include "CacheableArrayListM.hpp"
#include "CacheableIDentityHashMapM.hpp"
#include "CacheableDateM.hpp"

#include "SerializableM.hpp"


using namespace System;
using namespace System::IO;
using namespace gemfire;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      DataInput::DataInput(array<Byte>^ buffer)
      {
        if (buffer != nullptr && buffer->Length > 0) {
          _GF_MG_EXCEPTION_TRY

            int32_t len = buffer->Length;
            GF_NEW(m_buffer, uint8_t[len]);
            pin_ptr<const Byte> pin_buffer = &buffer[0];
            memcpy(m_buffer, (void*)pin_buffer, len);
            SetPtr(new gemfire::DataInput(m_buffer, len), true);

            m_buffer = const_cast<uint8_t*>(NativePtr->currentBufferPosition());
            m_bufferLength = NativePtr->getBytesRemaining();    

          _GF_MG_EXCEPTION_CATCH_ALL          
        }
        else {
          throw gcnew IllegalArgumentException("DataInput.ctor(): "
            "provided buffer is null or empty");
        }
      }

      DataInput::DataInput(array<Byte>^ buffer, int32_t len )
      {
        if (buffer != nullptr) {
          if (len == 0 || (int32_t)len > buffer->Length) {
            throw gcnew IllegalArgumentException(String::Format(
              "DataInput.ctor(): given length {0} is zero or greater than "
              "size of buffer {1}", len, buffer->Length));
          }
          //m_bytes = gcnew array<Byte>(len);
          //System::Array::Copy(buffer, 0, m_bytes, 0, len);
         _GF_MG_EXCEPTION_TRY

            GF_NEW(m_buffer, uint8_t[len]);
            pin_ptr<const Byte> pin_buffer = &buffer[0];
            memcpy(m_buffer, (void*)pin_buffer, len);
            SetPtr(new gemfire::DataInput(m_buffer, len), true);

            m_buffer = const_cast<uint8_t*>(NativePtr->currentBufferPosition());
            m_bufferLength = NativePtr->getBytesRemaining();    

          _GF_MG_EXCEPTION_CATCH_ALL
        }
        else {
          throw gcnew IllegalArgumentException("DataInput.ctor(): "
            "provided buffer is null");
        }
      }

      Byte DataInput::ReadByte( )
      {
        CheckBufferSize(1);
        return m_buffer[m_cursor++];
      }

      SByte DataInput::ReadSByte( )
      {
        CheckBufferSize(1);
        return m_buffer[m_cursor++];
      }

      bool DataInput::ReadBoolean( )
      {
        CheckBufferSize(1);
        Byte val = m_buffer[m_cursor++];
        if ( val == 1)
          return true;
        else
          return false;
      }

      array<Byte>^ DataInput::ReadBytes( )
      {
        int32_t length;
        length = ReadArrayLen();

        if (length >= 0) {
          if (length == 0)
            return gcnew array<Byte>(0);
          else {
            array<Byte>^ bytes = ReadBytesOnly(length);
            return bytes;
          }
        }
        return nullptr;
      }

      int DataInput::ReadArrayLen( )
      {
        int code;
        int len;
        
        code = Convert::ToInt32(ReadByte());

        if (code == 0xFF) {
          len = -1;
        } else {
          unsigned int result = code;
          if (result > 252) {  // 252 is java's ((byte)-4 && 0xFF)
            if (code == 0xFE) {
              result = ReadUInt16();
            } else if (code == 0xFD) {
              result = ReadUInt32();
            }
            else {
              throw gcnew IllegalStateException("unexpected array length code");
            }
            //TODO:: illegal length
          }
          len = (int)result;
        }
        return len;
      }

      array<SByte>^ DataInput::ReadSBytes( )
      {
        int32_t length;
        length = ReadArrayLen();

        if (length >= -1) {
          if (length == 0)
            return gcnew array<SByte>(0);
          else {
            array<SByte>^ bytes = ReadSBytesOnly(length);
            return bytes;
          }
        }
        return nullptr;
      }

      array<Byte>^ DataInput::ReadBytesOnly( uint32_t len )
      {
        if (len > 0) {
          CheckBufferSize(len);
          array<Byte>^ bytes = gcnew array<Byte>(len);
          
          for ( unsigned int i = 0; i < len; i++)
            bytes[i] = m_buffer[m_cursor++];

          return bytes;
        }
        return nullptr;
      }

      void DataInput::ReadBytesOnly( array<Byte> ^ buffer, int offset, int count )
      {
        if (count > 0) {
          CheckBufferSize((uint32_t)count);
          
          for ( int i = 0; i < count; i++)
            buffer[offset + i] = m_buffer[m_cursor++];
        }
      }

      array<SByte>^ DataInput::ReadSBytesOnly( uint32_t len )
      {
        if (len > 0) {
          CheckBufferSize(len);
          array<SByte>^ bytes = gcnew array<SByte>(len);

          for ( unsigned int i = 0; i < len; i++)
            bytes[i] = m_buffer[m_cursor++];

          return bytes;
        }
        return nullptr;
      }

      uint16_t DataInput::ReadUInt16( )
      {
        CheckBufferSize(2);
        uint16_t data = m_buffer[m_cursor++];
        data = (data << 8) | m_buffer[m_cursor++];
        return data;
      }

      uint32_t DataInput::ReadUInt32( )
      {
        CheckBufferSize(4);
        uint32_t data = m_buffer[m_cursor++];
        data = (data << 8) | m_buffer[m_cursor++];
        data = (data << 8) | m_buffer[m_cursor++];
        data = (data << 8) | m_buffer[m_cursor++];
        
        return data;
      }

      uint64_t DataInput::ReadUInt64( )
      {
        uint64_t data;
        
        CheckBufferSize(8);
        
        data = m_buffer[m_cursor++];
        data = (data << 8) | m_buffer[m_cursor++];
        data = (data << 8) | m_buffer[m_cursor++];
        data = (data << 8) | m_buffer[m_cursor++];
        data = (data << 8) | m_buffer[m_cursor++];
        data = (data << 8) | m_buffer[m_cursor++];
        data = (data << 8) | m_buffer[m_cursor++];
        data = (data << 8) | m_buffer[m_cursor++];
        
        return data;
      }

      int16_t DataInput::ReadInt16( )
      {
        return ReadUInt16();
      }

      int32_t DataInput::ReadInt32( )
      {
        return ReadUInt32();
      }

      int64_t DataInput::ReadInt64( )
      {
        return ReadUInt64();
      }

      array<Byte>^ DataInput::ReadReverseBytesOnly(int len)
      {
        CheckBufferSize(len);

        int i = 0;
        int j = m_cursor + len -1;
        array<Byte>^ bytes = gcnew array<Byte>(len);

        while ( i < len )
        {
          bytes[i++] = m_buffer[j--];
        }
        m_cursor += len;
        return bytes;
      }

      float DataInput::ReadFloat( )
      {
        float data;

        array<Byte>^ bytes = nullptr;          
        if(BitConverter::IsLittleEndian)
          bytes = ReadReverseBytesOnly(4);
        else
          bytes = ReadBytesOnly(4);

        data = BitConverter::ToSingle(bytes, 0);
        
        return data;
      }

      double DataInput::ReadDouble( )
      {
        double data;

        array<Byte>^ bytes = nullptr;          
        if(BitConverter::IsLittleEndian)
          bytes = ReadReverseBytesOnly(8);
        else
          bytes = ReadBytesOnly(8);

        data = BitConverter::ToDouble(bytes, 0);
        
        return data;
      }

      String^ DataInput::ReadUTF( )
      {
        int length = ReadInt16();
        CheckBufferSize(length);
        String^ str = DecodeBytes(length);
        return str;
      }

      String^ DataInput::ReadUTFHuge( )
      {
        int length = ReadInt32();
        CheckBufferSize(length);
        
        array<Char>^ chArray = gcnew array<Char>(length);
        
        for (int i = 0; i < length; i++)
        {
          Char ch = ReadByte();
          ch = ((ch << 8) | ReadByte());
          chArray[i] = ch;
        }

        String^ str = gcnew String(chArray);

        return str;
      }

      String^ DataInput::ReadASCIIHuge( )
      {
        int length = ReadInt32();
        CheckBufferSize(length);
        String^ str = DecodeBytes(length);
        return str;
      }

      IGFSerializable^ DataInput::ReadObject( )
      {
        return ReadInternalObject();        
      }

      /*
      Object^ DataInput::ReadGenericObject( )
      {
        return ReadInternalGenericObject();        
      }
      */

      
      IGFSerializable^ DataInput::ReadInternalObject( )
      {
        bool findinternal = false;
        int8_t typeId = ReadByte();
        int64_t compId = typeId;
        TypeFactoryMethod^ createType = nullptr;

        if (compId == GemfireTypeIds::NullObj) {
          return nullptr;
        }
        else if (compId == GemfireTypeIds::CacheableNullString) {
          //return SerializablePtr(CacheableString::createDeserializable());
          //TODO::
          return nullptr;
        }
        else if (compId == GemfireTypeIdsImpl::CacheableUserData) {
          int8_t classId = ReadByte();
          //compId |= ( ( (int64_t)classId ) << 32 );
          compId = (int64_t)classId;
        } else if ( compId == GemfireTypeIdsImpl::CacheableUserData2 ) {
          int16_t classId = ReadInt16();
          //compId |= ( ( (int64_t)classId ) << 32 );
          compId = (int64_t)classId;
        } else if ( compId == GemfireTypeIdsImpl::CacheableUserData4 ) {
          int32_t classId = ReadInt32();
          //compId |= ( ( (int64_t)classId ) << 32 );
          compId = (int64_t)classId;
        }else if (compId == GemfireTypeIdsImpl::FixedIDByte) {//TODO:hitesh need to verify again
          int8_t fixedId = ReadByte();
          compId = fixedId;
          findinternal = true;
        } else if (compId == GemfireTypeIdsImpl::FixedIDShort) {
          int16_t fixedId = ReadInt16();
          compId = fixedId;
          findinternal = true;
        } else if (compId == GemfireTypeIdsImpl::FixedIDInt) {
          int32_t fixedId = ReadInt32();
          compId = fixedId;
          findinternal = true;
        }
        if (findinternal) {
          compId += 0x80000000;
          createType = GemStone::GemFire::Cache::Serializable::GetManagedDelegate((int64_t)compId);
        } else {
            createType = GemStone::GemFire::Cache::Serializable::GetManagedDelegate(compId);
            if(createType == nullptr)
            {
              compId += 0x80000000;
              createType = GemStone::GemFire::Cache::Serializable::GetManagedDelegate(compId);

              /*if (createType == nullptr)
              {
                //TODO::hitesh final check for user type if its not in cache 
                compId -= 0x80000000;
                createType = GemStone::GemFire::Cache::Serializable::GetManagedDelegate(compId);
              }*/
            }
        }
        if ( createType == nullptr ) {
          throw gcnew IllegalStateException( "Unregistered typeId in deserialization, aborting." );
        }

        IGFSerializable^ newObj = createType();
        newObj->FromData(this);
        return newObj;
      }

      /*
      Object^ DataInput::ReadInternalGenericObject( )
      {
        bool findinternal = false;
        int8_t typeId = ReadByte();
        int64_t compId = typeId;
        TypeFactoryMethodGeneric^ createType = nullptr;

        if (compId == GemfireTypeIds::NullObj) {
          return nullptr;
        }
        else if (compId == GemfireTypeIds::CacheableNullString) {
          //return SerializablePtr(CacheableString::createDeserializable());
          //TODO::
          return nullptr;
        }
        else if (compId == GemfireTypeIdsImpl::CacheableUserData) {
          int8_t classId = ReadByte();
          //compId |= ( ( (int64_t)classId ) << 32 );
          compId = (int64_t)classId;
        } else if ( compId == GemfireTypeIdsImpl::CacheableUserData2 ) {
          int16_t classId = ReadInt16();
          //compId |= ( ( (int64_t)classId ) << 32 );
          compId = (int64_t)classId;
        } else if ( compId == GemfireTypeIdsImpl::CacheableUserData4 ) {
          int32_t classId = ReadInt32();
          //compId |= ( ( (int64_t)classId ) << 32 );
          compId = (int64_t)classId;
        }else if (compId == GemfireTypeIdsImpl::FixedIDByte) {//TODO:hitesh need to verify again
          int8_t fixedId = ReadByte();
          compId = fixedId;
          findinternal = true;
        } else if (compId == GemfireTypeIdsImpl::FixedIDShort) {
          int16_t fixedId = ReadInt16();
          compId = fixedId;
          findinternal = true;
        } else if (compId == GemfireTypeIdsImpl::FixedIDInt) {
          int32_t fixedId = ReadInt32();
          compId = fixedId;
          findinternal = true;
        }
        if (findinternal) {
          compId += 0x80000000;
          createType = GemStone::GemFire::Cache::Serializable::GetManagedDelegateGeneric((int64_t)compId);
        } else {
            createType = GemStone::GemFire::Cache::Serializable::GetManagedDelegateGeneric(compId);
            if(createType == nullptr)
            {
              Object^ retVal = ReadDotNetTypes(typeId);

              if(retVal != nullptr)
                return retVal;

              compId += 0x80000000;
              createType = Serializable::GetManagedDelegateGeneric(compId);              
            }
        }

        if ( createType != nullptr )
        {
          IGFSerializable^ newObj = createType();
          newObj->FromData(this);
          return newObj;
        }
        
        throw gcnew IllegalStateException( "Unregistered typeId in deserialization, aborting." );
      }
      */

      uint32_t DataInput::BytesRead::get( )
      {
        AdvanceUMCursor();
        SetBuffer();

        return NativePtr->getBytesRead();
      }

      uint32_t DataInput::BytesReadInternally::get()
      {
        return m_cursor;
      }

      uint32_t DataInput::BytesRemaining::get( )
      {
        AdvanceUMCursor();
        SetBuffer();
        return NativePtr->getBytesRemaining();
        //return m_bufferLength - m_cursor;
      }

      void DataInput::AdvanceCursor( int32_t offset )
      {
        m_cursor += offset;
      }

      void DataInput::RewindCursor( int32_t offset )
      {
        AdvanceUMCursor();        
        NativePtr->rewindCursor(offset);
        SetBuffer();
        //m_cursor -= offset;
      }

      void DataInput::Reset()
      {
        AdvanceUMCursor();
        NativePtr->reset();
        SetBuffer();
      //  m_cursor = 0;
      }

      void DataInput::Cleanup( )
      {
        //TODO:hitesh
        //GF_SAFE_DELETE_ARRAY(m_buffer);
        InternalCleanup( );
      }

    }
  }
}
