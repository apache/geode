/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include "cppcache/DataOutput.hpp"
#include "impl/NativeWrapper.hpp"
#include "LogM.hpp"
#include "ExceptionTypesM.hpp"
#include "SerializableM.hpp"
#include "SerializableM.hpp"

#include "CacheableStringM.hpp"
#include "CacheableDateM.hpp"
#include "CacheableVectorM.hpp"
#include "CacheableArrayListM.hpp"
#include "CacheableStackM.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      interface class IGFSerializable;

      /// <summary>
      /// Provides operations for writing primitive data values, and
      /// user-defined objects implementing IGFSerializable, to a byte stream.
      /// This class is intentionally not thread safe.
      /// </summary>
      [Obsolete("Use classes and APIs from the GemStone.GemFire.Cache.Generic namespace")]
      public ref class DataOutput sealed
        : public Internal::UMWrap<gemfire::DataOutput>
      {
      private:
        int m_cursor;
        bool m_isManagedObject;
        uint8_t * m_bytes;
        uint32_t m_remainingBufferLength;
      public:

        /// <summary>
        /// Default constructor.
        /// </summary>
        inline DataOutput( )
          : UMWrap( new gemfire::DataOutput( ), true )
        { 
          m_isManagedObject = true;
          m_cursor = 0;
          m_bytes = const_cast<uint8_t *>(NativePtr->getCursor());
          m_remainingBufferLength = NativePtr->getRemainingBufferLength();
        }

        /// <summary>
        /// Write length of the array to the <c>DataOutput</c>.
        /// </summary>
        /// <param name="len">Array len to write.</param>
        void WriteArrayLen( int32_t len );

        /// <summary>
        /// Write a byte to the <c>DataOutput</c>.
        /// </summary>
        /// <param name="value">The byte to write.</param>
        void WriteByte( Byte value );

        /// <summary>
        /// Write a signed byte to the <c>DataOutput</c>.
        /// </summary>
        /// <param name="value">The signed byte to write.</param>
        void WriteSByte( SByte value );

        /// <summary>
        /// Write a boolean value to the <c>DataOutput</c>.
        /// </summary>
        /// <param name="value">The boolean value to write.</param>
        void WriteBoolean( bool value );

        /// <summary>
        /// Write a given length of bytes to the <c>DataOutput</c>.
        /// </summary>
        /// <param name="bytes">The array of bytes to write.</param>
        /// <param name="len">
        /// The number of bytes from the start of array to write.
        /// </param>
        void WriteBytes( array<Byte>^ bytes, int32_t len );

        /// <summary>
        /// Write an array of bytes to the <c>DataOutput</c>.
        /// </summary>
        /// <param name="bytes">The array of bytes to write.</param>
        inline void WriteBytes( array<Byte>^ bytes )
        {
          WriteBytes( bytes, ( bytes == nullptr ? -1 : bytes->Length ) );
        }

        /// <summary>
        /// Write a given length of signed bytes to the <c>DataOutput</c>.
        /// </summary>
        /// <param name="bytes">The array of signed bytes to write.</param>
        /// <param name="len">
        /// The number of bytes from the start of array to write.
        /// </param>
        void WriteSBytes( array<SByte>^ bytes, int32_t len );

        /// <summary>
        /// Write an array of signed bytes to the <c>DataOutput</c>.
        /// </summary>
        /// <param name="bytes">The array of signed bytes to write.</param>
        inline void WriteSBytes( array<SByte>^ bytes )
        {
          WriteSBytes( bytes, ( bytes == nullptr ? -1 : bytes->Length )  );
        }

        /// <summary>
        /// Write a given length of bytes without its length to the
        /// <c>DataOutput</c>.
        /// </summary>
        /// <param name="bytes">The array of bytes to write.</param>
        /// <param name="len">
        /// The number of bytes from the start of array to write.
        /// </param>
        void WriteBytesOnly( array<Byte>^ bytes, uint32_t len );

        void WriteBytesOnly( array<Byte>^ bytes, uint32_t len, uint32_t offset );

        /// <summary>
        /// Write an array of bytes without its length to the
        /// <c>DataOutput</c>.
        /// </summary>
        /// <param name="bytes">The array of bytes to write.</param>
        inline void WriteBytesOnly( array<Byte>^ bytes )
        {
          WriteBytesOnly( bytes, ( bytes == nullptr ? 0 : bytes->Length )  );
        }

        /// <summary>
        /// Write a given length of signed bytes without its length
        /// to the <c>DataOutput</c>.
        /// </summary>
        /// <param name="bytes">The array of signed bytes to write.</param>
        /// <param name="len">
        /// The number of bytes from the start of array to write.
        /// </param>
        void WriteSBytesOnly( array<SByte>^ bytes, uint32_t len );

        /// <summary>
        /// Write an array of signed bytes without its length
        /// to the <c>DataOutput</c>.
        /// </summary>
        /// <param name="bytes">The array of signed bytes to write.</param>
        inline void WriteSBytesOnly( array<SByte>^ bytes )
        {
          WriteSBytesOnly( bytes, ( bytes == nullptr ? 0 : bytes->Length )  );
        }

        /// <summary>
        /// Write an unsigned short integer (int16_t) to the <c>DataOutput</c>.
        /// </summary>
        /// <param name="value">The unsigned 16-bit integer to write.</param>
        void WriteUInt16( uint16_t value );

        /// <summary>
        /// Write an unsigned 32-bit integer to the <c>DataOutput</c>.
        /// </summary>
        /// <param name="value">The unsigned 32-bit integer to write.</param>
        void WriteUInt32( uint32_t value );

        /// <summary>
        /// Write an unsigned 64-bit integer to the <c>DataOutput</c>.
        /// </summary>
        /// <param name="value">The unsigned 64-bit integer to write.</param>
        void WriteUInt64( uint64_t value );

        /// <summary>
        /// Write a 16-bit integer to the <c>DataOutput</c>.
        /// </summary>
        /// <param name="value">The 16-bit integer to write.</param>
        void WriteInt16( int16_t value );

        /// <summary>
        /// Write a 32-bit integer to the <c>DataOutput</c>.
        /// </summary>
        /// <param name="value">The 32-bit integer to write.</param>
        void WriteInt32( int32_t value );

        /// <summary>
        /// Write a 64-bit integer to the <c>DataOutput</c>.
        /// </summary>
        /// <param name="value">The 64-bit integer to write.</param>
        void WriteInt64( int64_t value );

        /// <summary>
        /// Write a float to the DataOutput.
        /// </summary>
        /// <param name="value">The float value to write.</param>
        void WriteFloat( float value );

        /// <summary>
        /// Write a double precision real number to the <c>DataOutput</c>.
        /// </summary>
        /// <param name="value">
        /// The double precision real number to write.
        /// </param>
        void WriteDouble( double value );

        /// <summary>
        /// Write a string using java-modified UTF-8 encoding to
        /// <c>DataOutput</c>.
        /// The maximum length supported is 2^16-1 beyond which the string
        /// shall be truncated.
        /// </summary>
        /// <param name="value">The UTF encoded string to write.</param>
        void WriteUTF( String^ value );

        /// <summary>
        /// Write a string using java-modified UTF-8 encoding to
        /// <c>DataOutput</c>.
        /// Length should be more than 2^16 -1. 
        /// </summary>
        /// <param name="value">The UTF encoded string to write.</param>
        void WriteUTFHuge( String^ value );

        /// <summary>
        /// Write a string(only ASCII char) to
        /// <c>DataOutput</c>.
        /// Length should be more than 2^16 -1.
        /// </summary>
        /// <param name="value">The UTF encoded string to write.</param>
        void WriteASCIIHuge( String^ value );

        /// <summary>
        /// Write an <c>IGFSerializable</c> object to the <c>DataOutput</c>.
        /// </summary>
        /// <param name="obj">The object to write.</param>
        void WriteObject( IGFSerializable^ obj );

        /// <summary>
        /// Write a <c>Serializable</c> object to the <c>DataOutput</c>.
        /// This is provided to conveniently pass primitive types (like string)
        /// that shall be implicitly converted to corresponding
        /// <c>IGFSerializable</c> wrapper types.
        /// </summary>
        /// <param name="obj">The object to write.</param>
        void WriteObject( GemStone::GemFire::Cache::Serializable^ obj );

        /// <summary>
        /// Advance the buffer cursor by the given offset.
        /// </summary>
        /// <param name="offset">
        /// The offset by which to advance the cursor.
        /// </param>
        void AdvanceCursor( uint32_t offset );

        /// <summary>
        /// Rewind the buffer cursor by the given offset.
        /// </summary>
        /// <param name="offset">
        /// The offset by which to rewind the cursor.
        /// </param>
        void RewindCursor( uint32_t offset );

        /// <summary>
        /// Get a copy of the current buffer.
        /// </summary>
        array<Byte>^ GetBuffer( );

        /// <summary>
        /// Get the length of current data in the buffer.
        /// </summary>
        property uint32_t BufferLength
        {
          uint32_t get( );
        }

        /// <summary>
        /// Reset the cursor to the start of the buffer.
        /// </summary>
        void Reset( );

        /// <summary>
        /// Get the underlying native unmanaged pointer.
        /// </summary>
        property IntPtr NativeIntPtr
        {
          inline IntPtr get()
          {
            return IntPtr(_NativePtr);
          }
        }

       // void WriteGenericObject(Object^ obj);

        void WriteDictionary(System::Collections::IDictionary^ dict)
        {
          if(dict != nullptr)
          {
            this->WriteArrayLen(dict->Count);
            for each( System::Collections::DictionaryEntry^ entry in dict) 
            {
							//TODO::splir
              //this->WriteGenericObject(entry->Key);
              //this->WriteGenericObject(entry->Value);
            }
          }
          else
          {
            WriteByte( (int8_t) -1);
          }
        }
        
        void WriteDate(System::DateTime dt)
        {
          if(dt.Ticks != 0L)
          {
            CacheableDate^ cd = CacheableDate::Create(dt);
            cd->ToData(this);
          }else
            this->WriteInt64(-1L);
        }

      internal:

        int EncodeString( String^ input, int length )
        {
          int j = m_cursor;

          for ( int i = 0; i < input->Length; i++ )
          {
            //EnsureCapacity(3);
            unsigned short c = (unsigned short)input[i];

            if( c == 0 )
            {
                m_bytes[m_cursor++] = 0xc0;
                m_bytes[m_cursor++] = 0x80;
            }
            else if ( c < 0x80 )//ASCII character
            {
              // 7-bits done in one byte.
              m_bytes[m_cursor++] = (uint8_t)c;
            }
            else if ( c < 0x800 )
            {
              // 8-11 bits done in 2 bytes
              m_bytes[m_cursor++] = ( 0xC0 | c >> 6 );
              m_bytes[m_cursor++] = ( 0x80 | c & 0x3F );
            }
            else 
            {
              // 12-16 bits done in 3 bytes
              m_bytes[m_cursor++] = ( 0xE0 | c >> 12 );
              m_bytes[m_cursor++] = ( 0x80 | c >> 6 & 0x3F );
              m_bytes[m_cursor++] = ( 0x80 | c & 0x3F );
            }            
          }

          if(length < (m_cursor - j))//extra byte after 0xffff
            return length;
          return m_cursor - j; //number of bytes
        }
       
        static int getEncodedLength(String^ input)
        {
          int count = 0;
          for ( int i = 0; i < input->Length; i++ )
          {
            unsigned short c = (unsigned short)input[i];

            if( c == 0)
            {
              count += 2;
            }
            else if ( c < 0x80 )//ASCII character
            {
              count++;
            }
            else if ( c < 0x800 )
            {
              count += 2;
            }
            else
            {
               count += 3;
            }
          }// end for

          return count;
        }

        static int8_t GetTypeId(uint32_t classId );
        
        static int8_t DSFID(uint32_t classId);        
  
        void WriteObjectInternal( IGFSerializable^ obj );     

        void WriteBytesToUMDataOutput()
        {
          NativePtr->advanceCursor(m_cursor);
          m_cursor = 0;
          m_remainingBufferLength = 0;
          m_bytes = nullptr;
        }

        void WriteObject(bool% obj)
        {
          WriteBoolean(obj);
        }

        void WriteObject(Byte% obj)
        {
          WriteByte(obj);
        }

        void WriteObject(Char% obj)
        {
          unsigned short us = (unsigned short)obj;
          m_bytes[m_cursor++] = us >> 8;
          m_bytes[m_cursor++] = (Byte)us; 
        }

        void WriteObject(Double% obj)
        {
          WriteDouble(obj);
        }

        void WriteObject(Single% obj)
        {
          WriteFloat(obj);
        }

        void WriteObject(int16_t% obj)
        {
          WriteInt16(obj);
        }

        void WriteObject(int32_t% obj)
        {
          WriteInt32(obj);
        }

        void WriteObject(int64_t% obj)
        {
          WriteInt64(obj);
        }

        
        template <typename mType>
        void WriteObject(array<mType>^ %objArray)
        {
          if(objArray != nullptr) {
            int arrayLen = objArray->Length;
            WriteArrayLen(arrayLen);
            if(arrayLen > 0) {
              int i = 0;
              for( i = 0; i < arrayLen; i++ ){
                WriteObject(objArray[i]);
              }
            }
          }
          else {
            WriteByte(0xff);
          }
        }

        bool IsManagedObject()
        {
          //TODO::HItesh
          return m_isManagedObject;
        }

        void SetBuffer()
        {
          m_cursor = 0;
          m_bytes = const_cast<uint8_t *>(NativePtr->getCursor());
          m_remainingBufferLength = NativePtr->getRemainingBufferLength();
        }

        inline void EnsureCapacity( int32_t size )
        {
          int32_t bytesLeft = m_remainingBufferLength - m_cursor;
          if ( bytesLeft < size ) {
            try
            {
              NativePtr->ensureCapacity(m_cursor + size);
              m_bytes = const_cast<uint8_t *>( NativePtr->getCursor());
              m_remainingBufferLength = NativePtr->getRemainingBufferLength();
            }
            catch(gemfire::OutOfMemoryException ex )
            {
              throw gcnew GemStone::GemFire::Cache::OutOfMemoryException(ex);
            }            
          }
        }

 
        /// <summary>
        /// Internal constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline DataOutput( gemfire::DataOutput* nativeptr, bool managedObject )
          : UMWrap( nativeptr, false )
        {
          m_isManagedObject = managedObject;
          m_cursor = 0;
          m_bytes = const_cast<uint8_t *>(nativeptr->getCursor());
          m_remainingBufferLength = nativeptr->getRemainingBufferLength();
        }

        /*inline DataOutput( gemfire::DataOutput* nativeptr )
          : UMWrap( nativeptr, false )
        {
          m_isManagedObject = false;
          m_cursor = 0;
          m_bytes = const_cast<uint8_t *>(nativeptr->getCursor());
          m_remainingBufferLength = nativeptr->getRemainingBufferLength();
        }*/
      };

    }
  }
}
