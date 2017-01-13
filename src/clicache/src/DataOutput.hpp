/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include <gfcpp/DataOutput.hpp>
//#include "impl/NativeWrapper.hpp"
#include "Log.hpp"
#include "ExceptionTypes.hpp"
#include "Serializable.hpp"

#include "CacheableString.hpp"
#include "CacheableDate.hpp"
#include "CacheableVector.hpp"
#include "CacheableArrayList.hpp"
#include "CacheableStack.hpp"

using namespace System;
using namespace System::Runtime::CompilerServices;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Generic
      {

      interface class IGFSerializable;

      /// <summary>
      /// Provides operations for writing primitive data values, and
      /// user-defined objects implementing IGFSerializable, to a byte stream.
      /// This class is intentionally not thread safe.
      /// </summary>
      public ref class DataOutput sealed
				: public Generic::Internal::UMWrap<gemfire::DataOutput>
      {
      private:
        int32_t m_cursor;
        bool m_isManagedObject;
        uint8_t * m_bytes;
        int32_t m_remainingBufferLength;
        bool m_ispdxSerialization;
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
          m_remainingBufferLength = (int32_t)NativePtr->getRemainingBufferLength();
          m_ispdxSerialization = false;
        }

        /// <summary>
        /// Write length of the array to the <c>DataOutput</c>.
        /// </summary>
        /// <param name="len">Array len to write.</param>
        void WriteArrayLen( int32_t len );
        
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
        /// Write a char value to the <c>DataOutput</c>.
        /// </summary>
        /// <param name="value">The char value to write.</param>
        void WriteChar( Char value );

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
       // void WriteObject( IGFSerializable^ obj );

        /// <summary>
        /// Write a <c>Serializable</c> object to the <c>DataOutput</c>.
        /// This is provided to conveniently pass primitive types (like string)
        /// that shall be implicitly converted to corresponding
        /// <c>IGFSerializable</c> wrapper types.
        /// </summary>
        /// <param name="obj">The object to write.</param>
        void WriteObject( Object^ obj );

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
       
        /// <summary>
        /// Write a Dictionary to the DataOutput.
        /// </summary>
        /// <param name="value">The object which implements IDictionary to write.</param>
 			  void WriteDictionary(System::Collections::IDictionary^ value);              

        /// <summary>
        /// Write a date to the DataOutput.
        /// </summary>
        /// <param name="value">The date value to write.</param>
        void WriteDate(System::DateTime value);

        /// <summary>
        /// Write a collection to the DataOutput.
        /// </summary>
        /// <param name="value">The object which implements IList to write.</param>
        void WriteCollection(System::Collections::IList^ value);
        
        /// <summary>
        /// Write a char array to the DataOutput.
        /// </summary>
        /// <param name="value">The char array to write.</param>
        void WriteCharArray(array<Char>^ value);

        /// <summary>
        /// Write a bool array to the DataOutput.
        /// </summary>
        /// <param name="value">The bool array to write.</param>
				void WriteBooleanArray(array<bool>^ value);

        /// <summary>
        /// Write a short array to the DataOutput.
        /// </summary>
        /// <param name="value">The short array to write.</param>
				void WriteShortArray(array<Int16>^ value);

        /// <summary>
        /// Write a int array to the DataOutput.
        /// </summary>
        /// <param name="value">The int array to write.</param>
				void WriteIntArray(array<Int32>^ value);

        /// <summary>
        /// Write a long array to the DataOutput.
        /// </summary>
        /// <param name="value">The long array to write.</param>
				void WriteLongArray(array<Int64>^ value);

        /// <summary>
        /// Write a float array to the DataOutput.
        /// </summary>
        /// <param name="value">The float array to write.</param>
				void WriteFloatArray(array<float>^ value);

        /// <summary>
        /// Write a double array to the DataOutput.
        /// </summary>
        /// <param name="value">The double array to write.</param>
				void WriteDoubleArray(array<double>^ value);

        /// <summary>
        /// Write a object array to the DataOutput.
        /// </summary>
        /// <param name="value">The object array to write.</param>
        void WriteObjectArray(List<Object^>^ value);

        /// <summary>
        /// Write a array of sign byte array to the DataOutput.
        /// </summary>
        /// <param name="value">The array of sign byte array to write.</param>
        void WriteArrayOfByteArrays(array<array<Byte>^>^ value);
               
      internal:

        void WriteDotNetObjectArray(Object^ objectArray);

        /// <summary>
        /// Write a byte to the <c>DataOutput</c>.
        /// </summary>
        /// <param name="value">The byte to write.</param>
        void WriteByte( Byte value );

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


			  int32_t GetBufferLengthPdx()
        {
          return (int32_t)NativePtr->getBufferLength();
        }

        void WriteString(String^ value);

        int32_t GetCursorPdx()
        {
          return m_cursor;
        }

        const char * GetPoolName()
        {
          return _NativePtr->getPoolName();
        }

        void WriteStringArray(array<String^>^ strArray);

        void EncodeUTF8String( String^ input, int encLength )
        {
          const int strLength = input->Length;
          const int end = m_cursor + encLength;
          for ( int i = 0; i < strLength && m_cursor < end; i++ )
          {
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

		  // TODO ASSERT end = m_cursor
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

        void setPdxSerialization(bool val)
        {
          m_ispdxSerialization = val;
        }

        void WriteStringWithType( String^ value );

        static int8_t GetTypeId(uint32_t classId );
        
        static int8_t DSFID(uint32_t classId);        
  
        void WriteObjectInternal( IGFSerializable^ obj );     

        void WriteBytesToUMDataOutput();
        
        void WriteObject(bool% obj);        

        void WriteObject(Byte% obj);        

        void WriteObject(Char% obj);        

        void WriteObject(Double% obj);
        
        void WriteObject(Single% obj);
        
        void WriteObject(int16_t% obj);
        
        void WriteObject(int32_t% obj);
        
        void WriteObject(int64_t% obj);
        
				void WriteObject(UInt16% obj);
        
        void WriteObject(UInt32% obj);       

        void WriteObject(UInt64% obj);
        
       // void WriteObject(array<UInt16>^ objArray);
        //void WriteObject(array<UInt32>^ objArray);
        //void WriteObject(array<UInt64>^ objArray);

        
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
          //TODO::
          return m_isManagedObject;
        }

        void SetBuffer()
        {
          m_cursor = 0;
          m_bytes = const_cast<uint8_t *>(NativePtr->getCursor());
          m_remainingBufferLength = (int32_t)NativePtr->getRemainingBufferLength();
        }

				uint8_t* GetStartBufferPosition()
        {
          return const_cast<uint8_t *>( NativePtr->getBuffer());;
        }

        inline void EnsureCapacity( int32_t size )
        {
          int32_t bytesLeft = m_remainingBufferLength - m_cursor;
          if ( bytesLeft < size ) {
            try
            {
              NativePtr->ensureCapacity(m_cursor + size);
              m_bytes = const_cast<uint8_t *>( NativePtr->getCursor());
              m_remainingBufferLength = (int32_t)NativePtr->getRemainingBufferLength();
            }
            catch(gemfire::OutOfMemoryException ex )
            {
              throw gcnew OutOfMemoryException(ex);
            }            
          }
        }

        //it expects list is not null
        inline void WriteList(System::Collections::IList^ list)
        {
          this->WriteArrayLen(list->Count);
          for each (Object^ obj in list) 
						this->WriteObject(obj);
        }

        uint8_t* GetBytes(uint8_t* src, uint32_t size)
        {
          return NativePtr->getBufferCopyFrom(src, size);
        }
 
        int32_t GetRemainingBufferLength()
        {
          return (int32_t) NativePtr->getRemainingBufferLength();
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
          m_remainingBufferLength = (int32_t)nativeptr->getRemainingBufferLength();
          m_ispdxSerialization = false;
        }
      };
      } // end namespace generic
    }
  }
}
