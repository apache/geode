/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include "cppcache/DataInput.hpp"
#include "impl/NativeWrapper.hpp"
#include "LogM.hpp"
#include "ExceptionTypesM.hpp"
#include "CacheableDateM.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      interface class IGFSerializable;

      /// <summary>
      /// Provides operations for reading primitive data values, byte arrays,
      /// strings, <c>IGFSerializable</c> objects from a byte stream.
      /// </summary>
      [Obsolete("Use classes and APIs from the GemStone.GemFire.Cache.Generic namespace")]
      public ref class DataInput sealed
        : public Internal::UMWrap<gemfire::DataInput>
      {
      public:

        /// <summary>
        /// Construct <c>DataInput</c> using an given array of bytes.
        /// </summary>
        /// <param name="buffer">
        /// The buffer to use for reading data values.
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// if the buffer is null
        /// </exception>
        DataInput( array<Byte>^ buffer );

        /// <summary>
        /// Construct <c>DataInput</c> using a given length of an array of
        /// bytes.
        /// </summary>
        /// <param name="buffer">
        /// The buffer to use for reading data values.
        /// </param>
        /// <param name="len">
        /// The number of bytes from the start of the buffer to use.
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// if the buffer is null
        /// </exception>
        DataInput( array<Byte>^ buffer, int32_t len );

        /// <summary>
        /// Dispose: frees the internal buffer.
        /// </summary>
        ~DataInput( ) { Cleanup( ); }

        /// <summary>
        /// Finalizer: frees the internal buffer.
        /// </summary>
        !DataInput( ) { Cleanup( ); }

        /// <summary>
        /// Read a byte from the stream.
        /// </summary>
        Byte ReadByte( );

        /// <summary>
        /// Read a signed byte from the stream.
        /// </summary>
        SByte ReadSByte( );

        /// <summary>
        /// Read a boolean value from the stream.
        /// </summary>
        bool ReadBoolean( );

        /// <summary>
        /// Read an array of bytes from the stream reading the length
        /// from the stream first.
        /// </summary>
        array<Byte>^ ReadBytes( );

        /// <summary>
        /// Read an array of signed bytes from the stream reading the length
        /// from the stream first.
        /// </summary>
        array<SByte>^ ReadSBytes( );

        /// <summary>
        /// Read the given number of bytes from the stream.
        /// </summary>
        /// <param name="len">Number of bytes to read.</param>
        array<Byte>^ ReadBytesOnly( uint32_t len );

        void ReadBytesOnly( array<Byte> ^ buffer, int offset, int count );

        /// <summary>
        /// Read the given number of signed bytes from the stream.
        /// </summary>
        /// <param name="len">Number of signed bytes to read.</param>
        array<SByte>^ ReadSBytesOnly( uint32_t len );

        /// <summary>
        /// Read a 16-bit unsigned integer from the stream.
        /// </summary>
        uint16_t ReadUInt16( );

        /// <summary>
        /// Read a 32-bit unsigned integer from the stream.
        /// </summary>
        uint32_t ReadUInt32( );

        /// <summary>
        /// Read a array len based on array size.
        /// </summary>
        int ReadArrayLen( );

        /// <summary>
        /// Read a 64-bit unsigned integer from the stream.
        /// </summary>
        uint64_t ReadUInt64( );

        /// <summary>
        /// Read a 16-bit integer from the stream.
        /// </summary>
        int16_t ReadInt16( );

        /// <summary>
        /// Read a 32-bit integer from the stream.
        /// </summary>
        int32_t ReadInt32( );

        /// <summary>
        /// Read a 64-bit integer from the stream.
        /// </summary>
        int64_t ReadInt64( );

        /// <summary>
        /// Read a floating point number from the stream.
        /// </summary>
        float ReadFloat( );

        /// <summary>
        /// Read a double precision number from the stream.
        /// </summary>
        double ReadDouble( );

        /// <summary>
        /// Read a string after java-modified UTF-8 decoding from the stream.
        /// The maximum length supported is 2^16-1 beyond which the string
        /// shall be truncated.
        /// </summary>
        String^ ReadUTF( );

        /// <summary>
        /// Read a string after java-modified UTF-8 decoding from the stream.
        /// </summary>
        String^ ReadUTFHuge( );

        /// <summary>
        /// Read a ASCII string from the stream. Where size is more than 2^16-1 
        /// </summary>
        String^ ReadASCIIHuge( );

        /// <summary>
        /// Read a serializable object from the data. Null objects are handled.
        /// </summary>
        IGFSerializable^ ReadObject( );

       /* /// <summary>
        /// Read a serializable object from the data. Null objects are handled.
        /// </summary>
        ///Object^ ReadGenericObject( );*/

        /// <summary>
        /// Get the count of bytes that have been read from the stream.
        /// </summary>
        property uint32_t BytesRead
        {
          uint32_t get( );
        }

        /// <summary>
        /// Get the count of bytes that are remaining in the buffer.
        /// </summary>
        property uint32_t BytesRemaining
        {
          uint32_t get();
        }

        /// <summary>
        /// Advance the cursor of the buffer by the given offset.
        /// </summary>
        /// <param name="offset">
        /// The offset(number of bytes) by which to advance the cursor.
        /// </param>
        void AdvanceCursor( int32_t offset );

        /// <summary>
        /// Rewind the cursor of the buffer by the given offset.
        /// </summary>
        /// <param name="offset">
        /// The offset(number of bytes) by which to rewind the cursor.
        /// </param>
        void RewindCursor( int32_t offset );

        /// <summary>
        /// Reset the cursor to the start of buffer.
        /// </summary>
        void Reset();

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

        System::Collections::Generic::IDictionary<Object^, Object^>^ ReadDictionary()
        {
          int len = this->ReadArrayLen();

          if(len == -1)
            return nullptr;
          else
          {
            System::Collections::Generic::IDictionary<Object^, Object^>^ dict = gcnew Dictionary<Object^, Object^>();
            for(int i =0; i< len; i++)
            {
							//TODO::split
              //Object^ key = this->ReadGenericObject();
              //Object^ val = this->ReadGenericObject();

              //dict->Add(key, val);
            }
            return dict;
          }

        }
        
        System::DateTime ReadDate()
        {
          int64_t ticks = ReadInt64();
          if(ticks != -1L)
          {
            m_cursor -= 8;
            CacheableDate^ cd = gcnew CacheableDate();
            return ((CacheableDate^)cd->FromData(this))->Value;
          }else{
            DateTime dt(0);
            return dt;
          }
        }

      internal:

        
        /// <summary>
        /// Get the count of bytes that have been read from the stream, for internale use only.
        /// </summary>
        property uint32_t BytesReadInternally
        {
          uint32_t get( );
        }

        void ReadObject(bool% obj)
        {
          obj = ReadBoolean();
        }

        void ReadObject(Byte% obj)
        {
          obj = ReadByte();
        }

        void ReadObject(Char% obj)
        {
          obj = (Char)ReadUInt16();
        }

        inline Char decodeChar( )
        {
          Char retChar;
          int b = m_buffer[ m_cursor++ ] & 0xff;
          int k = b >> 5;
          switch (  k )
            {
            default:
              retChar = ( Char ) ( b & 0x7f );
              break;
            case 6:
              {
                // two byte encoding
                // 110yyyyy 10xxxxxx
                // use low order 6 bits
                int y = b & 0x1f;
                // use low order 6 bits of the next byte
                // It should have high order bits 10, which we don't check.
                int x = m_buffer[ m_cursor++ ] & 0x3f;
                // 00000yyy yyxxxxxx
                retChar = ( Char ) ( y << 6 | x );
                break;
              }
            case 7:
              {
                // three byte encoding
                // 1110zzzz 10yyyyyy 10xxxxxx
                //assert ( b & 0x10 )
                  //     == 0 : "UTF8Decoder does not handle 32-bit characters";
                // use low order 4 bits
                int z = b & 0x0f;
                // use low order 6 bits of the next byte
                // It should have high order bits 10, which we don't check.
                int y = m_buffer[ m_cursor++ ] & 0x3f;
                // use low order 6 bits of the next byte
                // It should have high order bits 10, which we don't check.
                int x = m_buffer[ m_cursor++ ] & 0x3f;
                // zzzzyyyy yyxxxxxx
                int asint = ( z << 12 | y << 6 | x );
                retChar = ( Char ) asint;
                break;
              }
            }// end switch

            return retChar;
        }

        System::Collections::Hashtable^ ReadHashtable()
        {
          int len = this->ReadArrayLen();

          if(len == -1)
            return nullptr;
          else
          {
            System::Collections::Hashtable^ dict = gcnew System::Collections::Hashtable();
            for(int i =0; i< len; i++)
            {
							//TODO::split
              //Object^ key = this->ReadGenericObject();
              //Object^ val = this->ReadGenericObject();

              //dict->Add(key, val);
            }
            return dict;
          }
        }

        void ReadObject(Double% obj)
        {
          obj = ReadDouble();
        }

        void ReadObject(Single% obj)
        {
          obj = ReadFloat();
        }

        void ReadObject(int16_t% obj)
        {
          obj = ReadInt16();
        }

        void ReadObject(int32_t% obj)
        {
          obj = ReadInt32();
        }

        void ReadObject(int64_t% obj)
        {
          obj = ReadInt64();
        }

        template <typename mType>
        void ReadObject(array<mType>^ %objArray)
        {
          int arrayLen = ReadArrayLen();
          if(arrayLen > 0) {
            objArray = gcnew array<mType>(arrayLen);

            int i = 0;
            for( i = 0; i < arrayLen; i++ ){
              mType tmp;
              ReadObject(tmp);
              objArray[i] =  tmp; 
            }
          }
        }

        Object^ ReadStringArray()
        {
          int len = this->ReadArrayLen();
          if ( len == -1)
          {
            return nullptr;
          }
          else 
          {
            array<String^>^ ret = gcnew array<String^>(len);
            if (len > 0)
            {
              for( int i = 0; i < len; i++)
              {
                ret[i] = this->ReadUTF();
              }
            }
            return ret;
          }
        }
        
        void AdvanceUMCursor()
        {
          NativePtr->advanceCursor(m_cursor);
          m_cursor = 0;
          m_bufferLength = 0;
        }

        inline array<Byte>^ ReadReverseBytesOnly(int len);

        void SetBuffer()
        {
          m_buffer = const_cast<uint8_t*> (NativePtr->currentBufferPosition());
          m_cursor = 0;
          m_bufferLength = NativePtr->getBytesRemaining();   
        }

        String^ DecodeBytes(int length)
        {
          array<Char>^ output = gcnew array<Char>(length);
          // index input[]
          int i = 0;
          // index output[]
          int j = 0;
          while ( i < length )
          {
            // get next byte unsigned
            int b = m_buffer[ m_cursor++ ] & 0xff;
            i++;
            int k = b >> 5;
            // classify based on the high order 3 bits
            switch (  k )
              {
              default:
                // one byte encoding
                // 0xxxxxxx
                // use just low order 7 bits
                // 00000000 0xxxxxxx
                output[ j++ ] = ( Char ) ( b & 0x7f );
                break;
              case 6:
                {
                  // two byte encoding
                  // 110yyyyy 10xxxxxx
                  // use low order 6 bits
                  int y = b & 0x1f;
                  // use low order 6 bits of the next byte
                  // It should have high order bits 10, which we don't check.
                  int x = m_buffer[ m_cursor++ ] & 0x3f;
                  i++;
                  // 00000yyy yyxxxxxx
                  output[ j++ ] = ( Char ) ( y << 6 | x );
                  break;
                }
              case 7:
                {
                  // three byte encoding
                  // 1110zzzz 10yyyyyy 10xxxxxx
                  //assert ( b & 0x10 )
                    //     == 0 : "UTF8Decoder does not handle 32-bit characters";
                  // use low order 4 bits
                  int z = b & 0x0f;
                  // use low order 6 bits of the next byte
                  // It should have high order bits 10, which we don't check.
                  int y = m_buffer[ m_cursor++ ] & 0x3f;
                  i++;
                  // use low order 6 bits of the next byte
                  // It should have high order bits 10, which we don't check.
                  int x = m_buffer[ m_cursor++ ] & 0x3f;
                  i++;
                  // zzzzyyyy yyxxxxxx
                  int asint = ( z << 12 | y << 6 | x );
                  output[ j++ ] = ( Char ) asint;
                  break;
                }
              }// end switch
          }// end while

          String^ str = gcnew String(output, 0, j);
          return str;
        }

        void CheckBufferSize(int size)
        {
          if( (unsigned int)(m_cursor + size) > m_bufferLength )
            throw gcnew GemStone::GemFire::Cache::OutOfRangeException("DataInput: attempt to read beyond buffer");
        }

       // Object^ ReadInternalGenericObject();

        IGFSerializable^ ReadInternalObject();

        /// <summary>
        /// Internal constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline DataInput( gemfire::DataInput* nativeptr, bool managedObject )
          : UMWrap(nativeptr, false)
        { 
          m_cursor = 0;
          m_isManagedObject = managedObject;
          m_buffer = const_cast<uint8_t*>(nativeptr->currentBufferPosition());
          if ( m_buffer != NULL) {
            m_bufferLength = nativeptr->getBytesRemaining();            
          }
          else {
            m_bufferLength = 0;
          }
        }

       /* inline DataInput( gemfire::DataInput* nativeptr )
          : UMWrap(nativeptr, false)
        { 
          m_cursor = 0;
          m_isManagedObject = false;
          m_buffer = const_cast<uint8_t*>(nativeptr->currentBufferPosition());
          if ( m_buffer != NULL) {
            m_bufferLength = nativeptr->getBytesRemaining();            
          }
          else {
            m_bufferLength = 0;
          }
        }*/

        bool IsManagedObject()
        {
          return m_isManagedObject;
        }

      private:

        /// <summary>
        /// Internal buffer managed by the class.
        /// This is freed in the disposer/destructor.
        /// </summary>
        uint8_t* m_buffer;
        unsigned int m_bufferLength;
        int m_cursor;
        bool m_isManagedObject;
      
        void Cleanup( );
      };

    }
  }
}
