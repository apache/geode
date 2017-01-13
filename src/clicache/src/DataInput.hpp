/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include <gfcpp/DataInput.hpp>
#include "impl/NativeWrapper.hpp"
#include "Log.hpp"
#include "ExceptionTypes.hpp"
//#include "../../CacheableDate.hpp"


using namespace System;
using namespace System::Collections::Generic;

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
      /// Provides operations for reading primitive data values, byte arrays,
      /// strings, <c>IGFSerializable</c> objects from a byte stream.
      /// </summary>
      public ref class DataInput sealed
				: public Generic::Internal::UMWrap<gemfire::DataInput>
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
        /// Read a signed byte from the stream.
        /// </summary>
        SByte ReadSByte( );

        /// <summary>
        /// Read a boolean value from the stream.
        /// </summary>
        bool ReadBoolean( );

				/// <summary>
        /// Read a char value from the stream.
        /// </summary>
        Char ReadChar( );

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
        /// Read a array len based on array size.
        /// </summary>
        int ReadArrayLen( );

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
        Object^ ReadObject( );
        
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
        
        /// <summary>
        /// Read a dictionary from the stream in a given dictionary instance.
        /// </summary>
        /// <param name="dictionary">Object which implements System::Collections::IDictionary interface.</param>
        void ReadDictionary(System::Collections::IDictionary^ dictionary);
        
        /// <summary>
        /// Read a date from the stream.
        /// </summary>
				System::DateTime ReadDate( );

        /// <summary>
        /// Read a collection from the stream in a given collection instance.
        /// </summary>
        /// <param name="list">Object which implements System::Collections::IList interface.</param>
        void ReadCollection(System::Collections::IList^ list);
        
        /// <summary>
        /// Read a char array from the stream.
        /// </summary>
        array<Char>^ ReadCharArray( );

        /// <summary>
        /// Read a bool array from the stream.
        /// </summary>
				array<bool>^ ReadBooleanArray( );

        /// <summary>
        /// Read a short int array from the stream.
        /// </summary>
				array<Int16>^ ReadShortArray( );

        /// <summary>
        /// Read a int array from the stream.
        /// </summary>
				array<Int32>^ ReadIntArray();

        /// <summary>
        /// Read a long array from the stream.
        /// </summary>
				array<Int64>^ ReadLongArray();

        /// <summary>
        /// Read a float array from the stream.
        /// </summary>
				array<float>^ ReadFloatArray();

        /// <summary>
        /// Read a double array from the stream.
        /// </summary>
				array<double>^ ReadDoubleArray();

        /// <summary>
        /// Read a object array from the stream from the stream.
        /// </summary>
        List<Object^>^ ReadObjectArray();

        /// <summary>
        /// Read a array of signed byte array from the stream.
        /// </summary>
        array<array<Byte>^>^ ReadArrayOfByteArrays( );

      internal:

        void setPdxdeserialization(bool val)
        {
          m_ispdxDesrialization = true;
        }
        bool isRootObjectPdx()
        {
          return m_isRootObjectPdx;
        }
        void setRootObjectPdx(bool val)
        {
          m_isRootObjectPdx = val;
        }

        Object^ readDotNetObjectArray();
        System::Collections::Generic::IDictionary<Object^, Object^>^ ReadDictionary();

				String^ ReadString();

        const char * GetPoolName()
        {
          return _NativePtr->getPoolName();
        }

        Object^ ReadDotNetTypes(int8_t typeId);

        /// <summary>
        /// Get the count of bytes that have been read from the stream, for internal use only.
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
              Object^ key = this->ReadObject();
              Object^ val = this->ReadObject();

              dict->Add(key, val);
            }
            return dict;
          }
        }

        /// <summary>
        /// Read a byte from the stream.
        /// </summary>
        Byte ReadByte( );

        /// <summary>
        /// Read a 16-bit unsigned integer from the stream.
        /// </summary>
        uint16_t ReadUInt16( );

        /// <summary>
        /// Read a 32-bit unsigned integer from the stream.
        /// </summary>
        uint32_t ReadUInt32( );
       
        /// <summary>
        /// Read a 64-bit unsigned integer from the stream.
        /// </summary>
        uint64_t ReadUInt64( );

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

				 void ReadObject(array<SByte>^% obj)
        {
          obj = ReadSBytes();
        }        

        void DataInput::ReadObject(array<UInt16>^% obj);
        void DataInput::ReadObject(array<UInt32>^% obj);
        void DataInput::ReadObject(array<UInt64>^% obj);

        template <typename mType>
        void ReadObject(array<mType>^ %objArray)
        {
          int arrayLen = ReadArrayLen();
          if(arrayLen >= 0) {
            objArray = gcnew array<mType>(arrayLen);

            int i = 0;
            for( i = 0; i < arrayLen; i++ ){
              mType tmp;
              ReadObject(tmp);
              objArray[i] =  tmp; 
            }
          }
        }

				array<String^>^ ReadStringArray()
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
                Object^ obj = this->ReadObject();
                if(obj != nullptr)
                  ret[i] = static_cast<String^>(obj);
                else
                  ret[i] = nullptr;
              }
            }
            return ret;
          }
        }

				uint8_t* GetCursor()
        {
          return m_buffer + m_cursor;
        }

        uint8_t* GetBytes(uint8_t* src, uint32_t size)
        {
          return NativePtr->getBufferCopyFrom(src, size);
        }

        
        void AdvanceUMCursor()
        {
					NativePtr->advanceCursor(m_cursor);
          m_cursor = 0;
          m_bufferLength = 0;
        }

				void AdvanceCursorPdx(int offset)
        {
          m_cursor += offset;
        }

        void RewindCursorPdx(int rewind)
        {
          m_cursor = 0;
        }

        void ResetAndAdvanceCursorPdx(int offset)
        {
          m_cursor = offset;
        }

        void ResetPdx(int offset)
        {
          NativePtr->reset(offset);
          SetBuffer();
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
          //array<Char>^ output = gcnew array<Char>(length);
        
          if(m_forStringDecode->Length < length)
            m_forStringDecode = gcnew array<Char>(length);
          // index input[]
          int i = 0;
          // index output[]
          int j = 0;
          while ( i < length )
          {
            // get next byte unsigned
            //Byte b = m_buffer[ m_cursor++ ] & 0xff;
            Byte b = ReadByte();
            i++;
            Byte k = b >> 5;
            // classify based on the high order 3 bits
            switch (  k )
              {
              default:
                // one byte encoding
                // 0xxxxxxx
                // use just low order 7 bits
                // 00000000 0xxxxxxx
                m_forStringDecode[ j++ ] = ( Char ) ( b & 0x7f );
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
                  m_forStringDecode[ j++ ] = ( Char ) ( y << 6 | x );
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
                  m_forStringDecode[ j++ ] = ( Char ) asint;
                  break;
                }
              }// end switch
          }// end while

          String^ str = gcnew String(m_forStringDecode, 0, j);
          return str;
        }

        void CheckBufferSize(int size);
       

        Object^ ReadInternalGenericObject();

        Object^ ReadInternalObject();

        DataInput^ GetClone();

        /// <summary>
        /// Internal constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline DataInput( gemfire::DataInput* nativeptr, bool managedObject )
          : UMWrap(nativeptr, false)
        { 
          m_ispdxDesrialization = false;
          m_isRootObjectPdx = false;
          m_cursor = 0;
          m_isManagedObject = managedObject;
          m_forStringDecode = gcnew array<Char>(100);
          m_buffer = const_cast<uint8_t*>(nativeptr->currentBufferPosition());
          if ( m_buffer != NULL) {
            m_bufferLength = nativeptr->getBytesRemaining();     
					}
          else {
            m_bufferLength = 0;
          }
        }

        DataInput( uint8_t* buffer, int size );

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

        int GetPdxBytes()
        {
          return m_bufferLength;
        }

      private:

        /// <summary>
        /// Internal buffer managed by the class.
        /// This is freed in the disposer/destructor.
        /// </summary>
        bool m_ispdxDesrialization;
        bool m_isRootObjectPdx;
        uint8_t* m_buffer;
        unsigned int m_bufferLength;
        int m_cursor;
        bool m_isManagedObject;
        array<Char>^ m_forStringDecode;
      
        void Cleanup( );
      };
      } // end namespace generic
    }
  }
}
