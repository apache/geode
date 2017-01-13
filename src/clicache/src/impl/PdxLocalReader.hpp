/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "../gf_defs.hpp"
#include "../IPdxReader.hpp"
#include "PdxType.hpp"
//#include "../DataInput.hpp"
#include "PdxRemotePreservedData.hpp"
#include "../IPdxSerializable.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Generic
      {
      ref class DataInput;
      namespace Internal
      {
        ref class PdxLocalReader: public IPdxReader
        {

        protected:

          DataInput^        m_dataInput;
          PdxType^          m_pdxType;
          uint8_t*          m_startBuffer;
          Int32             m_startPosition;
          Int32             m_serializedLength;
          Int32             m_serializedLengthWithOffsets;
          Int32             m_offsetSize;
          uint8_t*          m_offsetsBuffer;
          bool              m_isDataNeedToPreserve;
          
          PdxRemotePreservedData^ m_pdxRemotePreserveData;

          array<Int32>^ m_localToRemoteMap;
          array<Int32>^ m_remoteToLocalMap;

          void initialize();
        public:

          PdxLocalReader(DataInput^ dataInput, PdxType^ pdxType, int pdxLen)
          {
            m_dataInput = dataInput;
            m_pdxType = pdxType;
            m_serializedLengthWithOffsets = pdxLen;

            m_localToRemoteMap = pdxType->GetLocalToRemoteMap();
            m_remoteToLocalMap = pdxType->GetRemoteToLocalMap();
            m_pdxRemotePreserveData = gcnew PdxRemotePreservedData();
            m_isDataNeedToPreserve = true;
            initialize();
          }

          PdxRemotePreservedData^ GetPreservedData(PdxType^ mergedVersion, IPdxSerializable^ object);

          void MoveStream();

        /// <summary>
        /// Read a byte from the stream.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
			 virtual SByte ReadByte( String^ fieldName );

        /// <summary>
        /// Read a signed byte from the stream.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual SByte ReadSByte( String^ fieldName );

        /// <summary>
        /// Read a boolean value from the stream.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
				virtual Boolean ReadBoolean( String^ fieldName );

        /// <summary>
        /// Read a char value from the stream.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual Char ReadChar( String^ fieldName );
        
        /// <summary>
        /// Read a 16-bit unsigned integer from the stream.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual uint16_t ReadUInt16( String^ fieldName );

        /// <summary>
        /// Read a 32-bit unsigned integer from the stream.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual uint32_t ReadUInt32( String^ fieldName );
        
        /// <summary>
        /// Read a 64-bit unsigned integer from the stream.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual uint64_t ReadUInt64( String^ fieldName );

        /// <summary>
        /// Read a 16-bit integer from the stream.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual short ReadShort( String^ fieldName );

        /// <summary>
        /// Read a 32-bit integer from the stream.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
				virtual Int32 ReadInt( String^ fieldName );

        /// <summary>
        /// Read a 64-bit integer from the stream.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual Int64 ReadLong( String^ fieldName );

        /// <summary>
        /// Read a floating point number from the stream.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual float ReadFloat( String^ fieldName );

        /// <summary>
        /// Read a double precision number from the stream.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual double ReadDouble( String^ fieldName );

        /// <summary>
        /// Read a string after java-modified UTF-8 decoding from the stream.
        /// The maximum length supported is 2^16-1 beyond which the string
        /// shall be truncated.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual String^ ReadString( String^ fieldName );

        /// <summary>
        /// Read a string after java-modified UTF-8 decoding from the stream.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual String^ ReadUTFHuge( String^ fieldName );

        /// <summary>
        /// Read a ASCII string from the stream. Where size is more than 2^16-1 
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual String^ ReadASCIIHuge( String^ fieldName );

        /// <summary>
        /// Read a serializable object from the data. Null objects are handled.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual Object^ ReadObject( String^ fieldName );
        
        //TODO:
        //virtual void WriteMap( String^ fieldName, System::Collections::IDictionary^ map );

        /// <summary>
        /// Read a collection from the data.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual void ReadCollection( String^ fieldName, System::Collections::IList^ collection);

        /// <summary>
        /// Read a Date from the data. 
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual System::DateTime ReadDate( String^ fieldName) ;
        //virtual void writeFile(String fieldName, File file) ;

        /// <summary>
        /// Read a boolean array from the data. 
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
				virtual array<Boolean>^ ReadBooleanArray( String^ fieldName );

        /// <summary>
        /// Read a char array from the data.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual array<Char>^ ReadCharArray(String^ fieldName );

        /// <summary>
        /// Read a byte array from the data.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual array<Byte>^ ReadByteArray(String^ fieldName);

        /// <summary>
        /// Read a sbyte array from the data.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual array<SByte>^ ReadSByteArray(String^ fieldName);

        /// <summary>
        /// Read a short from the data.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual array<short>^ ReadShortArray(String^ fieldName);

        /// <summary>
        /// Read a ushort array from the data.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual array<System::UInt16>^ ReadUnsignedShortArray(String^ fieldName);

        /// <summary>
        /// Read a int array from the data.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual array<System::Int32>^ ReadIntArray(String^ fieldName);

        /// <summary>
        /// Read a uint from the data.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual array<System::UInt32>^ ReadUnsignedIntArray(String^ fieldName);

        /// <summary>
        /// Read a long array from the data.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
				virtual array<Int64>^ ReadLongArray(String^ fieldName);

        /// <summary>
        /// Read a ulong array from the data.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual array<System::UInt64>^ ReadUnsignedLongArray(String^ fieldName );

        /// <summary>
        /// Read a float from the data.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual array<float>^ ReadFloatArray(String^ fieldName);

        /// <summary>
        /// Read a double array from the data.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual array<double>^ ReadDoubleArray(String^ fieldName);

        /// <summary>
        /// Read a string array from the data.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual array<String^>^ ReadStringArray(String^ fieldName);

        /// <summary>
        /// Read a object array from the data.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual List<Object^>^ ReadObjectArray(String^ fieldName);

        /// <summary>
        /// Read a two-dimenesional byte array from the data.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual array<array<Byte>^>^ ReadArrayOfByteArrays(String^ fieldName );

        //TODO:
        //virtual void WriteEnum(String^ fieldName, Enum e) ;
        //virtual void WriteInetAddress(String^ fieldName, InetAddress address);

        
         /// <summary>
        /// Whether field is available or not.
        /// </summary>
        /// <param name="fieldName">The name of a member field.</param>
        virtual bool HasField(String^ fieldName);
  
        /// <summary>
        /// Whether field is used as identity field or not.
        /// </summary>
        /// <param name="fieldName">The name of a member field.</param>
        virtual bool IsIdentityField(String^ fieldName);

        virtual IPdxUnreadFields^ ReadUnreadFields();
     
          /// <summary>
          /// Reads the named field and returns its value.
          /// </summary>
          virtual Object^ ReadField(String^ fieldName, Type^ type);
        };
      }
			}
    }
  }
}