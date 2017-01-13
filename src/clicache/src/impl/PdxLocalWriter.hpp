/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "../gf_defs.hpp"
#include "../IPdxWriter.hpp"
#include "PdxType.hpp"
#include "../GemFireClassIds.hpp"
#include "PdxRemotePreservedData.hpp"
using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Generic
      {
      ref class DataOutput;
      namespace Internal
      {
        ref class PdxLocalWriter : public IPdxWriter
        {
        protected:            
            PdxType^      m_pdxType;
            DataOutput^   m_dataOutput;
            uint8_t*      m_startPosition;
            Int32         m_startPositionOffset;
            String^         m_domainClassName;
            array<int>^   m_offsets;
            Int32         m_currentOffsetIndex;            
            PdxRemotePreservedData^ m_preserveData;
            //Type^       m_pdxDomainType;
            String^       m_pdxClassName;

        public:

          PdxLocalWriter(DataOutput^ dataOutput, PdxType^ pdxType)
          {
            m_dataOutput = dataOutput;
            m_pdxType = pdxType;
            m_currentOffsetIndex = 0;
            m_preserveData = nullptr;
            if(pdxType != nullptr)
              m_pdxClassName = pdxType->PdxClassName;
            //m_pdxDomainType = nullptr;
            initialize();
          }

          PdxLocalWriter(DataOutput^ dataOutput, PdxType^ pdxType, String^ pdxClassName)
          {
            m_dataOutput = dataOutput;
            m_pdxType = pdxType;
            m_currentOffsetIndex = 0;
            m_preserveData = nullptr;
           // m_pdxDomainType = pdxDomainType;
            m_pdxClassName = pdxClassName;
            initialize();
          }

          void initialize();          

          virtual void AddOffset();          

          virtual void EndObjectWriting(); 

          //this is used to get pdx stream when WriteablePdxStream udpadates the field
          //It should be called after pdx stream has been written to output
          uint8_t* GetPdxStream(int& pdxLen);

          void WritePdxHeader();

          virtual void WriteOffsets(Int32 len);

          virtual Int32 calculateLenWithOffsets();

          virtual bool isFieldWritingStarted();

          Int32 getStartPositionOffset()
          {
            return m_startPositionOffset;
          }
          
          /// <summary>
          /// Write a byte to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="value">The byte to write.</param>
          virtual IPdxWriter^ WriteByte( String^ fieldName, SByte value );

          /// <summary>
          /// Write a signed byte to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="value">The signed byte to write.</param>
          virtual IPdxWriter^ WriteSByte( String^ fieldName, SByte value );

          /// <summary>
          /// Write a boolean value to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="value">The boolean value to write.</param>
					virtual IPdxWriter^ WriteBoolean( String^ fieldName, Boolean value );

          /// <summary>
          /// Write a char value to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="value">The char value to write.</param>
          virtual IPdxWriter^ WriteChar( String^ fieldName, Char value );
                       
          /// <summary>
          /// Write an unsigned short integer (int16_t) to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="value">The unsigned 16-bit integer to write.</param>
          virtual IPdxWriter^ WriteUInt16( String^ fieldName, uint16_t value );

          /// <summary>
          /// Write an unsigned 32-bit integer to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="value">The unsigned 32-bit integer to write.</param>
          virtual IPdxWriter^ WriteUInt32( String^ fieldName, uint32_t value );

          /// <summary>
          /// Write an unsigned 64-bit integer to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="value">The unsigned 64-bit integer to write.</param>
          virtual IPdxWriter^ WriteUInt64( String^ fieldName, uint64_t value );

          /// <summary>
          /// Write a 16-bit integer to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="value">The 16-bit integer to write.</param>
          virtual IPdxWriter^ WriteShort( String^ fieldName, short value );

          /// <summary>
          /// Write a 32-bit integer to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="value">The 32-bit integer to write.</param>
					virtual IPdxWriter^ WriteInt( String^ fieldName, Int32 value );

          /// <summary>
          /// Write a 64-bit integer to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="value">The 64-bit integer to write.</param>
          virtual IPdxWriter^ WriteLong( String^ fieldName, Int64 value );

          /// <summary>
          /// Write a float to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="value">The float value to write.</param>
          virtual IPdxWriter^ WriteFloat( String^ fieldName, float value );

          /// <summary>
          /// Write a double precision real number to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="value">
          /// The double precision real number to write.
          /// </param>
          virtual IPdxWriter^ WriteDouble( String^ fieldName, double value );

          /// <summary>
          /// Write a string using java-modified UTF-8 encoding to
          /// <c>IPdxWriter</c>.
          /// The maximum length supported is 2^16-1 beyond which the string
          /// shall be truncated.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="value">The UTF encoded string to write.</param>
          virtual IPdxWriter^ WriteString( String^ fieldName, String^ value );

          /// <summary>
          /// Write a string using java-modified UTF-8 encoding to
          /// <c>IPdxWriter</c>.
          /// Length should be more than 2^16 -1. 
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="value">The UTF encoded string to write.</param>
          virtual IPdxWriter^ WriteUTFHuge( String^ fieldName, String^ value );

          /// <summary>
          /// Write a string(only ASCII char) to
          /// <c>IPdxWriter</c>.
          /// Length should be more than 2^16 -1.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="value">The UTF encoded string to write.</param>
          virtual IPdxWriter^ WriteASCIIHuge( String^ fieldName, String^ value );

          /// <summary>
          /// Write an <c>Object</c> object to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="obj">The object to write.</param>
          virtual IPdxWriter^ WriteObject( String^ fieldName, Object^ obj );

          //TODO:
          //virtual IPdxWriter^ WriteMap( String^ fieldName, System::Collections::IDictionary^ map );

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="collection">The collection to write.</param>
          virtual IPdxWriter^ WriteCollection( String^ fieldName, System::Collections::IList^ collection);

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="date">The date to write.</param>
          virtual IPdxWriter^ WriteDate( String^ fieldName, System::DateTime date);
          
          //TODO:
          //virtual IPdxWriter^ writeFile(String fieldName, File file) ;

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="boolArray">The boolArray to write.</param>
					virtual IPdxWriter^ WriteBooleanArray( String^ fieldName, array<Boolean>^ boolArray);

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="charArray">The charArray to write.</param>
          virtual IPdxWriter^ WriteCharArray(String^ fieldName, array<Char>^ charArray) ;

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="byteArray">The byteArray to write.</param>
					virtual IPdxWriter^ WriteByteArray(String^ fieldName, array<Byte>^ byteArray) ;

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="sbyteArray">The sbyteArray to write.</param>
          virtual IPdxWriter^ WriteSByteArray(String^ fieldName, array<SByte>^ sbyteArray);

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="shortArray">The shortArray to write.</param>
          virtual IPdxWriter^ WriteShortArray(String^ fieldName, array<short>^ shortArray);

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="ushortArray">The ushortArray to write.</param>
          virtual IPdxWriter^ WriteUnsignedShortArray(String^ fieldName, array<System::UInt16>^ ushortArray);

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="intArray">The intArray to write.</param>
          virtual IPdxWriter^ WriteIntArray(String^ fieldName, array<System::Int32>^ intArray);

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="uintArray">The uintArray to write.</param>
          virtual IPdxWriter^ WriteUnsignedIntArray(String^ fieldName, array<System::UInt32>^ uintArray);

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="longArray">The longArray to write.</param>
					virtual IPdxWriter^ WriteLongArray(String^ fieldName, array<Int64>^ longArray);

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="ulongArray">The ulongArray to write.</param>
          virtual IPdxWriter^ WriteUnsignedLongArray(String^ fieldName, array<System::UInt64>^ ulongArray);

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="floatArray">The floatArray to write.</param>
          virtual IPdxWriter^ WriteFloatArray(String^ fieldName, array<float>^ floatArray);

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="doubleArray">The doubleArray to write.</param>
          virtual IPdxWriter^ WriteDoubleArray(String^ fieldName, array<double>^ doubleArray);

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="stringArray">The stringArray to write.</param>
          virtual IPdxWriter^ WriteStringArray(String^ fieldName, array<String^>^ stringArray);

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="objectArray">The objectArray to write.</param>
          virtual IPdxWriter^ WriteObjectArray(String^ fieldName, List<Object^>^ objectArray);

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="byteArrays">The byteArrays to write.</param>
          virtual IPdxWriter^ WriteArrayOfByteArrays(String^ fieldName, array<array<Byte>^>^ byteArrays);
          
          //TODO:
          //virtual IPdxWriter^ WriteEnum(String^ fieldName, Enum e) ;
          //virtual IPdxWriter^ WriteInetAddress(String^ fieldName, InetAddress address);

          
          virtual IPdxWriter^ MarkIdentityField(String^ fieldName);

          virtual IPdxWriter^ WriteUnreadFields(IPdxUnreadFields^ unread);

           /// <summary>
					/// Writes the named field with the given value and type to the serialized form.
          /// This method uses the <code>fieldType</code> to determine which WriteXXX method it should call.
          /// If it can not find a specific match to a writeXXX method it will call <see cref="WriteObject(String^, Object^)">.
					/// 
					/// </summary>
					/// <returns>this PdxWriter</returns>
          virtual IPdxWriter^ WriteField(String^ fieldName, Object^ fieldValue, Type^ type);

          virtual void WriteByte(Byte byte);//for internal purpose
        };      
      }
			}
    }
  }
}