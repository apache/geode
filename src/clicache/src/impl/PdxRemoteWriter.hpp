/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "../gf_defs.hpp"
#include "PdxLocalWriter.hpp"

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
        ref class PdxRemoteWriter: public PdxLocalWriter
        {
        private:
          array<int>^             m_remoteTolocalMap;          
          Int32                   m_preserveDataIdx;
          Int32                   m_currentDataIdx;
          
          void initialize();
          void writePreserveData();          

        public:
          PdxRemoteWriter(DataOutput^ dataOutput,PdxType^ pdxType, PdxRemotePreservedData^ preservedData)
            : PdxLocalWriter(dataOutput, pdxType)
          {
            m_pdxType = pdxType;
            m_preserveData = preservedData;
            m_preserveDataIdx = 0;
            m_remoteTolocalMap = m_pdxType->GetRemoteToLocalMap();
            m_currentDataIdx = -1;
            m_pdxClassName = pdxType->PdxClassName;
            initialize();
          }

          PdxRemoteWriter(DataOutput^ dataOutput, String^ pdxClassName)
            : PdxLocalWriter(dataOutput , nullptr, pdxClassName)
          {
            m_pdxType = nullptr;
            m_preserveData = nullptr;
            m_preserveDataIdx = 0;
            //m_remoteTolocalMap = m_pdxType->GetRemoteToLocalMap();
            m_currentDataIdx = -1;
            m_pdxClassName = pdxClassName;
            initialize();
          }

          //i think on need to ovverride this local should work
          virtual void EndObjectWriting() override
          {
            writePreserveData();
            //write header 
            PdxLocalWriter::WritePdxHeader();
          }

          virtual bool isFieldWritingStarted() override;
        
          /// <summary>
          /// Write a byte to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="value">The byte to write.</param>
          virtual IPdxWriter^ WriteByte( String^ fieldName, SByte value ) override;

          /// <summary>
          /// Write a signed byte to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="value">The signed byte to write.</param>
          virtual IPdxWriter^ WriteSByte( String^ fieldName, SByte value ) override;

          /// <summary>
          /// Write a boolean value to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="value">The boolean value to write.</param>
          virtual IPdxWriter^ WriteBoolean( String^ fieldName, bool value ) override;

          /// <summary>
          /// Write a char value to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="value">The char value to write.</param>
          virtual IPdxWriter^ WriteChar( String^ fieldName, Char value ) override;

          /// <summary>
          /// Write an unsigned short integer (int16_t) to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="value">The unsigned 16-bit integer to write.</param>
          virtual IPdxWriter^ WriteUInt16( String^ fieldName, uint16_t value ) override;

          /// <summary>
          /// Write an unsigned 32-bit integer to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="value">The unsigned 32-bit integer to write.</param>
          virtual IPdxWriter^ WriteUInt32( String^ fieldName, uint32_t value ) override;

          /// <summary>
          /// Write an unsigned 64-bit integer to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="value">The unsigned 64-bit integer to write.</param>
          virtual IPdxWriter^ WriteUInt64( String^ fieldName, uint64_t value ) override;

          /// <summary>
          /// Write a 16-bit integer to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="value">The 16-bit integer to write.</param>
          virtual IPdxWriter^ WriteShort( String^ fieldName, short value ) override;

          /// <summary>
          /// Write a 32-bit integer to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="value">The 32-bit integer to write.</param>
          virtual IPdxWriter^ WriteInt( String^ fieldName, int32_t value ) override;

          /// <summary>
          /// Write a 64-bit integer to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="value">The 64-bit integer to write.</param>
          virtual IPdxWriter^ WriteLong( String^ fieldName, Int64 value ) override;

          /// <summary>
          /// Write a float to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="value">The float value to write.</param>
          virtual IPdxWriter^ WriteFloat( String^ fieldName, float value ) override;

          /// <summary>
          /// Write a double precision real number to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="value">
          /// The double precision real number to write.
          /// </param>
          virtual IPdxWriter^ WriteDouble( String^ fieldName, double value ) override;

          /// <summary>
          /// Write a string using java-modified UTF-8 encoding to
          /// <c>IPdxWriter</c>.
          /// The maximum length supported is 2^16-1 beyond which the string
          /// shall be truncated.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="value">The UTF encoded string to write.</param>
          virtual IPdxWriter^ WriteString( String^ fieldName, String^ value ) override;

          /// <summary>
          /// Write a string using java-modified UTF-8 encoding to
          /// <c>IPdxWriter</c>.
          /// Length should be more than 2^16 -1. 
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="value">The UTF encoded string to write.</param>
          virtual IPdxWriter^ WriteUTFHuge( String^ fieldName, String^ value ) override;

          /// <summary>
          /// Write a string(only ASCII char) to
          /// <c>IPdxWriter</c>.
          /// Length should be more than 2^16 -1.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="value">The UTF encoded string to write.</param>
          virtual IPdxWriter^ WriteASCIIHuge( String^ fieldName, String^ value ) override;

          /// <summary>
          /// Write an <c>Object</c> object to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="obj">The object to write.</param>
          virtual IPdxWriter^ WriteObject( String^ fieldName, Object^ obj ) override;

          //TODO:
          //virtual IPdxWriter^ WriteMap( String^ fieldName, System::Collections::IDictionary^ map );

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="collection">The collection to write.</param>
          virtual IPdxWriter^ WriteCollection( String^ fieldName, System::Collections::IList^ collection) override;

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="date">The date to write.</param>
          virtual IPdxWriter^ WriteDate( String^ fieldName, System::DateTime date) override;
          
          //TODO:
          //virtual IPdxWriter^ writeFile(String fieldName, File file) ;

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="boolArray">The boolArray to write.</param>
					virtual IPdxWriter^ WriteBooleanArray( String^ fieldName, array<Boolean>^ boolArray) override;

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="charArray">The charArray to write.</param>
          virtual IPdxWriter^ WriteCharArray(String^ fieldName, array<Char>^ charArray)  override;

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="byteArray">The byteArray to write.</param>
          virtual IPdxWriter^ WriteByteArray(String^ fieldName, array<Byte>^ byteArray)  override;

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="sbyteArray">The sbyteArray to write.</param>
          virtual IPdxWriter^ WriteSByteArray(String^ fieldName, array<SByte>^ sbyteArray) override;

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="shortArray">The shortArray to write.</param>
          virtual IPdxWriter^ WriteShortArray(String^ fieldName, array<short>^ shortArray) override;

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="ushortArray">The ushortArray to write.</param>
          virtual IPdxWriter^ WriteUnsignedShortArray(String^ fieldName, array<System::UInt16>^ ushortArray) override;

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="intArray">The intArray to write.</param>
          virtual IPdxWriter^ WriteIntArray(String^ fieldName, array<System::Int32>^ intArray) override;

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="uintArray">The uintArray to write.</param>
          virtual IPdxWriter^ WriteUnsignedIntArray(String^ fieldName, array<System::UInt32>^ uintArray) override;

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="longArray">The longArray to write.</param>
          virtual IPdxWriter^ WriteLongArray(String^ fieldName, array<Int64>^ longArray) override;

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="ulongArray">The ulongArray to write.</param>
          virtual IPdxWriter^ WriteUnsignedLongArray(String^ fieldName, array<System::UInt64>^ ulongArray) override;

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="floatArray">The floatArray to write.</param>
          virtual IPdxWriter^ WriteFloatArray(String^ fieldName, array<float>^ floatArray) override;

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="doubleArray">The doubleArray to write.</param>
          virtual IPdxWriter^ WriteDoubleArray(String^ fieldName, array<double>^ doubleArray) override;

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="stringArray">The stringArray to write.</param>
          virtual IPdxWriter^ WriteStringArray(String^ fieldName, array<String^>^ stringArray) override;

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="objectArray">The objectArray to write.</param>
          virtual IPdxWriter^ WriteObjectArray(String^ fieldName, List<Object^>^ objectArray) override;

          /// <summary>
          /// Write an collection to the <c>IPdxWriter</c>.
          /// </summary>
          /// <param name="fieldName">The name of the field associated with the value.</param>
          /// <param name="byteArrays">The byteArrays to write.</param>
          virtual IPdxWriter^ WriteArrayOfByteArrays(String^ fieldName, array<array<Byte>^>^ byteArrays) override;
          
          //TODO:
          //virtual IPdxWriter^ WriteEnum(String^ fieldName, Enum e) ;
          //virtual IPdxWriter^ WriteInetAddress(String^ fieldName, InetAddress address);

          
          virtual IPdxWriter^ WriteUnreadFields(IPdxUnreadFields^ unread) override;

        };      
      }
			}
    }
  }
}