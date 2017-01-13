/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "../gf_defs.hpp"
#include "PdxLocalReader.hpp"

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
        ref class PdxReaderWithTypeCollector: public PdxLocalReader
        {
        private:
          PdxType^      m_newPdxType;
        
					void checkType( String^ fieldName, Byte typeId, String^ fieldType);
        public:

          PdxReaderWithTypeCollector(DataInput^ dataInput, PdxType^ pdxType, int pdxlen)
            :PdxLocalReader(dataInput, pdxType, pdxlen)
          {
            m_newPdxType = gcnew PdxType(pdxType->PdxClassName, true);
          }

          property PdxType^ LocalType
          {
            PdxType^ get() {return m_newPdxType;}
          }

        /// <summary>
        /// Read a signed byte from the stream.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual SByte ReadByte( String^ fieldName )override;

        /// <summary>
        /// Read a signed byte from the stream.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual SByte ReadSByte( String^ fieldName )override;

        /// <summary>
        /// Read a boolean value from the stream.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual bool ReadBoolean( String^ fieldName )override;
        
        /// <summary>
        /// Read a char value from the stream.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual Char ReadChar( String^ fieldName )override;

        /// <summary>
        /// Read a 16-bit unsigned integer from the stream.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual uint16_t ReadUInt16( String^ fieldName )override;

        /// <summary>
        /// Read a 32-bit unsigned integer from the stream.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual uint32_t ReadUInt32( String^ fieldName )override;
        
        /// <summary>
        /// Read a 64-bit unsigned integer from the stream.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual uint64_t ReadUInt64( String^ fieldName )override;

        /// <summary>
        /// Read a 16-bit integer from the stream.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual short ReadShort( String^ fieldName )override;

        /// <summary>
        /// Read a 32-bit integer from the stream.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual int32_t ReadInt( String^ fieldName )override;

        /// <summary>
        /// Read a 64-bit integer from the stream.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual Int64 ReadLong( String^ fieldName )override;

        /// <summary>
        /// Read a floating point number from the stream.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual float ReadFloat( String^ fieldName )override;

        /// <summary>
        /// Read a double precision number from the stream.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual double ReadDouble( String^ fieldName )override;

        /// <summary>
        /// Read a string after java-modified UTF-8 decoding from the stream.
        /// The maximum length supported is 2^16-1 beyond which the string
        /// shall be truncated.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual String^ ReadString( String^ fieldName )override;

        /// <summary>
        /// Read a string after java-modified UTF-8 decoding from the stream.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual String^ ReadUTFHuge( String^ fieldName )override;

        /// <summary>
        /// Read a ASCII string from the stream. Where size is more than 2^16-1 
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual String^ ReadASCIIHuge( String^ fieldName )override;

        /// <summary>
        /// Read a serializable object from the data. Null objects are handled.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual Object^ ReadObject( String^ fieldName )override;
        
        //TODO:
        //virtual void WriteMap( String^ fieldName, System::Collections::IDictionary^ map );

        /// <summary>
        /// Read a collection from the data.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual void ReadCollection( String^ fieldName, System::Collections::IList^ collection)override;

        /// <summary>
        /// Read a Date from the data. 
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual System::DateTime ReadDate( String^ fieldName) override;
        //virtual void writeFile(String fieldName, File file) ;

        /// <summary>
        /// Read a boolean array from the data. 
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
				virtual array<Boolean>^ ReadBooleanArray( String^ fieldName )override;

        /// <summary>
        /// Read a char array from the data.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual array<Char>^ ReadCharArray(String^ fieldName )override;

        /// <summary>
        /// Read a byte array from the data.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual array<Byte>^ ReadByteArray(String^ fieldName)override;

        /// <summary>
        /// Read a sbyte array from the data.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual array<SByte>^ ReadSByteArray(String^ fieldName)override;

        /// <summary>
        /// Read a short from the data.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual array<short>^ ReadShortArray(String^ fieldName)override;

        /// <summary>
        /// Read a ushort array from the data.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual array<System::UInt16>^ ReadUnsignedShortArray(String^ fieldName)override;

        /// <summary>
        /// Read a int array from the data.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual array<System::Int32>^ ReadIntArray(String^ fieldName)override;

        /// <summary>
        /// Read a uint from the data.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual array<System::UInt32>^ ReadUnsignedIntArray(String^ fieldName)override;

        /// <summary>
        /// Read a long array from the data.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual array<Int64>^ ReadLongArray(String^ fieldName)override;

        /// <summary>
        /// Read a ulong array from the data.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual array<System::UInt64>^ ReadUnsignedLongArray(String^ fieldName )override;

        /// <summary>
        /// Read a float from the data.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual array<float>^ ReadFloatArray(String^ fieldName)override;

        /// <summary>
        /// Read a double array from the data.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual array<double>^ ReadDoubleArray(String^ fieldName)override;

        /// <summary>
        /// Read a string array from the data.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual array<String^>^ ReadStringArray(String^ fieldName)override;

        /// <summary>
        /// Read a object array from the data.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual List<Object^>^ ReadObjectArray(String^ fieldName)override;

        /// <summary>
        /// Read a two-dimenesional byte array from the data.
        /// </summary>
        /// <param name="fieldName">The name of a member field whose value to read.</param>
        virtual array<array<Byte>^>^ ReadArrayOfByteArrays(String^ fieldName )override;

        //TODO:
        //virtual void WriteEnum(String^ fieldName, Enum e) ;
        //virtual void WriteInetAddress(String^ fieldName, InetAddress address);

        
        };
      }
			}
    }
  }
}