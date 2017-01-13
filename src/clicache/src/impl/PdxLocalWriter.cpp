/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "PdxLocalWriter.hpp"
#include "PdxHelper.hpp"
#include "PdxTypeRegistry.hpp"
#include "../DataOutput.hpp"
#include "DotNetTypes.hpp"
using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Generic
      {
      namespace Internal
      {
        
          
          void PdxLocalWriter::initialize()
          {
            if(m_pdxType != nullptr)
            {
              m_currentOffsetIndex = 0;
              //TODO:: 1 offset below if you can avoid
              m_offsets = gcnew array<int>(m_pdxType->NumberOfVarLenFields);
            }

            //start position, this should start of c++ dataoutput buffer and then use bufferlen
            m_startPosition = m_dataOutput->GetStartBufferPosition();
            //TODO: need to use this carefully
            m_startPositionOffset = m_dataOutput->BufferLength;//data has been write
            m_dataOutput->AdvanceCursor(PdxHelper::PdxHeader);//to write pdx header
          }

          void PdxLocalWriter::AddOffset()
          {
            //bufferlength gives lenght which has been writeen to unmanged Dataoutput
            //m_startPositionOffset: from where pdx header length starts
            int bufferLen = m_dataOutput->BufferLength - m_startPositionOffset;
            int offset = bufferLen - PdxHelper::PdxHeader/* this needs to subtract*/;

            m_offsets[m_currentOffsetIndex++] = offset;
          }

          void PdxLocalWriter::EndObjectWriting()
          {
            //write header
            WritePdxHeader();
          }

          uint8_t* PdxLocalWriter::GetPdxStream(int& pdxLen)
          {
            uint8_t* stPos = m_dataOutput->GetStartBufferPosition() + m_startPositionOffset;
            int len = PdxHelper::ReadInt32(stPos);
            pdxLen = len;
            //ignore len and typeid
            return m_dataOutput->GetBytes(stPos + 8, len );
          }


          void PdxLocalWriter::WritePdxHeader()
          {
            Int32 len = calculateLenWithOffsets();
            Int32 typeId = m_pdxType->TypeId;

            //GetStartBufferPosition ; if unmanaged dataoutput get change
            uint8_t* starpos = m_dataOutput->GetStartBufferPosition() + m_startPositionOffset;
            PdxHelper::WriteInt32(starpos , len);
            PdxHelper::WriteInt32(starpos + 4, typeId);

            WriteOffsets(len);
          }

          Int32 PdxLocalWriter::calculateLenWithOffsets()
          {
            //int bufferLen = m_dataOutput->GetCursorPdx();
            int bufferLen = m_dataOutput->BufferLength - m_startPositionOffset;
            Int32 totalOffsets = 0;
            if(m_offsets->Length > 0)
              totalOffsets = m_offsets->Length -1;//for first var len no need to append offset
            Int32 totalLen = bufferLen - PdxHelper::PdxHeader + totalOffsets;
            

            if(totalLen <= 0xff)//1 byte
              return totalLen;
            else if (totalLen + totalOffsets <= 0xffff)//2 byte
              return totalLen + totalOffsets ;
            else//4 byte
              return totalLen + totalOffsets*3 ;                       
          }

          void PdxLocalWriter::WriteOffsets(Int32 len)
          {
            if(len <= 0xff)
            {
              for(int i = m_offsets->Length-1; i >0 ; i--)
                m_dataOutput->WriteByte((Byte)m_offsets[i]);

            }
            else if(len <= 0xffff)
            {
              for(int i = m_offsets->Length-1; i >0 ; i--)
								m_dataOutput->WriteUInt16((UInt16)m_offsets[i]);
            }
            else
            {
              for(int i = m_offsets->Length-1; i >0 ; i--)
                m_dataOutput->WriteUInt32((UInt32)m_offsets[i]);
            }
          }

          bool PdxLocalWriter::isFieldWritingStarted()
          {
            return true;
          }

          IPdxWriter^ PdxLocalWriter::WriteUnreadFields(IPdxUnreadFields^ unread)
          {
            if(isFieldWritingStarted())
            {
              throw gcnew IllegalStateException("WriteUnreadFields must be called before "
																									+ "any other fields are written.");
            }
           
            if(unread != nullptr)
            {
              m_preserveData = (PdxRemotePreservedData^)unread;

              m_pdxType = PdxTypeRegistry::GetPdxType(m_preserveData->MergedTypeId);
              if(m_pdxType == nullptr)
              {//its local type
                //this needs to fix for IPdxTypemapper
                m_pdxType = PdxTypeRegistry::GetLocalPdxType(m_pdxClassName);
              }
              m_offsets = gcnew array<int>(m_pdxType->NumberOfVarLenFields);
            }
            return this;
          }

          IPdxWriter^ PdxLocalWriter::WriteByte( String^ fieldName, SByte value )
          {
						m_dataOutput->WriteSByte(value);
            return this;
          }

          void PdxLocalWriter::WriteByte(Byte byte)
          {
            m_dataOutput->WriteByte(byte);
          }

          IPdxWriter^ PdxLocalWriter::WriteSByte( String^ fieldName, SByte value )
          {
            m_dataOutput->WriteSByte(value);
            return this;
          }

          IPdxWriter^ PdxLocalWriter::WriteBoolean( String^ fieldName, bool value )
          {
            m_dataOutput->WriteBoolean(value);
            return this;
          }

          IPdxWriter^ PdxLocalWriter::WriteChar( String^ fieldName, Char value )
          {
            m_dataOutput->WriteChar(value);
            return this;
          }
                       
          IPdxWriter^ PdxLocalWriter::WriteUInt16( String^ fieldName, uint16_t value )
          {
            m_dataOutput->WriteUInt16(value);
            return this;
          }

          IPdxWriter^ PdxLocalWriter::WriteUInt32( String^ fieldName, uint32_t value )
          {
            m_dataOutput->WriteUInt32(value);
            return this;
          }

          IPdxWriter^ PdxLocalWriter::WriteUInt64( String^ fieldName, uint64_t value )
          {
            m_dataOutput->WriteUInt64(value);
            return this;
          }

          IPdxWriter^ PdxLocalWriter::WriteShort( String^ fieldName, int16_t value )
          {
            m_dataOutput->WriteInt16(value);
            return this;
          }

          IPdxWriter^ PdxLocalWriter::WriteInt( String^ fieldName, int32_t value )
          {
            m_dataOutput->WriteInt32(value);
            return this;
          }

          IPdxWriter^ PdxLocalWriter::WriteLong( String^ fieldName, Int64 value )
          {
						m_dataOutput->WriteInt64(value);
            return this;
          }

          IPdxWriter^ PdxLocalWriter::WriteFloat( String^ fieldName, float value )
          {
            m_dataOutput->WriteFloat(value);
            return this;
          }

          IPdxWriter^ PdxLocalWriter::WriteDouble( String^ fieldName, double value )
          {
            m_dataOutput->WriteDouble(value);
            return this;
          }

          IPdxWriter^ PdxLocalWriter::WriteString( String^ fieldName, String^ value )
          {
            AddOffset();
            m_dataOutput->WriteString(value);
            return this;
          }

          IPdxWriter^ PdxLocalWriter::WriteUTFHuge( String^ fieldName, String^ value )
          {
            AddOffset();
            m_dataOutput->WriteUTFHuge(value);
            return this;
          }

          IPdxWriter^ PdxLocalWriter::WriteASCIIHuge( String^ fieldName, String^ value )
          {
            AddOffset();
            m_dataOutput->WriteASCIIHuge(value);
            return this;
          }

          IPdxWriter^ PdxLocalWriter::WriteObject( String^ fieldName, Object^ obj )
          {
            AddOffset();
            m_dataOutput->WriteObject(obj);
            return this;
          }

          //TODO:
          //IPdxWriter^ PdxLocalWriter::WriteMap( String^ fieldName, System::Collections::IDictionary^ map );

          IPdxWriter^ PdxLocalWriter::WriteCollection( String^ fieldName, System::Collections::IList^ obj)
          {
            AddOffset();
            m_dataOutput->WriteCollection(obj);
            return this;
          }

          IPdxWriter^ PdxLocalWriter::WriteDate( String^ fieldName, System::DateTime date)
          {
            m_dataOutput->WriteDate(date);
            return this;
          }
          
          IPdxWriter^ PdxLocalWriter::WriteBooleanArray( String^ fieldName, array<bool>^ boolArray)
          {
            AddOffset();
            m_dataOutput->WriteBooleanArray(boolArray);
            return this;
          }

          IPdxWriter^ PdxLocalWriter::WriteCharArray(String^ fieldName, array<Char>^ charArray)
          {
            AddOffset();
            m_dataOutput->WriteCharArray(charArray);
            return this;
          }

          IPdxWriter^ PdxLocalWriter::WriteByteArray(String^ fieldName, array<Byte>^ byteArray)
          {
            AddOffset();
						m_dataOutput->WriteBytes(byteArray);
            return this;
          }

          IPdxWriter^ PdxLocalWriter::WriteSByteArray(String^ fieldName, array<SByte>^ sbyteArray)
          {
            AddOffset();
            m_dataOutput->WriteSBytes(sbyteArray);
            return this;
          }

          IPdxWriter^ PdxLocalWriter::WriteShortArray(String^ fieldName, array<System::Int16>^ shortArray)
          {
            AddOffset();
            m_dataOutput->WriteShortArray(shortArray);//TODO::this don't write typeid looks confusing
            return this;
          }

          IPdxWriter^ PdxLocalWriter::WriteUnsignedShortArray(String^ fieldName, array<System::UInt16>^ ushortArray)
          {
            AddOffset();
           // m_dataOutput->WriteObject(ushortArray);
            return this;
          }

          IPdxWriter^ PdxLocalWriter::WriteIntArray(String^ fieldName, array<System::Int32>^ intArray)
          {
            AddOffset();
            m_dataOutput->WriteIntArray(intArray);
            return this;
          }

          IPdxWriter^ PdxLocalWriter::WriteUnsignedIntArray(String^ fieldName, array<System::UInt32>^ uintArray)
          {
            AddOffset();
           // m_dataOutput->WriteObject(uintArray);
            return this;
          }
					
          IPdxWriter^ PdxLocalWriter::WriteLongArray(String^ fieldName, array<Int64>^ longArray)
          {
            AddOffset();
            m_dataOutput->WriteLongArray(longArray);
            return this;
          }

          IPdxWriter^ PdxLocalWriter::WriteUnsignedLongArray(String^ fieldName, array<System::UInt64>^ ulongArray)
          {
            AddOffset();
            //m_dataOutput->WriteObject(ulongArray);
            return this;
          }

          IPdxWriter^ PdxLocalWriter::WriteFloatArray(String^ fieldName, array<float>^ floatArray)
          {
            AddOffset();
            m_dataOutput->WriteFloatArray(floatArray);
            return this;
          }

          IPdxWriter^ PdxLocalWriter::WriteDoubleArray(String^ fieldName, array<double>^ doubleArray)
          {
            AddOffset();
            m_dataOutput->WriteDoubleArray(doubleArray);
            return this;
          }

          IPdxWriter^ PdxLocalWriter::WriteStringArray(String^ fieldName, array<String^>^ stringArray)
          {
            AddOffset();
            m_dataOutput->WriteStringArray(stringArray);
            return this;
          }

          IPdxWriter^ PdxLocalWriter::WriteObjectArray(String^ fieldName, List<Object^>^ objectArray)
          {
            AddOffset();
            m_dataOutput->WriteObjectArray(objectArray);
            return this;
          }

          IPdxWriter^ PdxLocalWriter::WriteArrayOfByteArrays(String^ fieldName, array<array<Byte>^>^ byteArrays)
          {
            AddOffset();
            m_dataOutput->WriteArrayOfByteArrays(byteArrays);
            return this;
          }
          
          //TODO:
          //IPdxWriter^ PdxLocalWriter::WriteEnum(String^ fieldName, Enum e) ;
          //IPdxWriter^ PdxLocalWriter::WriteInetAddress(String^ fieldName, InetAddress address);


          IPdxWriter^ PdxLocalWriter::MarkIdentityField(String^ fieldName)
          {
            return this;
          }

          IPdxWriter^ PdxLocalWriter::WriteField(String^ fieldName, Object^ fieldValue, Type^ type)
          {
            if(type->Equals(DotNetTypes::IntType))
            {
              return this->WriteInt(fieldName, (int)fieldValue);
            }
            else if(type->Equals(DotNetTypes::StringType))
            {
              return this->WriteString(fieldName, (String^)fieldValue);
            }
            else if(type->Equals(DotNetTypes::BooleanType))
            {
              return this->WriteBoolean(fieldName, (bool)fieldValue);
            }
            else if(type->Equals(DotNetTypes::FloatType))
            {
              return this->WriteFloat(fieldName, (float)fieldValue);
            }
            else if(type->Equals(DotNetTypes::DoubleType))
            {
              return this->WriteDouble(fieldName, (double)fieldValue);
            }
            else if(type->Equals(DotNetTypes::CharType))
            {
              return this->WriteChar(fieldName, (Char)fieldValue);
            }
            else if(type->Equals(DotNetTypes::SByteType))
            {
              return this->WriteByte(fieldName, (SByte)fieldValue);
            }
            else if(type->Equals(DotNetTypes::ShortType))
            {
              return this->WriteShort(fieldName, (short)fieldValue);
            }
            else if(type->Equals(DotNetTypes::LongType))
            {
              return this->WriteLong(fieldName, (Int64)fieldValue);
            }
            else if(type->Equals(DotNetTypes::ByteArrayType))
            {
              return this->WriteByteArray(fieldName, (array<Byte>^)fieldValue);
            }
            else if(type->Equals(DotNetTypes::DoubleArrayType))
            {
              return this->WriteDoubleArray(fieldName, (array<double>^)fieldValue);
            }
            else if(type->Equals(DotNetTypes::FloatArrayType))
            {
              return this->WriteFloatArray(fieldName, (array<float>^)fieldValue);
            }
            else if(type->Equals(DotNetTypes::ShortArrayType))
            {
              return this->WriteShortArray(fieldName, (array<Int16>^)fieldValue);
            }
            else if(type->Equals(DotNetTypes::IntArrayType))
            {
              return this->WriteIntArray(fieldName, (array<int32>^)fieldValue);
            }
            else if(type->Equals(DotNetTypes::LongArrayType))
            {
              return this->WriteLongArray(fieldName, (array<Int64>^)fieldValue);
            }
            else if(type->Equals(DotNetTypes::BoolArrayType))
            {
              return this->WriteBooleanArray(fieldName, (array<bool>^)fieldValue);
            }
            else if(type->Equals(DotNetTypes::CharArrayType))
            {
              return this->WriteCharArray(fieldName, (array<Char>^)fieldValue);
            }
            else if(type->Equals(DotNetTypes::StringArrayType))
            {
              return this->WriteStringArray(fieldName, (array<String^>^)fieldValue);
            }
            else if(type->Equals(DotNetTypes::DateType))
            {
              return this->WriteDate(fieldName, (DateTime)fieldValue);
            }
            else if(type->Equals(DotNetTypes::ByteArrayOfArrayType))
            {
              return this->WriteArrayOfByteArrays(fieldName, (array<array<Byte>^>^)fieldValue);
            }
            else if(type->Equals(DotNetTypes::ObjectArrayType))
            {
              return this->WriteObjectArray(fieldName, safe_cast<System::Collections::Generic::List<Object^>^>(fieldValue));
            }
            else
            {
              return this->WriteObject(fieldName, fieldValue);
              //throw gcnew IllegalStateException("WriteField unable to serialize  " 
								//																	+ fieldName + " of " + type); 
            }
          }
			  }
      }
    }
  }
}