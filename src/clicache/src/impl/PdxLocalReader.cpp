/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "PdxLocalReader.hpp"
#include "PdxTypeRegistry.hpp"
#include "../DataInput.hpp"
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
        void PdxLocalReader::initialize()
        {
          //pdx header already read before this
          m_startBuffer = m_dataInput->GetCursor();
          m_startPosition = m_dataInput->BytesRead;//number of bytes read in c++;

          //m_serializedLengthWithOffsets = PdxHelper::ReadInt32(m_startBuffer);

          if(m_serializedLengthWithOffsets <= 0xff)
            m_offsetSize = 1;
          else if(m_serializedLengthWithOffsets <= 0xffff)
            m_offsetSize = 2;
          else
            m_offsetSize = 4;

          if(m_pdxType->NumberOfVarLenFields > 0)
            m_serializedLength = m_serializedLengthWithOffsets - ((m_pdxType->NumberOfVarLenFields -1) * m_offsetSize);
          else
            m_serializedLength = m_serializedLengthWithOffsets; 

          m_offsetsBuffer = m_startBuffer + m_serializedLength;
        }

        void PdxLocalReader::MoveStream()
        {
          //this will reset unmaged datainput as well
          m_dataInput->ResetPdx(m_startPosition + m_serializedLengthWithOffsets);
        }

        PdxRemotePreservedData^ PdxLocalReader::GetPreservedData(PdxType^ mergedVersion, IPdxSerializable^ pdxObject)
        {
          int nFieldExtra = m_pdxType->NumberOfFieldsExtra;
          if(nFieldExtra > 0 && PdxTypeRegistry::PdxIgnoreUnreadFields == false)
          {
            //m_pdxRemotePreserveData = gcnew PdxRemotePreservedData(m_pdxType!=nullptr? m_pdxType->TypeId : 0, mergedVersion->TypeId,nFieldExtra);
						m_pdxRemotePreserveData->Initialize(m_pdxType!=nullptr? m_pdxType->TypeId : 0, mergedVersion->TypeId,nFieldExtra,pdxObject);

            m_localToRemoteMap = m_pdxType->GetLocalToRemoteMap();
            m_remoteToLocalMap = m_pdxType->GetRemoteToLocalMap();

            int currentIdx = 0; 
            for(int i = 0; i < m_remoteToLocalMap->Length; i++)
            {
              if(m_remoteToLocalMap[i] == -1 || m_remoteToLocalMap[i] == -2)//this field needs to preserve
              {
                int pos = m_pdxType->GetFieldPosition(i, m_offsetsBuffer, m_offsetSize, m_serializedLength);
                int nFieldPos = 0;

                if( i == m_remoteToLocalMap->Length - 1)
                {
                  nFieldPos = m_serializedLength;
                }
                else
                {
                  nFieldPos = m_pdxType->GetFieldPosition(i + 1, m_offsetsBuffer, m_offsetSize, m_serializedLength);
                }

                m_dataInput->ResetAndAdvanceCursorPdx(pos);
                m_pdxRemotePreserveData[currentIdx++] = m_dataInput->ReadBytesOnly(nFieldPos - pos);
                m_dataInput->RewindCursorPdx(pos);
              }
            }
						//Log::Debug("PdxLocalReader::GetPreservedData:: " + m_isDataNeedToPreserve);
            if(m_isDataNeedToPreserve)
              return m_pdxRemotePreserveData;
            else
            {
              //sPdxTypeRegistry::SetPreserveData(pdxObject, m_pdxRemotePreserveData);
            }
          }
          return nullptr;
        }

				IPdxUnreadFields^ PdxLocalReader::ReadUnreadFields()
        {
					//Log::Debug("PdxLocalReader::ReadUnreadFields:: " + m_isDataNeedToPreserve + " ignore property " + PdxTypeRegistry::PdxIgnoreUnreadFields);
					if(PdxTypeRegistry::PdxIgnoreUnreadFields == true)
						return nullptr;
          m_isDataNeedToPreserve = false;
          return m_pdxRemotePreserveData;
        }

        SByte PdxLocalReader::ReadByte( String^ fieldName )
        {
					return m_dataInput->ReadSByte();
        }

        SByte PdxLocalReader::ReadSByte( String^ fieldName )
        {
          return m_dataInput->ReadSByte();
        }

				Boolean PdxLocalReader::ReadBoolean( String^ fieldName )
        {
          return m_dataInput->ReadBoolean();
        }

        Char PdxLocalReader::ReadChar( String^ fieldName )
        {
          return m_dataInput->ReadChar();
        }
        
        uint16_t PdxLocalReader::ReadUInt16( String^ fieldName )
        {
          return m_dataInput->ReadUInt16();
        }

        uint32_t PdxLocalReader::ReadUInt32( String^ fieldName )
        {
          return m_dataInput->ReadUInt32();
        }
        
        uint64_t PdxLocalReader::ReadUInt64( String^ fieldName )
        {
          return m_dataInput->ReadUInt64();
        }

        short PdxLocalReader::ReadShort( String^ fieldName )
        {
          return m_dataInput->ReadInt16();
        }

				Int32 PdxLocalReader::ReadInt( String^ fieldName )
        {
          return m_dataInput->ReadInt32();
        }

        Int64 PdxLocalReader::ReadLong( String^ fieldName )
        {
          return m_dataInput->ReadInt64();
        }

        float PdxLocalReader::ReadFloat( String^ fieldName )
        {
          return m_dataInput->ReadFloat();
        }

        double PdxLocalReader::ReadDouble( String^ fieldName )
        {
          return m_dataInput->ReadDouble();
        }

        String^ PdxLocalReader::ReadString( String^ fieldName )
        {
          return m_dataInput->ReadString();
        }

        String^ PdxLocalReader::ReadUTFHuge( String^ fieldName )
        {
          return m_dataInput->ReadUTFHuge();
        }

        String^ PdxLocalReader::ReadASCIIHuge( String^ fieldName )
        {
          return m_dataInput->ReadASCIIHuge();
        }

        Object^ PdxLocalReader::ReadObject( String^ fieldName )
        {
          return m_dataInput->ReadObject();
        }
        
        //TODO:
        //void WriteMap( String^ fieldName, System::Collections::IDictionary^ map );

        void PdxLocalReader::ReadCollection( String^ fieldName, System::Collections::IList^ collection)
        {
          return m_dataInput->ReadCollection(collection);
        }

        System::DateTime PdxLocalReader::ReadDate( String^ fieldName)
        {
          return m_dataInput->ReadDate();
        }
        //void writeFile(String fieldName, File file) ;

        array<bool>^ PdxLocalReader::ReadBooleanArray( String^ fieldName )
        {
          //array<bool>^ arr;
         // m_dataInput->ReadObject(arr);
          return m_dataInput->ReadBooleanArray();
        }

        array<Char>^ PdxLocalReader::ReadCharArray(String^ fieldName )
        {
          //array<Char>^ arr;
          //m_dataInput->ReadObject(arr);
          return m_dataInput->ReadCharArray();
        }

				array<Byte>^ PdxLocalReader::ReadByteArray(String^ fieldName)
        {
					array<Byte>^ arr;
          m_dataInput->ReadObject(arr);
          return arr;
        }

        array<SByte>^ PdxLocalReader::ReadSByteArray(String^ fieldName)
        {
          //array<SByte>^ arr;
          //m_dataInput->ReadObject(arr);
          return m_dataInput->ReadSBytes();
        }

        array<System::Int16>^ PdxLocalReader::ReadShortArray(String^ fieldName)
        {
          //array<Int16>^ arr;
          //m_dataInput->ReadObject(arr);
          return m_dataInput->ReadShortArray();
        }

        array<System::UInt16>^ PdxLocalReader::ReadUnsignedShortArray(String^ fieldName)
        {
          array<UInt16>^ arr;
          m_dataInput->ReadObject(arr);
          return arr;
        }

        array<System::Int32>^ PdxLocalReader::ReadIntArray(String^ fieldName)
        {
          //array<Int32>^ arr;
         /// m_dataInput->ReadObject(arr);
          return m_dataInput->ReadIntArray();
        }

        array<System::UInt32>^ PdxLocalReader::ReadUnsignedIntArray(String^ fieldName)
        {
          array<UInt32>^ arr;
          m_dataInput->ReadObject(arr);
          return arr;
        }

        array<Int64>^ PdxLocalReader::ReadLongArray(String^ fieldName)
        {
         // array<Int64>^ arr;
         // m_dataInput->ReadObject(arr);
          return m_dataInput->ReadLongArray();
        }

        array<System::UInt64>^ PdxLocalReader::ReadUnsignedLongArray(String^ fieldName )
        {
          array<UInt64>^ arr;
          m_dataInput->ReadObject(arr);
          return arr;
        }

        array<float>^ PdxLocalReader::ReadFloatArray(String^ fieldName)
        {
          //array<float>^ arr;
          //m_dataInput->ReadObject(arr);
          return m_dataInput->ReadFloatArray();
        }

        array<double>^ PdxLocalReader::ReadDoubleArray(String^ fieldName)
        {
          //array<Double>^ arr;
          //m_dataInput->ReadObject(arr);
          return m_dataInput->ReadDoubleArray();
        }

        array<String^>^ PdxLocalReader::ReadStringArray(String^ fieldName)
        {
          return m_dataInput->ReadStringArray();
        }

        List<Object^>^ PdxLocalReader::ReadObjectArray(String^ fieldName)
        {
          return m_dataInput->ReadObjectArray();
        }

				array<array<Byte>^>^ PdxLocalReader::ReadArrayOfByteArrays(String^ fieldName )
        {
          return m_dataInput->ReadArrayOfByteArrays();
        }

        //TODO:
        //void WriteEnum(String^ fieldName, Enum e) ;
        //void WriteInetAddress(String^ fieldName, InetAddress address);

       
        bool PdxLocalReader::HasField(String^ fieldName)
        {
          return m_pdxType->GetPdxField(fieldName) != nullptr;
        }

        bool PdxLocalReader::IsIdentityField(String^ fieldName)
        {
          PdxFieldType^ pft = m_pdxType->GetPdxField(fieldName) ;
          return (pft != nullptr) && (pft->IdentityField);
        }

        Object^ PdxLocalReader::ReadField(String^ fieldName, Type^ type)
        {
            if(type->Equals(DotNetTypes::IntType))
            {
              return this->ReadInt(fieldName);
            }
            else if(type->Equals(DotNetTypes::StringType))
            {
              return this->ReadString(fieldName);
            }
            else if(type->Equals(DotNetTypes::BooleanType))
            {
              return this->ReadBoolean(fieldName);
            }
            else if(type->Equals(DotNetTypes::FloatType))
            {
              return this->ReadFloat(fieldName);
            }
            else if(type->Equals(DotNetTypes::DoubleType))
            {
              return this->ReadDouble(fieldName);
            }
            else if(type->Equals(DotNetTypes::CharType))
            {
              return this->ReadChar(fieldName);
            }
            else if(type->Equals(DotNetTypes::SByteType))
            {
              return this->ReadByte(fieldName);
            }
            else if(type->Equals(DotNetTypes::ShortType))
            {
              return this->ReadShort(fieldName);
            }
            else if(type->Equals(DotNetTypes::LongType))
            {
              return this->ReadLong(fieldName);
            }
            else if(type->Equals(DotNetTypes::ByteArrayType))
            {
              return this->ReadByteArray(fieldName);
            }
            else if(type->Equals(DotNetTypes::DoubleArrayType))
            {
              return this->ReadDoubleArray(fieldName);
            }
            else if(type->Equals(DotNetTypes::FloatArrayType))
            {
              return this->ReadFloatArray(fieldName);
            }
            else if(type->Equals(DotNetTypes::ShortArrayType))
            {
              return this->ReadShortArray(fieldName);
            }
            else if(type->Equals(DotNetTypes::IntArrayType))
            {
              return this->ReadIntArray(fieldName);
            }
            else if(type->Equals(DotNetTypes::LongArrayType))
            {
              return this->ReadLongArray(fieldName);
            }
            else if(type->Equals(DotNetTypes::BoolArrayType))
            {
              return this->ReadBooleanArray(fieldName);
            }
            else if(type->Equals(DotNetTypes::CharArrayType))
            {
              return this->ReadCharArray(fieldName);
            }
            else if(type->Equals(DotNetTypes::StringArrayType))
            {
              return this->ReadStringArray(fieldName);
            }
            else if(type->Equals(DotNetTypes::DateType))
            {
              return this->ReadDate(fieldName);
            }
            else if(type->Equals(DotNetTypes::ByteArrayOfArrayType))
            {
              return this->ReadArrayOfByteArrays(fieldName);
            }
            else if(type->Equals(DotNetTypes::ObjectArrayType))
            {
              return this->ReadObjectArray(fieldName);
            }            
            else
            {
              return this->ReadObject(fieldName);
               //throw gcnew IllegalStateException("ReadField unable to de-serialize  " 
								//																	+ fieldName + " of " + type); 
            }
          }

				}
      }
    }
  }
}