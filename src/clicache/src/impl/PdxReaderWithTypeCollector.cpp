/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "PdxReaderWithTypeCollector.hpp"
#include <gfcpp/GemfireTypeIds.hpp>
#include "../GemFireClassIds.hpp"
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
				void PdxReaderWithTypeCollector::checkType( String^ fieldName, Byte typeId, String^ fieldType)
				{
					PdxFieldType^ pft = m_pdxType->GetPdxField(fieldName);

					if(pft != nullptr)
					{
						if(typeId != pft->TypeId)
						{
							throw gcnew IllegalStateException("Expected " + fieldType + " field but found field of type " + pft);
						}
					}
				}

        SByte PdxReaderWithTypeCollector::ReadByte( String^ fieldName )
        {
					checkType(fieldName, PdxTypes::BYTE, "byte");
          m_newPdxType->AddFixedLengthTypeField(fieldName, "byte", PdxTypes::BYTE, GemFireClassIds::BYTE_SIZE);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          if(position != -1)
          {
            m_dataInput->AdvanceCursorPdx(position);
            SByte retVal = PdxLocalReader::ReadByte(fieldName);
            m_dataInput->RewindCursorPdx(position);

            return retVal;
          }
          return 0;
        }

        SByte PdxReaderWithTypeCollector::ReadSByte( String^ fieldName )
        {
					checkType(fieldName, PdxTypes::BYTE, "byte");
          m_newPdxType->AddFixedLengthTypeField( fieldName, "byte", PdxTypes::BYTE, GemFireClassIds::BYTE_SIZE);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          if(position != -1)
          {
            m_dataInput->AdvanceCursorPdx(position);
            SByte retVal = PdxLocalReader::ReadSByte(fieldName);
            m_dataInput->RewindCursorPdx(position);
            return retVal;
          }
          return 0;
        }

				Boolean PdxReaderWithTypeCollector::ReadBoolean( String^ fieldName )
        {
					checkType(fieldName, PdxTypes::BOOLEAN, "boolean");
          m_newPdxType->AddFixedLengthTypeField(fieldName, "boolean", PdxTypes::BOOLEAN, GemFireClassIds::BOOLEAN_SIZE);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          m_dataInput->AdvanceCursorPdx(position);
          bool retVal = PdxLocalReader::ReadBoolean(fieldName);
          m_dataInput->RewindCursorPdx(position);
          return retVal;
        }

        Char PdxReaderWithTypeCollector::ReadChar( String^ fieldName )
        {
					checkType(fieldName, PdxTypes::CHAR, "char");
          m_newPdxType->AddFixedLengthTypeField(fieldName, "char", PdxTypes::CHAR, GemFireClassIds::CHAR_SIZE);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          m_dataInput->AdvanceCursorPdx(position);
          Char retVal = PdxLocalReader::ReadChar(fieldName);
          m_dataInput->RewindCursorPdx(position);
          return retVal;
        }
        
        uint16_t PdxReaderWithTypeCollector::ReadUInt16( String^ fieldName )
        {
					checkType(fieldName, PdxTypes::SHORT, "short");
          m_newPdxType->AddFixedLengthTypeField(fieldName, "short", PdxTypes::SHORT, GemFireClassIds::SHORT_SIZE);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          if(position != -1)
          {
            m_dataInput->AdvanceCursorPdx(position);
            UInt16 retVal = PdxLocalReader::ReadUInt16(fieldName);
            m_dataInput->RewindCursorPdx(position);
            return retVal;
          }
          return 0;
        }

        uint32_t PdxReaderWithTypeCollector::ReadUInt32( String^ fieldName )
        {
					checkType(fieldName, PdxTypes::INT, "int");
          m_newPdxType->AddFixedLengthTypeField(fieldName, "int", PdxTypes::INT, GemFireClassIds::INTEGER_SIZE);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          if(position != -1)
          {
            m_dataInput->AdvanceCursorPdx(position);
            UInt32 retVal = PdxLocalReader::ReadUInt32(fieldName);
            m_dataInput->RewindCursorPdx(position);
            return retVal;
          }
          return 0;
        }
        
        uint64_t PdxReaderWithTypeCollector::ReadUInt64( String^ fieldName )
        {
					checkType(fieldName, PdxTypes::LONG, "long");
          m_newPdxType->AddFixedLengthTypeField(fieldName, "long", PdxTypes::LONG, GemFireClassIds::LONG_SIZE);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          if(position != -1)
          {
            m_dataInput->AdvanceCursorPdx(position);
            UInt64 retVal = PdxLocalReader::ReadUInt64(fieldName);
            m_dataInput->RewindCursorPdx(position);
            return retVal;
          }
          return 0;
        }

        short PdxReaderWithTypeCollector::ReadShort( String^ fieldName )
        {
					checkType(fieldName, PdxTypes::SHORT, "short");
          m_newPdxType->AddFixedLengthTypeField(fieldName, "short", PdxTypes::SHORT, GemFireClassIds::SHORT_SIZE);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          if(position != -1)
          {
            m_dataInput->AdvanceCursorPdx(position);
            Int16 retVal = PdxLocalReader::ReadShort(fieldName);
            m_dataInput->RewindCursorPdx(position);
            return retVal;
          }
          return 0;
        }

				Int32 PdxReaderWithTypeCollector::ReadInt( String^ fieldName )
        {
					checkType(fieldName, PdxTypes::INT, "int");
          m_newPdxType->AddFixedLengthTypeField(fieldName, "int", PdxTypes::INT, GemFireClassIds::INTEGER_SIZE);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          if(position != -1)
          {
            m_dataInput->AdvanceCursorPdx(position);
            Int32 retVal = PdxLocalReader::ReadInt(fieldName);
            m_dataInput->RewindCursorPdx(position);
            return retVal;
          }
          return 0;
        }

        Int64 PdxReaderWithTypeCollector::ReadLong( String^ fieldName )
        {
					checkType(fieldName, PdxTypes::LONG, "long");
          m_newPdxType->AddFixedLengthTypeField(fieldName, "long", PdxTypes::LONG, GemFireClassIds::LONG_SIZE);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          if(position != -1)
          {
            m_dataInput->AdvanceCursorPdx(position);
            Int64 retVal = PdxLocalReader::ReadLong(fieldName);
            m_dataInput->RewindCursorPdx(position);
            return retVal;
          }
          return 0;
        }

        float PdxReaderWithTypeCollector::ReadFloat( String^ fieldName )
        {
					checkType(fieldName, PdxTypes::FLOAT, "float");
          m_newPdxType->AddFixedLengthTypeField(fieldName, "float", PdxTypes::FLOAT, GemFireClassIds::FLOAT_SIZE);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          if(position != -1)
          {
            m_dataInput->AdvanceCursorPdx(position);
            float retVal = PdxLocalReader::ReadFloat(fieldName);
            m_dataInput->RewindCursorPdx(position);
            return retVal;
          }
          return 0.0;
        }

        double PdxReaderWithTypeCollector::ReadDouble( String^ fieldName )
        {
					checkType(fieldName, PdxTypes::DOUBLE, "double");
          m_newPdxType->AddFixedLengthTypeField(fieldName, "double", PdxTypes::DOUBLE, GemFireClassIds::DOUBLE_SIZE);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          if(position != -1)
          {
            m_dataInput->AdvanceCursorPdx(position);
            Double retVal = PdxLocalReader::ReadDouble(fieldName);
            m_dataInput->RewindCursorPdx(position);
            return retVal;
          }
          return 0.0;
        }

        String^ PdxReaderWithTypeCollector::ReadString( String^ fieldName )
        {
					checkType(fieldName, PdxTypes::STRING, "String");
          m_newPdxType->AddVariableLengthTypeField(fieldName, "String", PdxTypes::STRING);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          if(position != -1)
          {
            m_dataInput->AdvanceCursorPdx(position);
            String^ retVal = PdxLocalReader::ReadString(fieldName);
            m_dataInput->RewindCursorPdx(position);
            return retVal;
          }
          return nullptr;
        }

        String^ PdxReaderWithTypeCollector::ReadUTFHuge( String^ fieldName )
        {
					checkType(fieldName, PdxTypes::STRING, "String");
          m_newPdxType->AddVariableLengthTypeField(fieldName, "String", PdxTypes::STRING);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          if(position != -1)
          {
            m_dataInput->AdvanceCursorPdx(position);
            String^ retVal = PdxLocalReader::ReadUTFHuge(fieldName);
            m_dataInput->RewindCursorPdx(position);
            return retVal;
          }
          return nullptr;
        }

        String^ PdxReaderWithTypeCollector::ReadASCIIHuge( String^ fieldName )
        {
					checkType(fieldName, PdxTypes::STRING, "String");
          m_newPdxType->AddVariableLengthTypeField(fieldName, "String", PdxTypes::STRING);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          if(position != -1)
          {
            m_dataInput->AdvanceCursorPdx(position);
            String^ retVal = PdxLocalReader::ReadASCIIHuge(fieldName);
            m_dataInput->RewindCursorPdx(position);
            return retVal;
          }
          return nullptr;
        }

        Object^ PdxReaderWithTypeCollector::ReadObject( String^ fieldName )
        {
					//field is collected after reading
					checkType(fieldName, PdxTypes::OBJECT, "Object");
          m_newPdxType->AddVariableLengthTypeField(fieldName, "Object"/*retVal->GetType()->FullName*/, PdxTypes::OBJECT);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          if(position != -1)
          {
            m_dataInput->AdvanceCursorPdx(position);
            Object^ retVal = PdxLocalReader::ReadObject(fieldName);            
            m_dataInput->ResetPdx(m_startPosition);//force native as well
            m_dataInput->RewindCursorPdx(position);
            return retVal;
          }
          return nullptr;
        }
        
        //TODO:
        //void WriteMap( String^ fieldName, System::Collections::IDictionary^ map );

        void PdxReaderWithTypeCollector::ReadCollection( String^ fieldName, System::Collections::IList^ obj)
        {
					checkType(fieldName, gemfire::GemfireTypeIds::CacheableArrayList, "Collection");
          m_newPdxType->AddVariableLengthTypeField(fieldName, /*obj->GetType()->FullName*/"Collection", gemfire::GemfireTypeIds::CacheableArrayList);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          if(position != -1)
          {
            m_dataInput->AdvanceCursorPdx(position);
            PdxLocalReader::ReadCollection(fieldName, obj);
            m_dataInput->RewindCursorPdx(position);
          }
        }

        System::DateTime PdxReaderWithTypeCollector::ReadDate( String^ fieldName)
        {
					checkType(fieldName, PdxTypes::DATE, "Date");
          m_newPdxType->AddFixedLengthTypeField(fieldName, "Date", PdxTypes::DATE, GemFireClassIds::DATE_SIZE);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          if(position != -1)
          {
            m_dataInput->AdvanceCursorPdx(position);
            System::DateTime retVal = PdxLocalReader::ReadDate(fieldName);
            m_dataInput->RewindCursorPdx(position);
            return retVal;
          }
          DateTime dt(0);
          return dt;
        }
        //void writeFile(String fieldName, File file) ;

        array<bool>^ PdxReaderWithTypeCollector::ReadBooleanArray( String^ fieldName )
        {
					checkType(fieldName, PdxTypes::BOOLEAN_ARRAY, "boolean[]");
          m_newPdxType->AddVariableLengthTypeField(fieldName, "boolean[]", PdxTypes::BOOLEAN_ARRAY);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          if(position != -1)
          {
            m_dataInput->AdvanceCursorPdx(position);
            array<bool>^ retVal = PdxLocalReader::ReadBooleanArray(fieldName);
            m_dataInput->RewindCursorPdx(position);
            return retVal;
          }
          return nullptr;
        }

        array<Char>^ PdxReaderWithTypeCollector::ReadCharArray(String^ fieldName )
        {
					checkType(fieldName, PdxTypes::CHAR_ARRAY, "char[]");
          m_newPdxType->AddVariableLengthTypeField(fieldName, "char[]", PdxTypes::CHAR_ARRAY);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          if(position != -1)
          {
            m_dataInput->AdvanceCursorPdx(position);
            array<Char>^ retVal = PdxLocalReader::ReadCharArray(fieldName);
            m_dataInput->RewindCursorPdx(position);
            return retVal;
          }
          return nullptr;
        }

        array<Byte>^ PdxReaderWithTypeCollector::ReadByteArray(String^ fieldName)
        {
					checkType(fieldName, PdxTypes::BYTE_ARRAY, "byte[]");
          m_newPdxType->AddVariableLengthTypeField(fieldName, "byte[]", PdxTypes::BYTE_ARRAY);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          if(position != -1)
          {
            m_dataInput->AdvanceCursorPdx(position);
            array<Byte>^ retVal = PdxLocalReader::ReadByteArray(fieldName);
            m_dataInput->RewindCursorPdx(position);
            return retVal;
          }
          return nullptr;
        }

        array<SByte>^ PdxReaderWithTypeCollector::ReadSByteArray(String^ fieldName)
        {
					checkType(fieldName, PdxTypes::BYTE_ARRAY, "byte[]");
          m_newPdxType->AddVariableLengthTypeField(fieldName, "byte[]", PdxTypes::BYTE_ARRAY);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          if(position != -1)
          {
            m_dataInput->AdvanceCursorPdx(position);
            array<SByte>^ retVal = PdxLocalReader::ReadSByteArray(fieldName);
            m_dataInput->RewindCursorPdx(position);
            return retVal;
          }
          return nullptr;
        }

        array<short>^ PdxReaderWithTypeCollector::ReadShortArray(String^ fieldName)
        {
					checkType(fieldName, PdxTypes::SHORT_ARRAY, "short[]");
          m_newPdxType->AddVariableLengthTypeField(fieldName, "short[]", PdxTypes::SHORT_ARRAY);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          if(position != -1)
          {
            m_dataInput->AdvanceCursorPdx(position);
            array<Int16>^ retVal = PdxLocalReader::ReadShortArray(fieldName);
            m_dataInput->RewindCursorPdx(position);
            return retVal;
          }
          return nullptr;
        }

        array<System::UInt16>^ PdxReaderWithTypeCollector::ReadUnsignedShortArray(String^ fieldName)
        {
					checkType(fieldName, PdxTypes::SHORT_ARRAY, "short[]");
          m_newPdxType->AddVariableLengthTypeField(fieldName, "short[]", PdxTypes::SHORT_ARRAY);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          if(position != -1)
          {
            m_dataInput->AdvanceCursorPdx(position);
            array<UInt16>^ retVal = PdxLocalReader::ReadUnsignedShortArray(fieldName);
            m_dataInput->RewindCursorPdx(position);
            return retVal;
          }
          return nullptr;
        }

        array<System::Int32>^ PdxReaderWithTypeCollector::ReadIntArray(String^ fieldName)
        {
					checkType(fieldName, PdxTypes::INT_ARRAY, "int[]");
          m_newPdxType->AddVariableLengthTypeField(fieldName, "int[]", PdxTypes::INT_ARRAY);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          if(position != -1)
          {
            m_dataInput->AdvanceCursorPdx(position);
            array<Int32>^ retVal = PdxLocalReader::ReadIntArray(fieldName);
            m_dataInput->RewindCursorPdx(position);
            return retVal;
          }
          return nullptr;
        }

        array<System::UInt32>^ PdxReaderWithTypeCollector::ReadUnsignedIntArray(String^ fieldName)
        {
					checkType(fieldName, PdxTypes::INT_ARRAY, "int[]");
          m_newPdxType->AddVariableLengthTypeField(fieldName, "int[]", PdxTypes::INT_ARRAY);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          if(position != -1)
          {
            m_dataInput->AdvanceCursorPdx(position);
            array<UInt32>^ retVal = PdxLocalReader::ReadUnsignedIntArray(fieldName);
            m_dataInput->RewindCursorPdx(position);
            return retVal;
          }
          return nullptr;
        }

        array<Int64>^ PdxReaderWithTypeCollector::ReadLongArray(String^ fieldName)
        {
					checkType(fieldName, PdxTypes::LONG_ARRAY, "long[]");
          m_newPdxType->AddVariableLengthTypeField(fieldName, "long[]", PdxTypes::LONG_ARRAY);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          if(position != -1)
          {
            m_dataInput->AdvanceCursorPdx(position);
            array<Int64>^ retVal = PdxLocalReader::ReadLongArray(fieldName);
            m_dataInput->RewindCursorPdx(position);
            return retVal;
          }
          return nullptr;
        }

        array<System::UInt64>^ PdxReaderWithTypeCollector::ReadUnsignedLongArray(String^ fieldName )
        {
					checkType(fieldName, PdxTypes::LONG_ARRAY, "long[]");
          m_newPdxType->AddVariableLengthTypeField(fieldName, "long[]", PdxTypes::LONG_ARRAY);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          if(position != -1)
          {
            m_dataInput->AdvanceCursorPdx(position);
            array<UInt64>^ retVal = PdxLocalReader::ReadUnsignedLongArray(fieldName);
            m_dataInput->RewindCursorPdx(position);
            return retVal;
          }
          return nullptr;
        }

        array<float>^ PdxReaderWithTypeCollector::ReadFloatArray(String^ fieldName)
        {
					checkType(fieldName, PdxTypes::FLOAT_ARRAY, "float[]");
          m_newPdxType->AddVariableLengthTypeField(fieldName, "float[]", PdxTypes::FLOAT_ARRAY);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          if(position != -1)
          {
            m_dataInput->AdvanceCursorPdx(position);
            array<float>^ retVal = PdxLocalReader::ReadFloatArray(fieldName);
            m_dataInput->RewindCursorPdx(position);
            return retVal;
          }
          return nullptr;
        }

        array<double>^ PdxReaderWithTypeCollector::ReadDoubleArray(String^ fieldName)
        {
					checkType(fieldName, PdxTypes::DOUBLE_ARRAY, "double[]");
          m_newPdxType->AddVariableLengthTypeField(fieldName, "double[]", PdxTypes::DOUBLE_ARRAY);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          if(position != -1)
          {
            m_dataInput->AdvanceCursorPdx(position);
            array<Double>^ retVal = PdxLocalReader::ReadDoubleArray(fieldName);
            m_dataInput->RewindCursorPdx(position);
            return retVal;
          }
          return nullptr;
        }

        array<String^>^ PdxReaderWithTypeCollector::ReadStringArray(String^ fieldName)
        {
					checkType(fieldName, PdxTypes::STRING_ARRAY, "String[]");
          m_newPdxType->AddVariableLengthTypeField(fieldName, "String[]", PdxTypes::STRING_ARRAY);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          if(position != -1)
          {
            m_dataInput->AdvanceCursorPdx(position);
            array<String^>^ retVal = PdxLocalReader::ReadStringArray(fieldName);
            m_dataInput->RewindCursorPdx(position);
            return retVal;
          }
          return nullptr;
        }

        List<Object^>^ PdxReaderWithTypeCollector::ReadObjectArray(String^ fieldName)
        {
					checkType(fieldName, PdxTypes::OBJECT_ARRAY, "Object[]");
          m_newPdxType->AddVariableLengthTypeField(fieldName, "Object[]", PdxTypes::OBJECT_ARRAY);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          if(position != -1)
          {
            m_dataInput->AdvanceCursorPdx(position);
            List<Object^>^ retVal = PdxLocalReader::ReadObjectArray(fieldName);
            m_dataInput->ResetPdx(m_startPosition);//force native as well
            m_dataInput->RewindCursorPdx(position);
            return retVal;
          }
          return nullptr;
        }

        array<array<Byte>^>^ PdxReaderWithTypeCollector::ReadArrayOfByteArrays(String^ fieldName )
        {
					checkType(fieldName, PdxTypes::ARRAY_OF_BYTE_ARRAYS, "byte[][]");
          m_newPdxType->AddVariableLengthTypeField(fieldName, "byte[][]", PdxTypes::ARRAY_OF_BYTE_ARRAYS);
          int position = m_pdxType->GetFieldPosition(fieldName, m_offsetsBuffer, m_offsetSize, m_serializedLength);
          if(position != -1)
          {
            m_dataInput->AdvanceCursorPdx(position);
            array<array<Byte>^>^ retVal = PdxLocalReader::ReadArrayOfByteArrays(fieldName);
            m_dataInput->RewindCursorPdx(position);
            return retVal;
          }
          return nullptr;
        }

        //TODO:
        //void WriteEnum(String^ fieldName, Enum e) ;
        //void WriteInetAddress(String^ fieldName, InetAddress address);

        
      }
			}
    }
  }
}