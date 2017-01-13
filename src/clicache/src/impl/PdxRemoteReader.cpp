/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "PdxRemoteReader.hpp"
#include "../Log.hpp"
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
        void PdxRemoteReader::setNextOffsetToRead()
        {
         
        }
        
        

        SByte PdxRemoteReader::ReadByte( String^ fieldName )
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadByte(fieldName);//in same order
          case -1:
            {
              return 0;//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              SByte retVal = PdxLocalReader::ReadByte(fieldName);
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }

        SByte PdxRemoteReader::ReadSByte( String^ fieldName )
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadSByte(fieldName);//in same order
          case -1:
            {
              return 0;//null value
            }
          default:
            {
              //sequence id read field and then update
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              SByte retVal = PdxLocalReader::ReadSByte(fieldName);
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }

				Boolean PdxRemoteReader::ReadBoolean( String^ fieldName )
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadBoolean(fieldName);//in same order
          case -1:
            {
              return false;//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              bool retVal = PdxLocalReader::ReadBoolean(fieldName);
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }

        Char PdxRemoteReader::ReadChar( String^ fieldName )
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadChar(fieldName);//in same order
          case -1:
            {
              return '\0';//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              Char retVal = PdxLocalReader::ReadChar(fieldName);
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }
        
        uint16_t PdxRemoteReader::ReadUInt16( String^ fieldName )
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadUInt16(fieldName);//in same order
          case -1:
            {
              return 0;//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              UInt16 retVal = PdxLocalReader::ReadUInt16(fieldName);
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }

        uint32_t PdxRemoteReader::ReadUInt32( String^ fieldName )
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadUInt32(fieldName);//in same order
          case -1:
            {
              return 0;//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              UInt32 retVal = PdxLocalReader::ReadUInt32(fieldName);
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }
        
        uint64_t PdxRemoteReader::ReadUInt64( String^ fieldName )
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadUInt64(fieldName);//in same order
          case -1:
            {
              return 0;//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              UInt64 retVal = PdxLocalReader::ReadUInt64(fieldName);
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }

        short PdxRemoteReader::ReadShort( String^ fieldName )
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadShort(fieldName);//in same order
          case -1:
            {
              return 0;//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              Int16 retVal = PdxLocalReader::ReadShort(fieldName);
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }

				Int32 PdxRemoteReader::ReadInt( String^ fieldName )
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

           //Log::Debug("found choice " + choice + " " + m_currentIndex);

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadInt(fieldName);//in same order
          case -1:
            {
              return 0;//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              //Log::Debug("found choice position:" + position);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              Int32 retVal = PdxLocalReader::ReadInt(fieldName);
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }

        Int64 PdxRemoteReader::ReadLong( String^ fieldName )
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadLong(fieldName);//in same order
          case -1:
            {
              return 0;//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              Int64 retVal = PdxLocalReader::ReadLong(fieldName);
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }

        float PdxRemoteReader::ReadFloat( String^ fieldName )
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadFloat(fieldName);//in same order
          case -1:
            {
              return 0.0;//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              float retVal = PdxLocalReader::ReadFloat(fieldName);
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }

        double PdxRemoteReader::ReadDouble( String^ fieldName )
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadDouble(fieldName);//in same order
          case -1:
            {
              return 0.0;//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              double retVal = PdxLocalReader::ReadDouble(fieldName);
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }

        String^ PdxRemoteReader::ReadString( String^ fieldName )
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadString(fieldName);//in same order
          case -1:
            {
              return nullptr;//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              String^ retVal = PdxLocalReader::ReadString(fieldName);
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }

        String^ PdxRemoteReader::ReadUTFHuge( String^ fieldName )
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadUTFHuge(fieldName);//in same order
          case -1:
            {
              return nullptr;//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              String^ retVal = PdxLocalReader::ReadUTFHuge(fieldName);
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }

        String^ PdxRemoteReader::ReadASCIIHuge( String^ fieldName )
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadASCIIHuge(fieldName);//in same order
          case -1:
            {
              return nullptr;//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              String^ retVal = PdxLocalReader::ReadASCIIHuge(fieldName);
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }

        Object^ PdxRemoteReader::ReadObject( String^ fieldName )
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadObject(fieldName);//in same order
          case -1:
            {
              return nullptr;//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              Object^ retVal = PdxLocalReader::ReadObject(fieldName);
              m_dataInput->ResetPdx(m_startPosition);//force native as well
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }
        
        //TODO:
        //void WriteMap( String^ fieldName, System::Collections::IDictionary^ map );

        void PdxRemoteReader::ReadCollection( String^ fieldName, System::Collections::IList^ collection)
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            PdxLocalReader::ReadCollection(fieldName, collection);//in same order
            break;
          case -1:
            {
              break;//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              PdxLocalReader::ReadCollection(fieldName, collection);
              m_dataInput->RewindCursorPdx(position);
            }
          }
        }

        System::DateTime PdxRemoteReader::ReadDate( String^ fieldName)
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadDate(fieldName);//in same order
          case -1:
            {
              DateTime dt(0);
              return dt;//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              System::DateTime retVal = PdxLocalReader::ReadDate(fieldName);
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }
        //void writeFile(String fieldName, File file) ;

				array<Boolean>^ PdxRemoteReader::ReadBooleanArray( String^ fieldName )
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadBooleanArray(fieldName);//in same order
          case -1:
            {
              return nullptr;//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              array<bool>^ retVal = PdxLocalReader::ReadBooleanArray(fieldName);
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }

        array<Char>^ PdxRemoteReader::ReadCharArray(String^ fieldName )
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadCharArray(fieldName);//in same order
          case -1:
            {
              return nullptr;//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              array<Char>^ retVal = PdxLocalReader::ReadCharArray(fieldName);
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }

        array<Byte>^ PdxRemoteReader::ReadByteArray(String^ fieldName)
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadByteArray(fieldName);//in same order
          case -1:
            {
              return nullptr;//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              array<Byte>^ retVal = PdxLocalReader::ReadByteArray(fieldName);
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }

        array<SByte>^ PdxRemoteReader::ReadSByteArray(String^ fieldName)
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadSByteArray(fieldName);//in same order
          case -1:
            {
              return nullptr;//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              array<SByte>^ retVal = PdxLocalReader::ReadSByteArray(fieldName);
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }

        array<short>^ PdxRemoteReader::ReadShortArray(String^ fieldName)
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadShortArray(fieldName);//in same order
          case -1:
            {
              return nullptr;//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              array<Int16>^ retVal = PdxLocalReader::ReadShortArray(fieldName);
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }

        array<System::UInt16>^ PdxRemoteReader::ReadUnsignedShortArray(String^ fieldName)
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadUnsignedShortArray(fieldName);//in same order
          case -1:
            {
              return nullptr;//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              array<UInt16>^ retVal = PdxLocalReader::ReadUnsignedShortArray(fieldName);
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }

        array<System::Int32>^ PdxRemoteReader::ReadIntArray(String^ fieldName)
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadIntArray(fieldName);//in same order
          case -1:
            {
              return nullptr;//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              array<Int32>^ retVal = PdxLocalReader::ReadIntArray(fieldName);
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }

        array<System::UInt32>^ PdxRemoteReader::ReadUnsignedIntArray(String^ fieldName)
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadUnsignedIntArray(fieldName);//in same order
          case -1:
            {
              return nullptr;//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              array<UInt32>^ retVal = PdxLocalReader::ReadUnsignedIntArray(fieldName);
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }

        array<Int64>^ PdxRemoteReader::ReadLongArray(String^ fieldName)
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadLongArray(fieldName);//in same order
          case -1:
            {
              return nullptr;//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              array<Int64>^ retVal = PdxLocalReader::ReadLongArray(fieldName);
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }

        array<System::UInt64>^ PdxRemoteReader::ReadUnsignedLongArray(String^ fieldName )
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadUnsignedLongArray(fieldName);//in same order
          case -1:
            {
              return nullptr;//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              array<UInt64>^ retVal = PdxLocalReader::ReadUnsignedLongArray(fieldName);
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }

        array<float>^ PdxRemoteReader::ReadFloatArray(String^ fieldName)
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadFloatArray(fieldName);//in same order
          case -1:
            {
              return nullptr;//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              array<float>^ retVal = PdxLocalReader::ReadFloatArray(fieldName);
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }

        array<double>^ PdxRemoteReader::ReadDoubleArray(String^ fieldName)
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadDoubleArray(fieldName);//in same order
          case -1:
            {
              return nullptr;//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              array<double>^ retVal = PdxLocalReader::ReadDoubleArray(fieldName);
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }

        array<String^>^ PdxRemoteReader::ReadStringArray(String^ fieldName)
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadStringArray(fieldName);//in same order
          case -1:
            {
              return nullptr;//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              array<String^>^ retVal = PdxLocalReader::ReadStringArray(fieldName);
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }

        List<Object^>^ PdxRemoteReader::ReadObjectArray(String^ fieldName)
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadObjectArray(fieldName);//in same order
          case -1:
            {
              return nullptr;//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              List<Object^>^ retVal = PdxLocalReader::ReadObjectArray(fieldName);
              m_dataInput->ResetPdx(m_startPosition);//force native as well
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }

        array<array<Byte>^>^ PdxRemoteReader::ReadArrayOfByteArrays(String^ fieldName )
        {
          int choice = m_localToRemoteMap[m_currentIndex++];

          switch(choice)
          {
          case -2:
            return PdxLocalReader::ReadArrayOfByteArrays(fieldName);//in same order
          case -1:
            {
              return nullptr;//null value
            }
          default:
            {
              //sequence id read field and then update 
              int position = m_pdxType->GetFieldPosition(choice, m_offsetsBuffer, m_offsetSize, m_serializedLength);
              m_dataInput->ResetAndAdvanceCursorPdx(position);
              array<array<Byte>^>^ retVal = PdxLocalReader::ReadArrayOfByteArrays(fieldName);
              m_dataInput->RewindCursorPdx(position);
              return retVal;
            }
          }
        }

        //TODO:
        //void WriteEnum(String^ fieldName, Enum e) ;
        //void WriteInetAddress(String^ fieldName, InetAddress address);

        
      }
			}
    }
  }
}