/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "gf_includes.hpp"
#include "DataOutput.hpp"
#include <GemfireTypeIdsImpl.hpp>
#include <vcclr.h>

#include "IGFSerializable.hpp"
#include "CacheableObjectArray.hpp"
#include "impl/PdxHelper.hpp"
#include "impl/PdxWrapper.hpp"
using namespace System;
using namespace System::Runtime::InteropServices;
using namespace gemfire;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Generic
      {

				void DataOutput::WriteByte( Byte value )
				{
					EnsureCapacity(1);
					m_bytes[m_cursor++] = value;
				}

				void DataOutput::WriteSByte( SByte value )
				{
					EnsureCapacity(1);
					m_bytes[m_cursor++] = value;
				}

				void DataOutput::WriteBoolean( bool value )
				{
					EnsureCapacity(1);
					if (value)
						m_bytes[m_cursor++] = 0x01;
					else
						m_bytes[m_cursor++] = 0x00;
				}

				void DataOutput::WriteChar( Char value )
				{
					EnsureCapacity(2);
					m_bytes[m_cursor++] = (uint8_t)(value >> 8);
					m_bytes[m_cursor++] = (uint8_t)value;
				}

				void DataOutput::WriteBytes( array<Byte>^ bytes, int32_t len )
				{
					if (bytes != nullptr && bytes->Length >= 0)
					{
						if ( len >= 0 && len <= bytes->Length )
						{
							WriteArrayLen(len);
							EnsureCapacity(len);
							for( int i = 0; i < len; i++ )
								m_bytes[m_cursor++] = bytes[i];
						}
						else
						{
							throw gcnew IllegalArgumentException("DataOutput::WriteBytes argument len is not in byte array range." );
						}
					}
					else
					{
						WriteByte(0xFF);
					}
				}

				void DataOutput::WriteArrayLen( int32_t len )
				{
					if (len == -1) {//0xff
						WriteByte(0xFF);
					} else if (len <= 252) { // 252 is java's ((byte)-4 && 0xFF) or 0xfc
						WriteByte((Byte)(len));
					} else if (len <= 0xFFFF) {
						WriteByte(0xFE);//0xfe
						WriteUInt16((len));
					} else {
						WriteByte((0xFD));//0xfd
						WriteUInt32(len);
					}
				}

				void DataOutput::WriteSBytes( array<SByte>^ bytes, int32_t len )
				{
					if (bytes != nullptr && bytes->Length >= 0)
					{
						if ( len >= 0 && len <= bytes->Length )
						{
							WriteArrayLen(len);
							EnsureCapacity(len);
							for( int i = 0; i < len; i++ )
								m_bytes[m_cursor++] = bytes[i];
						}
						else
						{
							throw gcnew IllegalArgumentException("DataOutput::WriteSBytes argument len is not in SByte array range." );
						}
					}
					else
					{
						WriteByte(0xFF);
					}
				}

				void DataOutput::WriteBytesOnly( array<Byte>^ bytes, uint32_t len )
				{
					WriteBytesOnly(bytes, len, 0);
				}

				void DataOutput::WriteBytesOnly( array<Byte>^ bytes, uint32_t len, uint32_t offset )
				{
					if (bytes != nullptr)
					{
						if ( len >= 0 && len <= ((uint32_t)bytes->Length - offset) )
						{
							EnsureCapacity(len);            
							for( uint32_t i = 0; i < len; i++ )
								m_bytes[m_cursor++] = bytes[offset + i];
						}
						else
						{
							throw gcnew IllegalArgumentException("DataOutput::WriteBytesOnly argument len is not in Byte array range." );
						}
					}
				}

				void DataOutput::WriteSBytesOnly( array<SByte>^ bytes, uint32_t len )
				{
					if (bytes != nullptr)
					{
						if ( len >= 0 && len <= (uint32_t)bytes->Length )
						{
							EnsureCapacity(len);
							for( uint32_t i = 0; i < len; i++ )
								m_bytes[m_cursor++] = bytes[i];
						}
						else
						{
							throw gcnew IllegalArgumentException("DataOutput::WriteSBytesOnly argument len is not in SByte array range." );
						}
					}
				}

				void DataOutput::WriteUInt16( uint16_t value )
				{
					EnsureCapacity(2);
					m_bytes[m_cursor++] = (uint8_t)(value >> 8);
					m_bytes[m_cursor++] = (uint8_t)value;
				}

				void DataOutput::WriteUInt32( uint32_t value )
				{
					EnsureCapacity(4);
					m_bytes[m_cursor++] = (uint8_t)(value >> 24);
					m_bytes[m_cursor++] = (uint8_t)(value >> 16);
					m_bytes[m_cursor++] = (uint8_t)(value >> 8);
					m_bytes[m_cursor++] = (uint8_t)value;
				}

				void DataOutput::WriteUInt64( uint64_t value )
				{
					EnsureCapacity(8);
					m_bytes[m_cursor++] = (uint8_t)(value >> 56);
					m_bytes[m_cursor++] = (uint8_t)(value >> 48);
					m_bytes[m_cursor++] = (uint8_t)(value >> 40);
					m_bytes[m_cursor++] = (uint8_t)(value >> 32);
					m_bytes[m_cursor++] = (uint8_t)(value >> 24);
					m_bytes[m_cursor++] = (uint8_t)(value >> 16);
					m_bytes[m_cursor++] = (uint8_t)(value >> 8);
					m_bytes[m_cursor++] = (uint8_t)value;
				}

				void DataOutput::WriteInt16( int16_t value )
				{
					WriteUInt16(value);
				}

				void DataOutput::WriteInt32( int32_t value )
				{
					WriteUInt32(value);
				}

				void DataOutput::WriteInt64( int64_t value )
				{
					WriteUInt64(value);
				}

				void DataOutput::WriteFloat( float value )
				{
					array<Byte>^ bytes = BitConverter::GetBytes(value);
					EnsureCapacity(4);
					for(int i = 4 - 1; i >=0 ; i--)
						m_bytes[m_cursor++] = bytes[i];
				}

				void DataOutput::WriteDouble( double value )
				{
					array<Byte>^ bytes = BitConverter::GetBytes(value);
					EnsureCapacity(8);
					for(int i = 8 - 1; i >=0 ; i--)
						m_bytes[m_cursor++] = bytes[i];
				}

			void DataOutput::WriteDictionary(System::Collections::IDictionary^ dict)
      {
        if(dict != nullptr)
        {
          this->WriteArrayLen(dict->Count);
          for each( System::Collections::DictionaryEntry^ entry in dict) 
          {
            this->WriteObject(entry->Key);
            this->WriteObject(entry->Value);
          }
        }
        else
        {
          WriteByte( (int8_t) -1);
        }
      }

      void DataOutput::WriteCollection(System::Collections::IList^ collection)
      {
        if(collection != nullptr)
        {
          this->WriteArrayLen(collection->Count);
          for each (Object^ obj in collection) {
            this->WriteObject(obj);
          }
        }
        else
          this->WriteByte((int8_t) -1);
      }

      void DataOutput::WriteDate(System::DateTime date)
      {
        if(date.Ticks != 0L)
        {
          CacheableDate^ cd = gcnew CacheableDate(date);
          cd->ToData(this);
        }
        else
          this->WriteInt64(-1L);
      }

      void DataOutput::WriteCharArray(array<Char>^ charArray)
      {
        if(charArray != nullptr)
        {
          this->WriteArrayLen(charArray->Length);
          for(int i = 0; i < charArray->Length; i++) {
            this->WriteObject(charArray[i]);
          }
        }
        else
          this->WriteByte((int8_t) -1);
      }

      void DataOutput::WriteObjectArray(List<Object^>^ objectArray)
      {
        if(objectArray != nullptr)
        {
          CacheableObjectArray^ coa = CacheableObjectArray::Create(objectArray);
          coa->ToData(this);
        }
        else
          this->WriteByte((int8_t) -1);
      }

      void DataOutput::WriteDotNetObjectArray(Object^ objectArray)
      {
        System::Collections::IList^ list = (System::Collections::IList^)objectArray;
        this->WriteArrayLen(list->Count);
        WriteByte((int8_t)gemfire::GemfireTypeIdsImpl::Class);
        String^ pdxDomainClassname = Serializable::GetPdxTypeName(objectArray->GetType()->GetElementType()->FullName);
        WriteByte((int8_t)gemfire::GemfireTypeIds::CacheableASCIIString);
        WriteUTF(pdxDomainClassname);
        for each(Object^ o in list)
          WriteObject(o);
      }

      void DataOutput::WriteArrayOfByteArrays(array<array<Byte>^>^ byteArrays)
      {
        if(byteArrays != nullptr)
        {
          int fdLen = byteArrays->Length;
          this->WriteArrayLen(byteArrays->Length);
          for(int i = 0; i < fdLen; i++) {
						this->WriteBytes(byteArrays[i]);
          }
        }
        else
          this->WriteByte((int8_t) -1);
      }

				void DataOutput::WriteUTF( String^ value )
				{
					if (value != nullptr) {
						int len = getEncodedLength(value);
	          
						if (len > 0xffff)
							len = 0xffff;

						WriteUInt16(len);
						EnsureCapacity(len);
						EncodeUTF8String(value, len);
					}
					else {
						WriteUInt16(0);
					}
				}

        void DataOutput::WriteStringWithType( String^ value )
				{
          //value will not be null
					int len = getEncodedLength(value);
          						            
          if (len > 0xffff)
          {
            if(len == value->Length)//huge ascii
            {
              WriteByte(GemfireTypeIds::CacheableASCIIStringHuge);
              WriteASCIIHuge(value);
            }
            else//huge utf
            {
              WriteByte(GemfireTypeIds::CacheableStringHuge);
              WriteUTFHuge(value);
            }
            return;
          }

		      if(len == value->Length)
          {
            WriteByte(GemfireTypeIds::CacheableASCIIString);//ascii string
          }
          else
          {
            WriteByte(GemfireTypeIds::CacheableString);//utf string
          }
          WriteUInt16(len);
					EnsureCapacity(len);
					EncodeUTF8String(value, len);
					
				}

				void DataOutput::WriteASCIIHuge( String^ value )
				{
					if (value != nullptr) {
						const int strLength = value->Length;
						WriteUInt32(strLength);
						EnsureCapacity(strLength);
						for ( int i = 0; i < strLength; i++ ) {
							m_bytes[m_cursor++] = (uint8_t)value[i];
						}
					}
					else {
						WriteUInt32(0);
					}
				}

				/* Write UTF-16 */
				void DataOutput::WriteUTFHuge( String^ value )
				{
					if (value != nullptr) {
						WriteUInt32(value->Length);
						EnsureCapacity(value->Length * 2);
						for( int i = 0; i < value->Length; i++)
						{
							Char ch = value[i];
							m_bytes[m_cursor++] = (Byte)((ch & 0xff00) >> 8);
							m_bytes[m_cursor++] = (Byte)(ch & 0xff);
						}
					}
					else {
						WriteUInt32(0);
					}
				}

				/*void DataOutput::WriteObject( Object^ obj )
				{
					 WriteObjectInternal((IGFSerializable^)obj);
				}*/

				/*void DataOutput::WriteObject( Object^ obj )
				{
					WriteObject( (IGFSerializable^)obj );
				}*/

				int8_t DataOutput::GetTypeId(uint32_t classId )
				{
						if (classId >= 0x80000000) {
							return (int8_t)((classId - 0x80000000) % 0x20000000);
						}
						else if (classId <= 0x7F) {
							return (int8_t)GemfireTypeIdsImpl::CacheableUserData;
						}
						else if (classId <= 0x7FFF) {
							return (int8_t)GemfireTypeIdsImpl::CacheableUserData2;
						}
						else {
							return (int8_t)GemfireTypeIdsImpl::CacheableUserData4;
						}
				}

				int8_t DataOutput::DSFID(uint32_t classId)
				{
					// convention that [0x8000000, 0xa0000000) is for FixedIDDefault,
					// [0xa000000, 0xc0000000) is for FixedIDByte,
					// [0xc0000000, 0xe0000000) is for FixedIDShort
					// and [0xe0000000, 0xffffffff] is for FixedIDInt
					// Note: depends on fact that FixedIDByte is 1, FixedIDShort is 2
					// and FixedIDInt is 3; if this changes then correct this accordingly
					if (classId >= 0x80000000) {
						return (int8_t)((classId - 0x80000000) / 0x20000000);
					}
					return 0;
				}

				void DataOutput::WriteObject(Object^ obj)
				{
					
					if ( obj == nullptr ) 
					{
						WriteByte( (int8_t) GemfireTypeIds::NullObj );
						return;
					}
          
         if(m_ispdxSerialization && obj->GetType()->IsEnum)
         {
           //need to set             
           int enumVal = Internal::PdxHelper::GetEnumValue(obj->GetType()->FullName, Enum::GetName(obj->GetType(), obj), obj->GetHashCode());
           WriteByte(GemFireClassIds::PDX_ENUM); 
           WriteByte(enumVal >> 24);
           WriteArrayLen(enumVal & 0xFFFFFF); 
           return;
         }

					//GemStone::GemFire::Cache::Generic::Log::Debug("DataOutput::WriteObject " + obj);

					Byte typeId = GemStone::GemFire::Cache::Generic::Serializable::GetManagedTypeMappingGeneric(obj->GetType());

					switch(typeId)
					{
					case gemfire::GemfireTypeIds::CacheableByte:
						{
							WriteByte(typeId);
							WriteSByte((SByte)obj);
							return;
						}
					case gemfire::GemfireTypeIds::CacheableBoolean:
						{
							WriteByte(typeId);
							WriteBoolean((bool)obj);
							return;
						}
					case gemfire::GemfireTypeIds::CacheableWideChar:
						{
							WriteByte(typeId);
							WriteObject((Char)obj);
							return;
						}
					case gemfire::GemfireTypeIds::CacheableDouble:
						{
							WriteByte(typeId);
							WriteDouble((Double)obj);
							return;
						}
					case gemfire::GemfireTypeIds::CacheableASCIIString:
						{
							//CacheableString^ cStr = CacheableString::Create((String^)obj);
							////  TODO: igfser mapping between generic and non generic
							//WriteObjectInternal(cStr);
              WriteStringWithType((String^)obj);
							return;
						}
					case gemfire::GemfireTypeIds::CacheableFloat:
						{
							WriteByte(typeId);
							WriteFloat((float)obj);
							return;
						}
					case gemfire::GemfireTypeIds::CacheableInt16:
						{
							WriteByte(typeId);
							WriteInt16((Int16)obj);
							return;
						}
					case gemfire::GemfireTypeIds::CacheableInt32:
						{
							WriteByte(typeId);
							WriteInt32((Int32)obj);
							return;
						}
					case gemfire::GemfireTypeIds::CacheableInt64:
						{
							WriteByte(typeId);
							WriteInt64((Int64)obj);
							return;
						}
					case gemfire::GemfireTypeIds::CacheableDate:
						{
							//CacheableDate^ cd = gcnew CacheableDate((DateTime)obj);
							//  TODO: igfser mapping between generic and non generic
							//WriteObjectInternal(cd);
							WriteByte(typeId);
							WriteDate((DateTime)obj);
							return;
						}
					case gemfire::GemfireTypeIds::CacheableBytes:
						{
							WriteByte(typeId);
							WriteBytes((array<Byte>^)obj);
							return;
						}
					case gemfire::GemfireTypeIds::CacheableDoubleArray:
						{
							WriteByte(typeId);
							WriteObject((array<Double>^)obj);
							return;
						}
					case gemfire::GemfireTypeIds::CacheableFloatArray:
					{
							WriteByte(typeId);
							WriteObject((array<float>^)obj);
							return;
						}
					case gemfire::GemfireTypeIds::CacheableInt16Array:
					{
							WriteByte(typeId);
							WriteObject((array<Int16>^)obj);
							return;
					}
					case gemfire::GemfireTypeIds::CacheableInt32Array:
					{
							WriteByte(typeId);
							WriteObject((array<Int32>^)obj);
							return;
					}
					case gemfire::GemfireTypeIds::CacheableInt64Array:
					{
							WriteByte(typeId);
							WriteObject((array<Int64>^)obj);
							return;
					}
					case gemfire::GemfireTypeIds::BooleanArray:
					{
							WriteByte(typeId);
							WriteObject((array<bool>^)obj);
							return;
					}
					case gemfire::GemfireTypeIds::CharArray:
					{
							WriteByte(typeId);
							WriteObject((array<char>^)obj);
							return;
					}
					case gemfire::GemfireTypeIds::CacheableStringArray:
					{
							WriteByte(typeId);
							WriteObject((array<String^>^)obj);
							return;
					}
					case gemfire::GemfireTypeIds::CacheableHashTable:
					case gemfire::GemfireTypeIds::CacheableHashMap:
					case gemfire::GemfireTypeIds::CacheableIdentityHashMap:
					{
							WriteByte(typeId);
							WriteDictionary((System::Collections::IDictionary^)obj);
							return;
					}
					case gemfire::GemfireTypeIds::CacheableVector:
					{
						//CacheableVector^ cv = gcnew CacheableVector((System::Collections::IList^)obj);
						////  TODO: igfser mapping between generic and non generic
						//WriteObjectInternal(cv);
            WriteByte(gemfire::GemfireTypeIds::CacheableVector);
            WriteList((System::Collections::IList^)obj);
						return;
					}
          case gemfire::GemfireTypeIds::CacheableLinkedList:
					{
						//CacheableArrayList^ cal = gcnew CacheableArrayList((System::Collections::IList^)obj);
						////  TODO: igfser mapping between generic and non generic
						//WriteObjectInternal(cal);
            WriteByte(gemfire::GemfireTypeIds::CacheableLinkedList);
            System::Collections::ICollection^ linkedList = (System::Collections::ICollection^)obj;
            this->WriteArrayLen(linkedList->Count);
            for each (Object^ o in linkedList) 
						  this->WriteObject(o);
						return;
					}
          case gemfire::GemfireTypeIds::CacheableArrayList:
					{
						//CacheableArrayList^ cal = gcnew CacheableArrayList((System::Collections::IList^)obj);
						////  TODO: igfser mapping between generic and non generic
						//WriteObjectInternal(cal);
            WriteByte(gemfire::GemfireTypeIds::CacheableArrayList);
            WriteList((System::Collections::IList^)obj);
						return;
					}
					case gemfire::GemfireTypeIds::CacheableStack:
					{
						CacheableStack^ cs = gcnew CacheableStack((System::Collections::ICollection^)obj);
						//  TODO: igfser mapping between generic and non generic
						WriteObjectInternal(cs);
						return;
					}
					default:
						{
							IPdxSerializable^ pdxObj = dynamic_cast<IPdxSerializable^>(obj);
							if(pdxObj !=  nullptr)
							{
								WriteByte(GemFireClassIds::PDX);
								Internal::PdxHelper::SerializePdx(this, pdxObj);
								return;
							}
							else
							{
                //pdx serialization and is array of object
                if(m_ispdxSerialization && obj->GetType()->IsArray)
                {
                  WriteByte(gemfire::GemfireTypeIds::CacheableObjectArray);
                  WriteDotNetObjectArray(obj);
                  return;
                }

								IGFSerializable^ ct = dynamic_cast<IGFSerializable^>(obj);
								if(ct != nullptr) {
									WriteObjectInternal(ct);
									return ;
								}

                if(Serializable::IsObjectAndPdxSerializerRegistered(nullptr))
                {
                  pdxObj = gcnew PdxWrapper(obj);
                  WriteByte(GemFireClassIds::PDX);
								  Internal::PdxHelper::SerializePdx(this, pdxObj);
								  return;
                }
							}
						
              throw gcnew System::Exception("DataOutput not found appropriate type to write it for object: " + obj->GetType());
						}
					}
				}

				void DataOutput::WriteStringArray(array<String^>^ strArray)
				{
					if(strArray != nullptr)
					{
						this->WriteArrayLen(strArray->Length);
						for(int i = 0; i < strArray->Length; i++)
						{
						 // this->WriteUTF(strArray[i]);
							WriteObject(strArray[i]);
						}
					}
					else
						WriteByte(-1);
				}

				void DataOutput::WriteObjectInternal( IGFSerializable^ obj )
				{
					//CacheableKey^ key = gcnew CacheableKey();
					if ( obj == nullptr ) {
						WriteByte( (int8_t) GemfireTypeIds::NullObj );
					} else {
						int8_t typeId = DataOutput::GetTypeId( obj->ClassId);
						switch (DataOutput::DSFID(obj->ClassId)) {
							case GemfireTypeIdsImpl::FixedIDByte:
								WriteByte((int8_t)GemfireTypeIdsImpl::FixedIDByte);
								WriteByte( typeId ); // write the type ID.
								break;
							case GemfireTypeIdsImpl::FixedIDShort:
								WriteByte((int8_t)GemfireTypeIdsImpl::FixedIDShort);
								WriteInt16( (int16_t)typeId ); // write the type ID.
								break;
							case GemfireTypeIdsImpl::FixedIDInt:
								WriteByte((int8_t)GemfireTypeIdsImpl::FixedIDInt);
								WriteInt32( (int32_t)typeId ); // write the type ID.
								break;
							default:
								WriteByte( typeId ); // write the type ID.
								break;
						}
	          
						if ( (int32_t)typeId == GemfireTypeIdsImpl::CacheableUserData ) {
							WriteByte( (int8_t) obj->ClassId );
						} else if ( (int32_t)typeId == GemfireTypeIdsImpl::CacheableUserData2 ) {
							WriteInt16( (int16_t) obj->ClassId );
						} else if ( (int32_t)typeId == GemfireTypeIdsImpl::CacheableUserData4 ) {
							WriteInt32( (int32_t) obj->ClassId );
						}
						obj->ToData( this ); // let the obj serialize itself.
					}
				}

				void DataOutput::AdvanceCursor( uint32_t offset )
				{
          EnsureCapacity(offset);
					m_cursor += offset;
				}

				void DataOutput::RewindCursor( uint32_t offset )
				{
					//first set native one
					WriteBytesToUMDataOutput();
					NativePtr->rewindCursor(offset);
					SetBuffer();
				}

				array<Byte>^ DataOutput::GetBuffer( )
				{
					WriteBytesToUMDataOutput();
					SetBuffer();
	        
					int buffLen = NativePtr->getBufferLength();
					array<Byte>^ buffer = gcnew array<Byte>( buffLen );

					if ( buffLen > 0 ) {
						pin_ptr<Byte> pin_buffer = &buffer[ 0 ];
						memcpy( (void*)pin_buffer, NativePtr->getBuffer( ), buffLen );
					}
					return buffer;
				}

				uint32_t DataOutput::BufferLength::get( )
				{
					//first set native one
					WriteBytesToUMDataOutput();
					SetBuffer();

					return NativePtr->getBufferLength();
				}

				void DataOutput::Reset( )
				{
					WriteBytesToUMDataOutput();
					NativePtr->reset();
					SetBuffer();
				}
							
				void DataOutput::WriteString(String^ value)
				{
					if(value == nullptr)
					{
						this->WriteByte(GemfireTypeIds::CacheableNullString);
					}
					else
					{
            WriteObject(value);
						/*CacheableString^ cs = gcnew CacheableString(value);

						this->WriteByte( (Byte)(cs->ClassId - 0x80000000));
						cs->ToData(this);*/
					}
				}

				 void DataOutput::WriteBytesToUMDataOutput()
        {
					NativePtr->advanceCursor(m_cursor);
          m_cursor = 0;
          m_remainingBufferLength = 0;
          m_bytes = nullptr;
        }

        void DataOutput::WriteObject(bool% obj)
        {
          WriteBoolean(obj);
        }

        void DataOutput::WriteObject(Byte% obj)
        {
          WriteByte(obj);
        }

        void DataOutput::WriteObject(Char% obj)
        {
          unsigned short us = (unsigned short)obj;
		  EnsureCapacity(2);
          m_bytes[m_cursor++] = us >> 8;
          m_bytes[m_cursor++] = (Byte)us; 
        }

        void DataOutput::WriteObject(Double% obj)
        {
          WriteDouble(obj);
        }

        void DataOutput::WriteObject(Single% obj)
        {
          WriteFloat(obj);
        }

        void DataOutput::WriteObject(int16_t% obj)
        {
          WriteInt16(obj);
        }

        void DataOutput::WriteObject(int32_t% obj)
        {
          WriteInt32(obj);
        }

        void DataOutput::WriteObject(int64_t% obj)
        {
          WriteInt64(obj);
        }

				void DataOutput::WriteObject(UInt16% obj)
        {
          WriteUInt16(obj);
        }

        void DataOutput::WriteObject(UInt32% obj)
        {
          WriteUInt32(obj);
        }

        void DataOutput::WriteObject(UInt64% obj)
        {
          WriteUInt64(obj);
        }

				void DataOutput::WriteBooleanArray(array<bool>^ boolArray)
				{
					WriteObject<bool>(boolArray);
				}

				void DataOutput::WriteShortArray(array<Int16>^ shortArray)
				{
					WriteObject<Int16>(shortArray);
				}

				void DataOutput::WriteIntArray(array<Int32>^ intArray)
				{
					WriteObject<Int32>(intArray);
				}

				void DataOutput::WriteLongArray(array<Int64>^ longArray)
				{
					WriteObject<Int64>(longArray);
				}

			  void DataOutput::WriteFloatArray(array<float>^ floatArray)
				{
					WriteObject<float>(floatArray);
				}

				void DataOutput::WriteDoubleArray(array<double>^ doubleArray)
				{
					WriteObject<double>(doubleArray);
				}
      } // end namespace generic
    }
  }
}
