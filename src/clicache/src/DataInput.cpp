/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "gf_includes.hpp"
#include "DataInput.hpp"
#include <gfcpp/Cache.hpp>
//#include "CacheFactory.hpp"
#include "Cache.hpp"
#include <vcclr.h>
//#include <gfcpp/GemfireTypeIds.hpp>
#include <GemfireTypeIdsImpl.hpp>
#include "CacheableString.hpp"
#include "CacheableHashMap.hpp"
#include "CacheableStack.hpp"
#include "CacheableVector.hpp"
#include "CacheableArrayList.hpp"
#include "CacheableIDentityHashMap.hpp"
#include "CacheableDate.hpp"
#include "CacheableObjectArray.hpp"

#include "Serializable.hpp"
#include "impl/PdxHelper.hpp"

using namespace System;
using namespace System::IO;
using namespace gemfire;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Generic
      {
        DataInput::DataInput( uint8_t* buffer, int size )
        {
          m_ispdxDesrialization = false;
          m_isRootObjectPdx = false;
          if (buffer != nullptr && size > 0) {
						_GF_MG_EXCEPTION_TRY2

							SetPtr(new gemfire::DataInput(buffer, size), true);
              m_cursor = 0;
              m_isManagedObject = false;
              m_forStringDecode = gcnew array<Char>(100);

							m_buffer = const_cast<uint8_t*>(NativePtr->currentBufferPosition());
							m_bufferLength = NativePtr->getBytesRemaining();    

						_GF_MG_EXCEPTION_CATCH_ALL2          
					}
					else {
						throw gcnew IllegalArgumentException("DataInput.ctor(): "
							"provided buffer is null or empty");
					}
        }

				DataInput::DataInput(array<Byte>^ buffer)
				{
          m_ispdxDesrialization = false;
          m_isRootObjectPdx = false;
					if (buffer != nullptr && buffer->Length > 0) {
						_GF_MG_EXCEPTION_TRY2

							int32_t len = buffer->Length;
							GF_NEW(m_buffer, uint8_t[len]);
							pin_ptr<const Byte> pin_buffer = &buffer[0];
							memcpy(m_buffer, (void*)pin_buffer, len);
							SetPtr(new gemfire::DataInput(m_buffer, len), true);

              m_cursor = 0;
              m_isManagedObject = false;
              m_forStringDecode = gcnew array<Char>(100);

							m_buffer = const_cast<uint8_t*>(NativePtr->currentBufferPosition());
							m_bufferLength = NativePtr->getBytesRemaining();    

						_GF_MG_EXCEPTION_CATCH_ALL2          
					}
					else {
						throw gcnew IllegalArgumentException("DataInput.ctor(): "
							"provided buffer is null or empty");
					}
				}

				DataInput::DataInput(array<Byte>^ buffer, int32_t len )
				{
          m_ispdxDesrialization = false;
          m_isRootObjectPdx = false;
					if (buffer != nullptr) {
						if (len == 0 || (int32_t)len > buffer->Length) {
							throw gcnew IllegalArgumentException(String::Format(
								"DataInput.ctor(): given length {0} is zero or greater than "
								"size of buffer {1}", len, buffer->Length));
						}
						//m_bytes = gcnew array<Byte>(len);
						//System::Array::Copy(buffer, 0, m_bytes, 0, len);
					 _GF_MG_EXCEPTION_TRY2

							GF_NEW(m_buffer, uint8_t[len]);
							pin_ptr<const Byte> pin_buffer = &buffer[0];
							memcpy(m_buffer, (void*)pin_buffer, len);
							SetPtr(new gemfire::DataInput(m_buffer, len), true);

							m_buffer = const_cast<uint8_t*>(NativePtr->currentBufferPosition());
							m_bufferLength = NativePtr->getBytesRemaining();    

						_GF_MG_EXCEPTION_CATCH_ALL2
					}
					else {
						throw gcnew IllegalArgumentException("DataInput.ctor(): "
							"provided buffer is null");
					}
				}

        void DataInput::CheckBufferSize(int size)
        {          
          if( (unsigned int)(m_cursor + size) > m_bufferLength )
          {
            Log::Debug("DataInput::CheckBufferSize m_cursor:" + m_cursor + " size:" + size + " m_bufferLength:" + m_bufferLength);
            throw gcnew OutOfRangeException("DataInput: attempt to read beyond buffer");
          }
        }

        DataInput^ DataInput::GetClone()
        {
          return gcnew DataInput(m_buffer, m_bufferLength);
        }

				Byte DataInput::ReadByte( )
				{
					CheckBufferSize(1);
					return m_buffer[m_cursor++];
				}

				SByte DataInput::ReadSByte( )
				{
					CheckBufferSize(1);
					return m_buffer[m_cursor++];
				}

				bool DataInput::ReadBoolean( )
				{
					CheckBufferSize(1);
					Byte val = m_buffer[m_cursor++];
					if ( val == 1)
						return true;
					else
						return false;
				}

				Char DataInput::ReadChar( )
				{
					CheckBufferSize(2);
					Char data = m_buffer[m_cursor++];
					data = (data << 8) | m_buffer[m_cursor++];
					return data;
				}

				array<Byte>^ DataInput::ReadBytes( )
				{
					int32_t length;
					length = ReadArrayLen();

					if (length >= 0) {
						if (length == 0)
							return gcnew array<Byte>(0);
						else {
							array<Byte>^ bytes = ReadBytesOnly(length);
							return bytes;
						}
					}
					return nullptr;
				}

				int DataInput::ReadArrayLen( )
				{
					int code;
					int len;
	        
					code = Convert::ToInt32(ReadByte());

					if (code == 0xFF) {
						len = -1;
					} else {
						unsigned int result = code;
						if (result > 252) {  // 252 is java's ((byte)-4 && 0xFF)
							if (code == 0xFE) {
								result = ReadUInt16();
							} else if (code == 0xFD) {
								result = ReadUInt32();
							}
							else {
								throw gcnew IllegalStateException("unexpected array length code");
							}
							//TODO:: illegal length
						}
						len = (int)result;
					}
					return len;
				}

				array<SByte>^ DataInput::ReadSBytes( )
				{
					int32_t length;
					length = ReadArrayLen();

					if (length > -1) {
						if (length == 0)
							return gcnew array<SByte>(0);
						else {
							array<SByte>^ bytes = ReadSBytesOnly(length);
							return bytes;
						}
					}
					return nullptr;
				}

				array<Byte>^ DataInput::ReadBytesOnly( uint32_t len )
				{
					if (len > 0) {
						CheckBufferSize(len);
						array<Byte>^ bytes = gcnew array<Byte>(len);
	          
						for ( unsigned int i = 0; i < len; i++)
							bytes[i] = m_buffer[m_cursor++];

						return bytes;
					}
					return nullptr;
				}

				void DataInput::ReadBytesOnly( array<Byte> ^ buffer, int offset, int count )
				{
					if (count > 0) {
						CheckBufferSize((uint32_t)count);
	          
						for ( int i = 0; i < count; i++)
							buffer[offset + i] = m_buffer[m_cursor++];
					}
				}

				array<SByte>^ DataInput::ReadSBytesOnly( uint32_t len )
				{
					if (len > 0) {
						CheckBufferSize(len);
						array<SByte>^ bytes = gcnew array<SByte>(len);

						for ( unsigned int i = 0; i < len; i++)
							bytes[i] = (SByte)m_buffer[m_cursor++];

						return bytes;
					}
					return nullptr;
				}

				uint16_t DataInput::ReadUInt16( )
				{
					CheckBufferSize(2);
					uint16_t data = m_buffer[m_cursor++];
					data = (data << 8) | m_buffer[m_cursor++];
					return data;
				}

				uint32_t DataInput::ReadUInt32( )
				{
					CheckBufferSize(4);
					uint32_t data = m_buffer[m_cursor++];
					data = (data << 8) | m_buffer[m_cursor++];
					data = (data << 8) | m_buffer[m_cursor++];
					data = (data << 8) | m_buffer[m_cursor++];
	        
					return data;
				}

				uint64_t DataInput::ReadUInt64( )
				{
					uint64_t data;
	        
					CheckBufferSize(8);
	        
					data = m_buffer[m_cursor++];
					data = (data << 8) | m_buffer[m_cursor++];
					data = (data << 8) | m_buffer[m_cursor++];
					data = (data << 8) | m_buffer[m_cursor++];
					data = (data << 8) | m_buffer[m_cursor++];
					data = (data << 8) | m_buffer[m_cursor++];
					data = (data << 8) | m_buffer[m_cursor++];
					data = (data << 8) | m_buffer[m_cursor++];
	        
					return data;
				}

				int16_t DataInput::ReadInt16( )
				{
					return ReadUInt16();
				}

				int32_t DataInput::ReadInt32( )
				{
					return ReadUInt32();
				}

				int64_t DataInput::ReadInt64( )
				{
					return ReadUInt64();
				}

				array<Byte>^ DataInput::ReadReverseBytesOnly(int len)
				{
					CheckBufferSize(len);

					int i = 0;
					int j = m_cursor + len -1;
					array<Byte>^ bytes = gcnew array<Byte>(len);

					while ( i < len )
					{
						bytes[i++] = m_buffer[j--];
					}
					m_cursor += len;
					return bytes;
				}

				float DataInput::ReadFloat( )
				{
					float data;

					array<Byte>^ bytes = nullptr;          
					if(BitConverter::IsLittleEndian)
						bytes = ReadReverseBytesOnly(4);
					else
						bytes = ReadBytesOnly(4);

					data = BitConverter::ToSingle(bytes, 0);
	        
					return data;
				}

				double DataInput::ReadDouble( )
				{
					double data;

					array<Byte>^ bytes = nullptr;          
					if(BitConverter::IsLittleEndian)
						bytes = ReadReverseBytesOnly(8);
					else
						bytes = ReadBytesOnly(8);

					data = BitConverter::ToDouble(bytes, 0);
	        
					return data;
				}

				String^ DataInput::ReadUTF( )
				{
					int length = ReadUInt16();
					CheckBufferSize(length);
					String^ str = DecodeBytes(length);
					return str;
				}

				String^ DataInput::ReadUTFHuge( )
				{
					int length = ReadUInt32();
					CheckBufferSize(length);
	        
					array<Char>^ chArray = gcnew array<Char>(length);
	        
					for (int i = 0; i < length; i++)
					{
						Char ch = ReadByte();
						ch = ((ch << 8) | ReadByte());
						chArray[i] = ch;
					}

					String^ str = gcnew String(chArray);

					return str;
				}

				String^ DataInput::ReadASCIIHuge( )
				{
					int length = ReadInt32();
					CheckBufferSize(length);
					String^ str = DecodeBytes(length);
					return str;
				}

				Object^ DataInput::ReadObject( )
				{
					return ReadInternalObject();        
				}

			/*	Object^ DataInput::ReadGenericObject( )
				{
					return ReadInternalGenericObject();        
				}*/

				Object^ DataInput::ReadDotNetTypes(int8_t typeId)
				{
					switch(typeId)
					{
					case gemfire::GemfireTypeIds::CacheableByte:
						{
							return ReadSByte();
						}
					case gemfire::GemfireTypeIds::CacheableBoolean:
						{
							bool obj;
							ReadObject(obj);
							return obj;
						}
					case gemfire::GemfireTypeIds::CacheableWideChar:
						{
							Char obj;
							ReadObject(obj);
							return obj;
						}
					case gemfire::GemfireTypeIds::CacheableDouble:
						{
							Double obj;
							ReadObject(obj);
							return obj;
						}
					case gemfire::GemfireTypeIds::CacheableASCIIString:
						{
						/*	CacheableString^ cs = static_cast<CacheableString^>(CacheableString::CreateDeserializable());
							cs->FromData(this);
							return cs->Value;*/
              return ReadUTF();
						}
					case gemfire::GemfireTypeIds::CacheableASCIIStringHuge:
						{
							/*CacheableString^ cs = static_cast<CacheableString^>(CacheableString::createDeserializableHuge());
							cs->FromData(this);
							return cs->Value;*/
              return ReadASCIIHuge();
						}
					case gemfire::GemfireTypeIds::CacheableString:
						{
							/*CacheableString^ cs = static_cast<CacheableString^>(CacheableString::createUTFDeserializable());
							cs->FromData(this);
							return cs->Value;*/
              return ReadUTF();
						}
					case gemfire::GemfireTypeIds::CacheableStringHuge:
						{
							//TODO: need to look all strings types
							/*CacheableString^ cs = static_cast<CacheableString^>(CacheableString::createUTFDeserializableHuge());
							cs->FromData(this);
							return cs->Value;*/
              return ReadUTFHuge();
						}
					case gemfire::GemfireTypeIds::CacheableFloat:
						{
							float obj;
							ReadObject(obj);
							return obj;
						}
					case gemfire::GemfireTypeIds::CacheableInt16:
						{
							Int16 obj;
							ReadObject(obj);
							return obj;
						}
					case gemfire::GemfireTypeIds::CacheableInt32:
						{
							Int32 obj;
							ReadObject(obj);
							return obj;
						}
					case gemfire::GemfireTypeIds::CacheableInt64:
						{
							Int64 obj;
							ReadObject(obj);
							return obj;
						}
					case gemfire::GemfireTypeIds::CacheableDate:
						{
							CacheableDate^ cd = CacheableDate::Create();
							cd->FromData(this);
							return cd->Value;
						}
					case gemfire::GemfireTypeIds::CacheableBytes:
						{
							return ReadBytes();
						}
					case gemfire::GemfireTypeIds::CacheableDoubleArray:
						{
							array<Double>^ obj;
							ReadObject(obj);
							return obj;
						}
					case gemfire::GemfireTypeIds::CacheableFloatArray:
						{
							array<float>^ obj;
							ReadObject(obj);
							return obj;
						}
					case gemfire::GemfireTypeIds::CacheableInt16Array:
						{
							array<Int16>^ obj;
							ReadObject(obj);
							return obj;
						}
					case gemfire::GemfireTypeIds::CacheableInt32Array:
						{
							array<Int32>^ obj;
							ReadObject(obj);
							return obj;
						}
					case gemfire::GemfireTypeIds::BooleanArray:
						{
							array<bool>^ obj;
							ReadObject(obj);
							return obj;
						}
					case gemfire::GemfireTypeIds::CharArray:
						{
							array<Char>^ obj;
							ReadObject(obj);
							return obj;
						}
					case gemfire::GemfireTypeIds::CacheableInt64Array:
						{
							array<Int64>^ obj;
							ReadObject(obj);
							return obj;
						}
					case gemfire::GemfireTypeIds::CacheableStringArray:
						{
							return ReadStringArray();
						}
					case gemfire::GemfireTypeIds::CacheableHashTable:
						{
							return ReadHashtable();
						}
					case gemfire::GemfireTypeIds::CacheableHashMap:
						{
							CacheableHashMap^ chm = static_cast<CacheableHashMap^>(CacheableHashMap::CreateDeserializable());
							chm->FromData(this);
							return chm->Value;
						}
					case gemfire::GemfireTypeIds::CacheableIdentityHashMap:
						{
							CacheableIdentityHashMap^ chm = static_cast<CacheableIdentityHashMap^>(CacheableIdentityHashMap::CreateDeserializable());
							chm->FromData(this);
							return chm->Value;
						}
					case gemfire::GemfireTypeIds::CacheableVector:
						{
							/*CacheableVector^ cv = static_cast<CacheableVector^>(CacheableVector::CreateDeserializable());
							cv->FromData(this);
							return cv->Value;*/
              int len = ReadArrayLen();
              System::Collections::ArrayList^ retA = gcnew System::Collections::ArrayList(len);
              
              for( int i = 0; i < len; i++)
              {
                retA->Add(this->ReadObject());
              }
              return retA;
						}
					case gemfire::GemfireTypeIds::CacheableArrayList:
						{
							/*CacheableArrayList^ cv = static_cast<CacheableArrayList^>(CacheableArrayList::CreateDeserializable());
							cv->FromData(this);
							return cv->Value;*/
              int len = ReadArrayLen();
              System::Collections::Generic::List<Object^>^ retA = gcnew System::Collections::Generic::List<Object^>(len);
              for( int i = 0; i < len; i++)
              {
                retA->Add(this->ReadObject());
              }
              return retA;

						}
          case gemfire::GemfireTypeIds::CacheableLinkedList:
						{
							/*CacheableArrayList^ cv = static_cast<CacheableArrayList^>(CacheableArrayList::CreateDeserializable());
							cv->FromData(this);
							return cv->Value;*/
              int len = ReadArrayLen();
              System::Collections::Generic::LinkedList<Object^>^ retA = gcnew System::Collections::Generic::LinkedList<Object^>();
              for( int i = 0; i < len; i++)
              {
                retA->AddLast(this->ReadObject());
              }
              return retA;

						}
					case gemfire::GemfireTypeIds::CacheableStack:
						{
							CacheableStack^ cv = static_cast<CacheableStack^>(CacheableStack::CreateDeserializable());
							cv->FromData(this);
							return cv->Value;
						}
					default:
						return nullptr;
					}
				}

				Object^ DataInput::ReadInternalObject( )
				{
          //Log::Debug("DataInput::ReadInternalObject m_cursor " + m_cursor);
					bool findinternal = false;
					int8_t typeId = ReadByte();
					int64_t compId = typeId;
					TypeFactoryMethodGeneric^ createType = nullptr;

					if (compId == GemfireTypeIds::NullObj) {
						return nullptr;
					}
					else if(compId == GemFireClassIds::PDX)
					{
            //cache current state and reset after reading pdx object
            int cacheCursor = m_cursor;
            uint8_t* cacheBuffer = m_buffer;
            unsigned int cacheBufferLength = m_bufferLength;
            Object^ ret = Internal::PdxHelper::DeserializePdx(this, false);
            int tmp = NativePtr->getBytesRemaining();
            m_cursor = cacheBufferLength - tmp;
            m_buffer = cacheBuffer;
            m_bufferLength = cacheBufferLength;
            NativePtr->rewindCursor(m_cursor);

            if(ret != nullptr)
            {
              PdxWrapper^ pdxWrapper = dynamic_cast<PdxWrapper^>(ret);

              if(pdxWrapper != nullptr)
              {
                return pdxWrapper->GetObject();
              }
            }
            return ret;
					}
          else if (compId == GemFireClassIds::PDX_ENUM)
          {
            int8_t dsId = ReadByte();
            int tmp = ReadArrayLen();
            int enumId = (dsId << 24) | (tmp & 0xFFFFFF);

            Object^ enumVal = Internal::PdxHelper::GetEnum(enumId);
            return enumVal;
          }
					else if (compId == GemfireTypeIds::CacheableNullString) {
						//return SerializablePtr(CacheableString::createDeserializable());
						//TODO::
						return nullptr;
					}
					else if (compId == GemfireTypeIdsImpl::CacheableUserData) {
						int8_t classId = ReadByte();
						//compId |= ( ( (int64_t)classId ) << 32 );
						compId = (int64_t)classId;
					} else if ( compId == GemfireTypeIdsImpl::CacheableUserData2 ) {
						int16_t classId = ReadInt16();
						//compId |= ( ( (int64_t)classId ) << 32 );
						compId = (int64_t)classId;
					} else if ( compId == GemfireTypeIdsImpl::CacheableUserData4 ) {
						int32_t classId = ReadInt32();
						//compId |= ( ( (int64_t)classId ) << 32 );
						compId = (int64_t)classId;
					}else if (compId == GemfireTypeIdsImpl::FixedIDByte) {//TODO: need to verify again
						int8_t fixedId = ReadByte();
						compId = fixedId;
						findinternal = true;
					} else if (compId == GemfireTypeIdsImpl::FixedIDShort) {
						int16_t fixedId = ReadInt16();
						compId = fixedId;
						findinternal = true;
					} else if (compId == GemfireTypeIdsImpl::FixedIDInt) {
						int32_t fixedId = ReadInt32();
						compId = fixedId;
						findinternal = true;
					}
					if (findinternal) {
						compId += 0x80000000;
						createType = Serializable::GetManagedDelegateGeneric((int64_t)compId);
					} else {
							createType = Serializable::GetManagedDelegateGeneric(compId);
							if(createType == nullptr)
							{
								 Object^ retVal = ReadDotNetTypes(typeId);

								if(retVal != nullptr)
									return retVal;

                if(m_ispdxDesrialization && typeId == gemfire::GemfireTypeIds::CacheableObjectArray)
                {//object array and pdxSerialization
                  return readDotNetObjectArray();
                }
								compId += 0x80000000;
								createType = Serializable::GetManagedDelegateGeneric(compId);

								/*if (createType == nullptr)
								{
									//TODO:: final check for user type if its not in cache 
									compId -= 0x80000000;
									createType = Serializable::GetManagedDelegate(compId);
								}*/
							}
					}
          
					if ( createType == nullptr ) {            
						throw gcnew IllegalStateException( "Unregistered typeId " +typeId + " in deserialization, aborting." );
					}

          bool isPdxDeserialization = m_ispdxDesrialization;
          m_ispdxDesrialization = false;//for nested objects
					IGFSerializable^ newObj = createType();
					newObj->FromData(this);
          m_ispdxDesrialization = isPdxDeserialization;
					return newObj;
				}

        Object^ DataInput::readDotNetObjectArray()
        {
           int len = ReadArrayLen();
           String^ className = nullptr;
           if (len >= 0) 
           {
              ReadByte(); // ignore CLASS typeid
              className = (String^)ReadObject();
              className = Serializable::GetLocalTypeName(className);
              System::Collections::IList^ list = nullptr;
              if(len == 0)
              {
                list = (System::Collections::IList^)Serializable::GetArrayObject(className, len);
                return list;
              }
              //read first object

              Object^ ret = ReadObject();//in case it returns pdxinstance or java.lang.object

              list = (System::Collections::IList^)Serializable::GetArrayObject(ret->GetType()->FullName, len);
              
              list[0] = ret;
              for (int32_t index = 1; index < list->Count; ++index) 
              {
                list[index] = ReadObject();
              }
              return list;
           }
           return nullptr;
         }

				Object^ DataInput::ReadInternalGenericObject( )
				{
					bool findinternal = false;
					int8_t typeId = ReadByte();
					int64_t compId = typeId;
					TypeFactoryMethodGeneric^ createType = nullptr;

					if (compId == GemfireTypeIds::NullObj) {
						return nullptr;
					}
					else if(compId == GemFireClassIds::PDX)
					{
						return Internal::PdxHelper::DeserializePdx(this, false);
					}
					else if (compId == GemfireTypeIds::CacheableNullString) {
						//return SerializablePtr(CacheableString::createDeserializable());
						//TODO::
						return nullptr;
					}
					else if (compId == GemfireTypeIdsImpl::CacheableUserData) {
						int8_t classId = ReadByte();
						//compId |= ( ( (int64_t)classId ) << 32 );
						compId = (int64_t)classId;
					} else if ( compId == GemfireTypeIdsImpl::CacheableUserData2 ) {
						int16_t classId = ReadInt16();
						//compId |= ( ( (int64_t)classId ) << 32 );
						compId = (int64_t)classId;
					} else if ( compId == GemfireTypeIdsImpl::CacheableUserData4 ) {
						int32_t classId = ReadInt32();
						//compId |= ( ( (int64_t)classId ) << 32 );
						compId = (int64_t)classId;
					}else if (compId == GemfireTypeIdsImpl::FixedIDByte) {//TODO: need to verify again
						int8_t fixedId = ReadByte();
						compId = fixedId;
						findinternal = true;
					} else if (compId == GemfireTypeIdsImpl::FixedIDShort) {
						int16_t fixedId = ReadInt16();
						compId = fixedId;
						findinternal = true;
					} else if (compId == GemfireTypeIdsImpl::FixedIDInt) {
						int32_t fixedId = ReadInt32();
						compId = fixedId;
						findinternal = true;
					}
					if (findinternal) {
						compId += 0x80000000;
						createType = Serializable::GetManagedDelegateGeneric((int64_t)compId);
					} else {
							createType = Serializable::GetManagedDelegateGeneric(compId);
							if(createType == nullptr)
							{
								Object^ retVal = ReadDotNetTypes(typeId);

								if(retVal != nullptr)
									return retVal;

								compId += 0x80000000;
								createType = Serializable::GetManagedDelegateGeneric(compId);              
							}
					}

					if ( createType != nullptr )
					{
						IGFSerializable^ newObj = createType();
						newObj->FromData(this);
						return newObj;
					}
	        
					throw gcnew IllegalStateException( "Unregistered typeId in deserialization, aborting." );
				}

				uint32_t DataInput::BytesRead::get( )
				{
					AdvanceUMCursor();
					SetBuffer();

					return NativePtr->getBytesRead();
				}

				uint32_t DataInput::BytesReadInternally::get()
				{
					return m_cursor;
				}

				uint32_t DataInput::BytesRemaining::get( )
				{
					AdvanceUMCursor();
					SetBuffer();
					return NativePtr->getBytesRemaining();
					//return m_bufferLength - m_cursor;
				}

				void DataInput::AdvanceCursor( int32_t offset )
				{
					m_cursor += offset;
				}

				void DataInput::RewindCursor( int32_t offset )
				{
					AdvanceUMCursor();        
					NativePtr->rewindCursor(offset);
					SetBuffer();
					//m_cursor -= offset;
				}

				void DataInput::Reset()
				{
					AdvanceUMCursor();
					NativePtr->reset();
					SetBuffer();
				//  m_cursor = 0;
				}

				void DataInput::Cleanup( )
				{
					//TODO:
					//GF_SAFE_DELETE_ARRAY(m_buffer);
					InternalCleanup( );
				}

        void DataInput::ReadDictionary(System::Collections::IDictionary^ dict)
        {
          int len = this->ReadArrayLen();

          if(len > 0)
					{
						for(int i =0; i< len; i++)
						{
							Object^ key = this->ReadObject();
							Object^ val = this->ReadObject();

							dict->Add(key, val);
						}
					}
        }

				IDictionary<Object^, Object^>^ DataInput::ReadDictionary()
				{
					int len = this->ReadArrayLen();

					if(len == -1)
						return nullptr;
					else
					{
						IDictionary<Object^, Object^>^ dict = gcnew Dictionary<Object^, Object^>();
						for(int i =0; i< len; i++)
						{
							Object^ key = this->ReadObject();
							Object^ val = this->ReadObject();

							dict->Add(key, val);
						}
						return dict;
					}        
				}

				System::DateTime DataInput::ReadDate()
				{
					long ticks = (long)ReadInt64();
					if(ticks != -1L)
					{
						m_cursor -= 8;//for above
						CacheableDate^ cd = CacheableDate::Create();
						cd->FromData(this);
						return cd->Value;
					}
					else
					{
						DateTime dt(0);
						return dt;
					}
				}

				void DataInput::ReadCollection(System::Collections::IList^ coll)
				{
					int len = ReadArrayLen();
					for( int i = 0; i < len; i++)
					{
						coll->Add(ReadObject());
					}
				}
	      
				array<Char>^ DataInput::ReadCharArray( )
				{
					array<Char>^ arr;
          this->ReadObject(arr);
					return arr;
				}

				array<bool>^ DataInput::ReadBooleanArray( )
				{
					array<bool>^ arr;
          this->ReadObject(arr);
					return arr;
				}

				array<Int16>^ DataInput::ReadShortArray( )
				{
					array<Int16>^ arr;
          this->ReadObject(arr);
					return arr;
				}

				array<Int32>^ DataInput::ReadIntArray()
				{
					array<Int32>^ arr;
          this->ReadObject(arr);
					return arr;
				}

				array<Int64>^ DataInput::ReadLongArray()
				{
					array<Int64>^ arr;
          this->ReadObject(arr);
					return arr;
				}

				array<float>^ DataInput::ReadFloatArray()
				{
					array<float>^ arr;
          this->ReadObject(arr);
					return arr;
				}

				array<double>^ DataInput::ReadDoubleArray()
				{
					array<double>^ arr;
          this->ReadObject(arr);
					return arr;
				}

				List<Object^>^ DataInput::ReadObjectArray()
				{
          //this to know whether it is null or it is empty
          int storeCursor = m_cursor;
          int len = this->ReadArrayLen();
          if(len == -1)
            return nullptr;
          //this will be read further by fromdata
          m_cursor = m_cursor - (m_cursor - storeCursor);
          

					CacheableObjectArray^ coa = CacheableObjectArray::Create();
					coa->FromData(this);        
					List<Object^>^ retObj = (List<Object^>^)coa;

          if(retObj->Count >= 0)
            return retObj;
          return nullptr;
				}

				array<array<Byte>^>^ DataInput::ReadArrayOfByteArrays( )
				{
					int len = ReadArrayLen();
					if(len >= 0)
					{
						array<array<Byte>^>^ retVal = gcnew array<array<Byte>^>(len);
						for( int i = 0; i < len; i++)
						{
							retVal[i] = this->ReadBytes();
						}
						return retVal;
					}
					else
						return nullptr;
				}

				void DataInput::ReadObject(array<UInt16>^% obj)
				{
					int len = ReadArrayLen();
					if(len >= 0)
					{
						obj = gcnew array<UInt16>(len);
						for( int i = 0; i < len; i++)
						{
							obj[i] = this->ReadUInt16();
						}
					}
				}

				void DataInput::ReadObject(array<UInt32>^% obj)
				{
					int len = ReadArrayLen();
					if(len >= 0)
					{
						obj = gcnew array<UInt32>(len);
						for( int i = 0; i < len; i++)
						{
							obj[i] = this->ReadUInt32();
						}
					}
				}

				void DataInput::ReadObject(array<UInt64>^% obj)
				{
					int len = ReadArrayLen();
					if(len >= 0)
					{
						obj = gcnew array<UInt64>(len);
						for( int i = 0; i < len; i++)
						{
							obj[i] = this->ReadUInt64();
						}
					}
				}

				String^ DataInput::ReadString()
				{
					UInt32 typeId = (Int32)ReadByte() ;

					if(typeId == GemfireTypeIds::CacheableNullString)
						return nullptr;

				  if (typeId == GemfireTypeIds::CacheableASCIIString ||
              typeId == GemfireTypeIds::CacheableString)
          {
            return ReadUTF();
          }
          else if (typeId == GemfireTypeIds::CacheableASCIIStringHuge)
          {
            return ReadASCIIHuge();
          }
          else 
          {
            return ReadUTFHuge();
          }
				}
      } // end namespace generic
    }
  }
}
