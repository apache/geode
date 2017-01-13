/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "../gf_includes.hpp"
#include "ManagedCacheableKeyBytes.hpp"
#include "../DataInput.hpp"
#include "../DataOutput.hpp"
#include "../Serializable.hpp"
#include "../CacheableString.hpp"
#include <GemfireTypeIdsImpl.hpp>
#include "../ExceptionTypes.hpp"
#include "ManagedString.hpp"


using namespace System;

namespace gemfire
{
  void ManagedCacheableKeyBytesGeneric::toData( gemfire::DataOutput& output ) const
  {
    GemStone::GemFire::Cache::Generic::Log::Debug("ManagedCacheableKeyBytesGeneric::toData: current domain ID: " + System::Threading::Thread::GetDomainID() + " for object: " + System::Convert::ToString((int)this) + " with its domain ID: " + m_domainId );
    try {
      //TODO: I think this should work as it is
      output.writeBytesOnly(m_bytes, m_size);
    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
  }

  gemfire::Serializable* ManagedCacheableKeyBytesGeneric::fromData( gemfire::DataInput& input )
  {
    try {
      
       GemStone::GemFire::Cache::Generic::Log::Debug("ManagedCacheableKeyBytesGeneric::fromData: classid " + m_classId + "aid = " + + System::Threading::Thread::GetDomainID() );
       GemStone::GemFire::Cache::Generic::DataInput mg_input( &input, true );
       const uint8_t* objStartPos = input.currentBufferPosition();
         
       GemStone::GemFire::Cache::Generic::IGFSerializable^ obj = GemStone::GemFire::Cache::Generic::Serializable::GetTypeFactoryMethodGeneric(m_classId)();
       obj->FromData(%mg_input);
    
       input.advanceCursor(mg_input.BytesReadInternally);

       m_hashCode = obj->GetHashCode();

       const uint8_t* objEndPos = input.currentBufferPosition();
       
       //m_size = mg_input.BytesRead;
       m_size = (uint32_t)(objEndPos - objStartPos);
       GemStone::GemFire::Cache::Generic::Log::Debug("ManagedCacheableKeyBytesGeneric::fromData: objectSize = " + m_size + " m_hashCode = " + m_hashCode);
       m_bytes = input.getBufferCopyFrom(objStartPos, m_size);
       
    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
    return this;
  }

  uint32_t ManagedCacheableKeyBytesGeneric::objectSize( ) const
  {
    try {
      //return m_managedptr->ObjectSize;
      return m_size;
    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
    return 0;
  }

  int32_t ManagedCacheableKeyBytesGeneric::classId() const
  {
    uint32_t classId;
    try {
      //classId = m_managedptr->ClassId;
      classId = m_classId;
    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
    return (classId >= 0x80000000 ? 0 : classId);
  }

  int8_t ManagedCacheableKeyBytesGeneric::typeId() const
  {
    try {
      //uint32_t classId = m_managedptr->ClassId;
      uint32_t classId = m_classId;
      if (classId >= 0x80000000) {
        return (int8_t)((classId - 0x80000000) % 0x20000000);
      }
      else if (classId <= 0x7F) {
        return (int8_t)gemfire::GemfireTypeIdsImpl::CacheableUserData;
      }
      else if (classId <= 0x7FFF) {
        return (int8_t)gemfire::GemfireTypeIdsImpl::CacheableUserData2;
      }
      else {
        return (int8_t)gemfire::GemfireTypeIdsImpl::CacheableUserData4;
      }
    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
    return 0;
  }

  int8_t ManagedCacheableKeyBytesGeneric::DSFID() const
  {
    // convention that [0x8000000, 0xa0000000) is for FixedIDDefault,
    // [0xa000000, 0xc0000000) is for FixedIDByte,
    // [0xc0000000, 0xe0000000) is for FixedIDShort
    // and [0xe0000000, 0xffffffff] is for FixedIDInt
    // Note: depends on fact that FixedIDByte is 1, FixedIDShort is 2
    // and FixedIDInt is 3; if this changes then correct this accordingly
    //uint32_t classId = m_managedptr->ClassId;
    uint32_t classId = m_classId;
    if (classId >= 0x80000000) {
      return (int8_t)((classId - 0x80000000) / 0x20000000);
    }
    return 0;
  }

  gemfire::CacheableStringPtr ManagedCacheableKeyBytesGeneric::toString( ) const
  {
    try {
      GemStone::GemFire::Cache::Generic::IGFSerializable^ manageObject = getManagedObject();
      if(manageObject != nullptr)
      {
	      gemfire::CacheableStringPtr cStr;
				GemStone::GemFire::Cache::Generic::CacheableString::GetCacheableString(
          manageObject->ToString(), cStr );
        return cStr;
      }
    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
    return NULLPTR;
  }

  bool ManagedCacheableKeyBytesGeneric::operator ==(const gemfire::CacheableKey& other) const
  {
    try {
      GemStone::GemFire::Cache::Generic::Log::Debug("ManagedCacheableKeyBytesGeneric::equal");
      // now checking classId(), typeId(), DSFID() etc. will be much more
      // expensive than just a dynamic_cast
      const ManagedCacheableKeyBytesGeneric* p_other =
        dynamic_cast<const ManagedCacheableKeyBytesGeneric*>(&other);
      if (p_other != NULL) {
       gemfire::DataInput di(m_bytes, m_size);
        GemStone::GemFire::Cache::Generic::DataInput mg_input(&di, true);
        GemStone::GemFire::Cache::Generic::IGFSerializable^ obj =
          GemStone::GemFire::Cache::Generic::Serializable::GetTypeFactoryMethodGeneric(m_classId)();
        obj->FromData(%mg_input);
        bool ret = obj->Equals(p_other->ptr());
        GemStone::GemFire::Cache::Generic::Log::Debug("ManagedCacheableKeyBytesGeneric::equal return VAL = " + ret);
        return ret;
      }
    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
    GemStone::GemFire::Cache::Generic::Log::Debug("ManagedCacheableKeyBytesGeneric::equal returns false");
    return false;
  }

  bool ManagedCacheableKeyBytesGeneric::operator ==(const ManagedCacheableKeyBytesGeneric& other) const
  {
    try {
        GemStone::GemFire::Cache::Generic::Log::Debug("ManagedCacheableKeyBytesGeneric::equal. ");
        gemfire::DataInput di(m_bytes, m_size);
        GemStone::GemFire::Cache::Generic::DataInput mg_input(&di, true);
        GemStone::GemFire::Cache::Generic::IGFSerializable^ obj =
          GemStone::GemFire::Cache::Generic::Serializable::GetTypeFactoryMethodGeneric(m_classId)();
        obj->FromData(%mg_input);
        bool ret = obj->Equals(other.ptr());
        GemStone::GemFire::Cache::Generic::Log::Debug("ManagedCacheableKeyBytesGeneric::equal return VAL = " + ret);
        return ret;
        //return obj->Equals(other.ptr());
    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
    GemStone::GemFire::Cache::Generic::Log::Debug("ManagedCacheableKeyBytesGeneric::equal return false");
    return false;
  }

  uint32_t ManagedCacheableKeyBytesGeneric::hashcode( ) const
  {
    return m_hashCode;
  }

  size_t ManagedCacheableKeyBytesGeneric::logString( char* buffer, size_t maxLength ) const
  {
	  try {
       GemStone::GemFire::Cache::Generic::IGFSerializable^ manageObject = getManagedObject();
      if(manageObject != nullptr)
      {
        if ( maxLength > 0 ) {
          String^ logstr = manageObject->GetType( )->Name + '(' +
            manageObject->ToString( ) + ')';
					GemStone::GemFire::Cache::Generic::ManagedString mg_str( logstr );
          return snprintf( buffer, maxLength, "%s", mg_str.CharPtr );
        }
      }
    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
    return 0;
  }

  GemStone::GemFire::Cache::Generic::IGFSerializable^
    ManagedCacheableKeyBytesGeneric::getManagedObject() const
  {

    GemStone::GemFire::Cache::Generic::Log::Debug("ManagedCacheableKeyBytesGeneric::getManagedObject " + m_size);

		//System::Text::StringBuilder^ sb = gcnew System::Text::StringBuilder(2000);
		//for(uint32_t i = 0; i<m_size; i++)
		//{
		//	if(m_bytes[i] != 0)
		//		sb->Append(System::Convert::ToChar( m_bytes[i]));
		//	//sb->Append(' ');
		//}

  //  GemStone::GemFire::Cache::Generic::Log::Debug("ManagedCacheableKeyBytesGeneric::getManagedObject " + sb);
    gemfire::DataInput dinp(m_bytes, m_size);
    GemStone::GemFire::Cache::Generic::DataInput mg_dinp(&dinp, true);
    GemStone::GemFire::Cache::Generic::TypeFactoryMethodGeneric^ creationMethod =
      GemStone::GemFire::Cache::Generic::Serializable::GetTypeFactoryMethodGeneric(m_classId);
    GemStone::GemFire::Cache::Generic::IGFSerializable^ newObj = creationMethod();
    return newObj->FromData(%mg_dinp);
  }
}
