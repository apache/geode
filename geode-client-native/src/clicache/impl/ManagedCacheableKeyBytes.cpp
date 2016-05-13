/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "../gf_includes.hpp"
#include "ManagedCacheableKeyBytes.hpp"
#include "../DataInputM.hpp"
#include "../DataOutputM.hpp"
#include "../SerializableM.hpp"
#include "../CacheableStringM.hpp"
#include <cppcache/impl/GemfireTypeIdsImpl.hpp>


using namespace System;

namespace gemfire
{
  void ManagedCacheableKeyBytes::toData( DataOutput& output ) const
  {
    GemStone::GemFire::Cache::Log::Debug("ManagedCacheableKeyBytes::toData: current domain ID: " + System::Threading::Thread::GetDomainID() + " for object: " + System::Convert::ToString((int)this) + " with its domain ID: " + m_domainId );
    try {
        output.writeBytesOnly(m_bytes, m_size);
    }
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
  }

  Serializable* ManagedCacheableKeyBytes::fromData( DataInput& input )
  {
    try {
      GemStone::GemFire::Cache::Log::Debug("ManagedCacheableKeyBytes::fromData: classid " + m_classId);
       GemStone::GemFire::Cache::DataInput mg_input( &input, true );
       const uint8_t* objStartPos = input.currentBufferPosition();
         
       GemStone::GemFire::Cache::IGFSerializable^ obj = GemStone::GemFire::Cache::Serializable::GetTypeFactoryMethod(m_classId)();
       obj->FromData(%mg_input);
    
       input.advanceCursor(mg_input.BytesReadInternally);

       m_hashCode = obj->GetHashCode();

       const uint8_t* objEndPos = input.currentBufferPosition();
       
       //m_size = mg_input.BytesRead;
       m_size = (uint32_t)(objEndPos - objStartPos);
       GemStone::GemFire::Cache::Log::Debug("ManagedCacheableKeyBytes::fromData: objectSize = " + m_size + " m_hashCode = " + m_hashCode);
       m_bytes = input.getBufferCopyFrom(objStartPos, m_size);
    }
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
    return this;
  }

  uint32_t ManagedCacheableKeyBytes::objectSize( ) const
  {
    try {
      //return m_managedptr->ObjectSize;
      return m_size;
    }
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
    return 0;
  }

  int32_t ManagedCacheableKeyBytes::classId() const
  {
    uint32_t classId;
    try {
      //classId = m_managedptr->ClassId;
      classId = m_classId;
    }
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
    return (classId >= 0x80000000 ? 0 : classId);
  }

  int8_t ManagedCacheableKeyBytes::typeId() const
  {
    try {
      //uint32_t classId = m_managedptr->ClassId;
      uint32_t classId = m_classId;
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
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
    return 0;
  }

  int8_t ManagedCacheableKeyBytes::DSFID() const
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

  CacheableStringPtr ManagedCacheableKeyBytes::toString( ) const
  {
    try {
      GemStone::GemFire::Cache::IGFSerializable^ manageObject = getManagedObject();
      if(manageObject != nullptr)
      {
	      CacheableStringPtr cStr;
        GemStone::GemFire::Cache::CacheableString::GetCacheableString(
          manageObject->ToString(), cStr );
        return cStr;
      }
    }
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
    return NULLPTR;
  }

  bool ManagedCacheableKeyBytes::operator ==(const CacheableKey& other) const
  {
    try {
      GemStone::GemFire::Cache::Log::Debug("ManagedCacheableKeyBytes::equal");
      // now checking classId(), typeId(), DSFID() etc. will be much more
      // expensive than just a dynamic_cast
      const ManagedCacheableKeyBytes* p_other =
        dynamic_cast<const ManagedCacheableKeyBytes*>(&other);
      if (p_other != NULL) {
       gemfire::DataInput di(m_bytes, m_size);
        GemStone::GemFire::Cache::DataInput mg_input(&di, true);
        GemStone::GemFire::Cache::IGFSerializable^ obj = GemStone::GemFire::Cache::Serializable::GetTypeFactoryMethod(m_classId)();
        obj->FromData(%mg_input);
        bool ret = obj->Equals(p_other->ptr());
        GemStone::GemFire::Cache::Log::Debug("ManagedCacheableKeyBytes::equal return VAL = " + ret);
        return ret;
      }
    }
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
    GemStone::GemFire::Cache::Log::Debug("ManagedCacheableKeyBytes::equal returns false");
    return false;
  }

  bool ManagedCacheableKeyBytes::operator ==(const ManagedCacheableKeyBytes& other) const
  {
    try {
      GemStone::GemFire::Cache::Log::Debug("ManagedCacheableKeyBytes::equal. ");
        gemfire::DataInput di(m_bytes, m_size);
        GemStone::GemFire::Cache::DataInput mg_input(&di, true);
        GemStone::GemFire::Cache::IGFSerializable^ obj = GemStone::GemFire::Cache::Serializable::GetTypeFactoryMethod(m_classId)();
        obj->FromData(%mg_input);
        bool ret = obj->Equals(other.ptr());
        GemStone::GemFire::Cache::Log::Debug("ManagedCacheableKeyBytes::equal return VAL = " + ret);
        return ret;
        //return obj->Equals(other.ptr());
    }
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
    GemStone::GemFire::Cache::Log::Debug("ManagedCacheableKeyBytes::equal return false");
    return false;
  }

  uint32_t ManagedCacheableKeyBytes::hashcode( ) const
  {
    return m_hashCode;
  }

  size_t ManagedCacheableKeyBytes::logString( char* buffer, size_t maxLength ) const
  {
	  try {
       GemStone::GemFire::Cache::IGFSerializable^ manageObject = getManagedObject();
      if(manageObject != nullptr)
      {
        if ( maxLength > 0 ) {
          String^ logstr = manageObject->GetType( )->Name + '(' +
            manageObject->ToString( ) + ')';
          GemStone::GemFire::ManagedString mg_str( logstr );
          return snprintf( buffer, maxLength, "%s", mg_str.CharPtr );
        }
      }
    }
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
    return 0;
  }

  GemStone::GemFire::Cache::IGFSerializable^
    ManagedCacheableKeyBytes::getManagedObject() const
  {

    GemStone::GemFire::Cache::Log::Debug("ManagedCacheableKeyBytes::getManagedObject");
    
    gemfire::DataInput dinp(m_bytes, m_size);
    GemStone::GemFire::Cache::DataInput mg_dinp(&dinp, true);
    GemStone::GemFire::Cache::TypeFactoryMethod^ creationMethod =
        GemStone::GemFire::Cache::Serializable::GetTypeFactoryMethod(m_classId);
    GemStone::GemFire::Cache::IGFSerializable^ newObj = creationMethod();
    return newObj->FromData(%mg_dinp);
  }
}
