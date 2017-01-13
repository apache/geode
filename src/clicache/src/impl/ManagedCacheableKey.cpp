/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "../gf_includes.hpp"
#include "../ICacheableKey.hpp"
#include "ManagedCacheableKey.hpp"
#include "../DataInput.hpp"
#include "../DataOutput.hpp"
#include "../CacheableString.hpp"
#include <GemfireTypeIdsImpl.hpp>
#include "../ExceptionTypes.hpp"
#include "../Log.hpp"

using namespace System;

namespace gemfire
{

  void ManagedCacheableKeyGeneric::toData( gemfire::DataOutput& output ) const
  {
    try {
      uint32 pos = (int)output.getBufferLength();
			//GemStone::GemFire::Cache::Generic::Log::Debug("ManagedCacheableKeyGeneric::toData");      
      GemStone::GemFire::Cache::Generic::DataOutput mg_output( &output, true );
      m_managedptr->ToData( %mg_output );
      //this will move the cursor in c++ layer
      mg_output.WriteBytesToUMDataOutput();
      
      ManagedCacheableKeyGeneric* tmp = const_cast<ManagedCacheableKeyGeneric*>(this);
      tmp->m_objectSize = (int)(output.getBufferLength() - pos);
    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
  }

  gemfire::Serializable* ManagedCacheableKeyGeneric::fromData( gemfire::DataInput& input )
  {
    try {
      int pos = input.getBytesRead();
     //GemStone::GemFire::Cache::Generic::Log::Debug("ManagedCacheableKeyGeneric::fromData");      
      GemStone::GemFire::Cache::Generic::DataInput mg_input (&input, true);
      m_managedptr = m_managedptr->FromData( %mg_input );

      //this will move the cursor in c++ layer
      input.advanceCursor(mg_input.BytesReadInternally);
      m_objectSize = input.getBytesRead() - pos;
      //if(m_hashcode == 0)
        //m_hashcode = m_managedptr->GetHashCode();
        

    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
    return this;
  }

  uint32_t ManagedCacheableKeyGeneric::objectSize( ) const
  {
    try {
      int ret = m_managedptr->ObjectSize;
      if(ret > m_objectSize)
        return ret;
      else
        return m_objectSize;
    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
    return 0;
  }

  int32_t ManagedCacheableKeyGeneric::classId() const
  {
		//GemStone::GemFire::Cache::Generic::Log::Debug("ManagedCacheableKeyGeneric::classid " + m_classId);
    /*uint32_t classId;
    try {
      classId = m_managedptr->ClassId;
    }
    catch (GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemFireException::ThrowNative(ex);
    }*/
    return (m_classId >= 0x80000000 ? 0 : m_classId);
  }

  int8_t ManagedCacheableKeyGeneric::typeId() const
  {
		//GemStone::GemFire::Cache::Generic::Log::Debug("ManagedCacheableKeyGeneric::typeId " + m_classId);
    if (m_classId >= 0x80000000) {
      return (int8_t)((m_classId - 0x80000000) % 0x20000000);
    }
    else if (m_classId <= 0x7F) {
			//GemStone::GemFire::Cache::Generic::Log::Debug("ManagedCacheableKeyGeneric::typeId inin"); 
      return (int8_t)GemfireTypeIdsImpl::CacheableUserData;
    }
    else if (m_classId <= 0x7FFF) {
      return (int8_t)GemfireTypeIdsImpl::CacheableUserData2;
    }
    else {
      return (int8_t)GemfireTypeIdsImpl::CacheableUserData4;
    }
  }

  int8_t ManagedCacheableKeyGeneric::DSFID() const
  {
    // convention that [0x8000000, 0xa0000000) is for FixedIDDefault,
    // [0xa000000, 0xc0000000) is for FixedIDByte,
    // [0xc0000000, 0xe0000000) is for FixedIDShort
    // and [0xe0000000, 0xffffffff] is for FixedIDInt
    // Note: depends on fact that FixedIDByte is 1, FixedIDShort is 2
    // and FixedIDInt is 3; if this changes then correct this accordingly
    if (m_classId >= 0x80000000) {
      return (int8_t)((m_classId - 0x80000000) / 0x20000000);
    }
    return 0;
  }

  gemfire::CacheableStringPtr ManagedCacheableKeyGeneric::toString( ) const
  {
    try {
      gemfire::CacheableStringPtr cStr;
			GemStone::GemFire::Cache::Generic::CacheableString::GetCacheableString(
        m_managedptr->ToString( ), cStr );
      return cStr;
    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
    return NULLPTR;
  }

  bool ManagedCacheableKeyGeneric::operator ==(const gemfire::CacheableKey& other) const
  {
    try {
      // now checking classId(), typeId(), DSFID() etc. will be much more
      // expensive than just a dynamic_cast
      const ManagedCacheableKeyGeneric* p_other =
        dynamic_cast<const ManagedCacheableKeyGeneric*>(&other);
      if (p_other != NULL) {
        return static_cast<GemStone::GemFire::Cache::Generic::ICacheableKey^>(
          (static_cast<GemStone::GemFire::Cache::Generic::IGFSerializable^>((GemStone::GemFire::Cache::Generic::IGFSerializable^)m_managedptr)))->Equals(
          static_cast<GemStone::GemFire::Cache::Generic::ICacheableKey^>(p_other->ptr()));
      }
      return false;
    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
    return false;
  }

  bool ManagedCacheableKeyGeneric::operator ==(const ManagedCacheableKeyGeneric& other) const
  {
    try {
      return static_cast<GemStone::GemFire::Cache::Generic::ICacheableKey^>(
        (GemStone::GemFire::Cache::Generic::IGFSerializable^)(GemStone::GemFire::Cache::Generic::IGFSerializable^)m_managedptr)->Equals(
        static_cast<GemStone::GemFire::Cache::Generic::ICacheableKey^>(other.ptr()));
    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
    return false;
  }

  uint32_t ManagedCacheableKeyGeneric::hashcode( ) const
  {
    if  (m_hashcode != 0)
      return m_hashcode;
    try {
      
      ManagedCacheableKeyGeneric* tmp = const_cast<ManagedCacheableKeyGeneric*>(this);
      tmp->m_hashcode = ((GemStone::GemFire::Cache::Generic::ICacheableKey^)
        (GemStone::GemFire::Cache::Generic::IGFSerializable^)m_managedptr)
        ->GetHashCode( );
       return m_hashcode ;
    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
    return 0;
  }

  size_t ManagedCacheableKeyGeneric::logString( char* buffer, size_t maxLength ) const
  {
    try {
      if ( maxLength > 0 ) {
        String^ logstr = m_managedptr->GetType( )->Name + '(' +
          m_managedptr->ToString( ) + ')';
				GemStone::GemFire::Cache::Generic::ManagedString mg_str( logstr );
        return snprintf( buffer, maxLength, "%s", mg_str.CharPtr );
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

}
