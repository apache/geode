/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "../gf_includes.hpp"
#include "ManagedCacheableKey.hpp"
#include "../DataInputM.hpp"
#include "../DataOutputM.hpp"
#include "../CacheableStringM.hpp"
#include <cppcache/impl/GemfireTypeIdsImpl.hpp>



using namespace System;

namespace gemfire
{

  void ManagedCacheableKey::toData( DataOutput& output ) const
  {
    try {
      GemStone::GemFire::Cache::DataOutput mg_output( &output, true );
      m_managedptr->ToData( %mg_output );
      //this will move the cursor in c++ layer
      mg_output.WriteBytesToUMDataOutput();
    }
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
  }

  Serializable* ManagedCacheableKey::fromData( DataInput& input )
  {
    try {
      GemStone::GemFire::Cache::DataInput mg_input (&input, true);
      m_managedptr = m_managedptr->FromData( %mg_input );

      //this will move the cursor in c++ layer
      input.advanceCursor(mg_input.BytesReadInternally);

      m_hashcode = m_managedptr->GetHashCode();
    }
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }    
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
    
    return this;
  }

  uint32_t ManagedCacheableKey::objectSize( ) const
  {
    try {
      return m_managedptr->ObjectSize;
    }
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
    return 0;
  }

  int32_t ManagedCacheableKey::classId() const
  {
    /*uint32_t classId;
    try {
      classId = m_managedptr->ClassId;
    }
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }*/
    return (m_classId >= 0x80000000 ? 0 : m_classId);
  }

  int8_t ManagedCacheableKey::typeId() const
  {
    if (m_classId >= 0x80000000) {
      return (int8_t)((m_classId - 0x80000000) % 0x20000000);
    }
    else if (m_classId <= 0x7F) {
      return (int8_t)GemfireTypeIdsImpl::CacheableUserData;
    }
    else if (m_classId <= 0x7FFF) {
      return (int8_t)GemfireTypeIdsImpl::CacheableUserData2;
    }
    else {
      return (int8_t)GemfireTypeIdsImpl::CacheableUserData4;
    }
  }

  int8_t ManagedCacheableKey::DSFID() const
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

  CacheableStringPtr ManagedCacheableKey::toString( ) const
  {
    try {
      CacheableStringPtr cStr;
      GemStone::GemFire::Cache::CacheableString::GetCacheableString(
        m_managedptr->ToString( ), cStr );
      return cStr;
    }
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
    return NULLPTR;
  }

  bool ManagedCacheableKey::operator ==(const CacheableKey& other) const
  {
    try {
      // now checking classId(), typeId(), DSFID() etc. will be much more
      // expensive than just a dynamic_cast
      const ManagedCacheableKey* p_other =
        dynamic_cast<const ManagedCacheableKey*>(&other);
      if (p_other != NULL) {
        return static_cast<GemStone::GemFire::Cache::ICacheableKey^>(
          (GemStone::GemFire::Cache::IGFSerializable^)m_managedptr)->Equals(
          static_cast<GemStone::GemFire::Cache::ICacheableKey^>(p_other->ptr()));
      }
      return false;
    }
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
    return false;
  }

  bool ManagedCacheableKey::operator ==(const ManagedCacheableKey& other) const
  {
    try {
      return static_cast<GemStone::GemFire::Cache::ICacheableKey^>(
        (GemStone::GemFire::Cache::IGFSerializable^)m_managedptr)->Equals(
        static_cast<GemStone::GemFire::Cache::ICacheableKey^>(other.ptr()));
    }
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
    return false;
  }

  uint32_t ManagedCacheableKey::hashcode( ) const
  {
    if  (m_hashcode != 0)
      return m_hashcode;
    try {
      return ((GemStone::GemFire::Cache::ICacheableKey^)
        (GemStone::GemFire::Cache::IGFSerializable^)m_managedptr)
        ->GetHashCode( );
    }
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
    return 0;
  }

  size_t ManagedCacheableKey::logString( char* buffer, size_t maxLength ) const
  {
    try {
      if ( maxLength > 0 ) {
        String^ logstr = m_managedptr->GetType( )->Name + '(' +
          m_managedptr->ToString( ) + ')';
        GemStone::GemFire::ManagedString mg_str( logstr );
        return snprintf( buffer, maxLength, "%s", mg_str.CharPtr );
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

}
