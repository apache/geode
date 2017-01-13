
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once
#include "PdxManagedCacheableKey.hpp"
#include "../DataInput.hpp"
#include "../DataOutput.hpp"
#include "../CacheableString.hpp"
#include <GemfireTypeIdsImpl.hpp>
#include "../ExceptionTypes.hpp"
#include "ManagedString.hpp"
#include "PdxHelper.hpp"
#include "SafeConvert.hpp"
using namespace System;

namespace gemfire
{
  void PdxManagedCacheableKey::toData( gemfire::DataOutput& output ) const
  {
    try {
     uint32 pos = (int)output.getBufferLength();
     GemStone::GemFire::Cache::Generic::DataOutput mg_output( &output, true );
     GemStone::GemFire::Cache::Generic::Internal::PdxHelper::SerializePdx(%mg_output, m_managedptr);
      //m_managedptr->ToData( %mg_output );
      //this will move the cursor in c++ layer
      mg_output.WriteBytesToUMDataOutput();
      PdxManagedCacheableKey* tmp = const_cast<PdxManagedCacheableKey*>(this);
      tmp->m_objectSize = (int)(output.getBufferLength() - pos);
    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
  }

  Serializable* PdxManagedCacheableKey::fromData( gemfire::DataInput& input )
  {
    try {
      int pos = input.getBytesRead();
      GemStone::GemFire::Cache::Generic::DataInput mg_input (&input, true);
      //m_managedptr = m_managedptr->FromData( %mg_input );
       GemStone::GemFire::Cache::Generic::IPdxSerializable^ tmp= GemStone::GemFire::Cache::Generic::Internal::PdxHelper::DeserializePdx(%mg_input, false);
       m_managedptr = tmp;
       m_managedDeltaptr = dynamic_cast<GemStone::GemFire::Cache::Generic::IGFDelta^>(tmp);

      //this will move the cursor in c++ layer
      input.advanceCursor(mg_input.BytesReadInternally);
      m_objectSize = input.getBytesRead() - pos;
     // m_hashcode = m_managedptr->GetHashCode();
    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
    return this;
  }

  uint32_t PdxManagedCacheableKey::objectSize( ) const
  {
    try {
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

  int32_t PdxManagedCacheableKey::classId() const
  {
    /*uint32_t classId;
    try {
      classId = m_managedptr->ClassId;
    }
    catch (Com::Vmware::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }*/
    //return (m_classId >= 0x80000000 ? 0 : m_classId);
    return 0;
  }

  int8_t PdxManagedCacheableKey::typeId() const
  {
    /*if (m_classId >= 0x80000000) {
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
    }*/
    return (int8_t)GemfireTypeIdsImpl::PDX;
  }

  int8_t PdxManagedCacheableKey::DSFID() const
  {
    // convention that [0x8000000, 0xa0000000) is for FixedIDDefault,
    // [0xa000000, 0xc0000000) is for FixedIDByte,
    // [0xc0000000, 0xe0000000) is for FixedIDShort
    // and [0xe0000000, 0xffffffff] is for FixedIDInt
    // Note: depends on fact that FixedIDByte is 1, FixedIDShort is 2
    // and FixedIDInt is 3; if this changes then correct this accordingly
    //if (m_classId >= 0x80000000) {
    //  return (int8_t)((m_classId - 0x80000000) / 0x20000000);
    //}
    return 0;
  }

  gemfire::CacheableStringPtr PdxManagedCacheableKey::toString( ) const
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

  bool PdxManagedCacheableKey::operator ==(const gemfire::CacheableKey& other) const
  {
    try {
      // now checking classId(), typeId(), DSFID() etc. will be much more
      // expensive than just a dynamic_cast
      const PdxManagedCacheableKey* p_other =
        dynamic_cast<const PdxManagedCacheableKey*>(&other);
      if (p_other != NULL) {
				return ((GemStone::GemFire::Cache::Generic::IPdxSerializable^)m_managedptr)->Equals((p_other->ptr()));
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

  bool PdxManagedCacheableKey::operator ==(const PdxManagedCacheableKey& other) const
  {
    try {
      return ((GemStone::GemFire::Cache::Generic::IPdxSerializable^)m_managedptr)->Equals((other.ptr()));
    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
    return false;
  }

  uint32_t PdxManagedCacheableKey::hashcode( ) const
  {
    if  (m_hashcode != 0)
      return m_hashcode;
    try {
      PdxManagedCacheableKey* tmp = const_cast<PdxManagedCacheableKey*>(this);
      tmp->m_hashcode = (
        (GemStone::GemFire::Cache::Generic::IPdxSerializable^)tmp->m_managedptr)
        ->GetHashCode( );
      return m_hashcode;
    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
    return 0;
  }

  size_t PdxManagedCacheableKey::logString( char* buffer, size_t maxLength ) const
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

  bool PdxManagedCacheableKey::hasDelta()
  {
    if(m_managedDeltaptr)
    {
      return m_managedDeltaptr->HasDelta();
    }
    return false;
  }

  void PdxManagedCacheableKey::toDelta( DataOutput& output) const
  {
    try {
      GemStone::GemFire::Cache::Generic::DataOutput mg_output( &output, true );
      m_managedDeltaptr->ToDelta( %mg_output );
      //this will move the cursor in c++ layer
      mg_output.WriteBytesToUMDataOutput();
    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
  }

  void PdxManagedCacheableKey::fromDelta( DataInput& input )
  {
    try {
      GemStone::GemFire::Cache::Generic::DataInput mg_input( &input, true );
      m_managedDeltaptr->FromDelta( %mg_input );

      //this will move the cursor in c++ layer
      input.advanceCursor(mg_input.BytesReadInternally);

      m_hashcode = m_managedptr->GetHashCode();
    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
  }

  DeltaPtr PdxManagedCacheableKey::clone()
  {
    try {
      ICloneable^ cloneable = dynamic_cast< ICloneable^ >( (
        GemStone::GemFire::Cache::Generic::IGFDelta^ ) m_managedDeltaptr );
      if ( cloneable ) {
        GemStone::GemFire::Cache::Generic::IPdxSerializable^ Mclone =
          dynamic_cast< GemStone::GemFire::Cache::Generic::IPdxSerializable^ >( cloneable->Clone( ) );
        return DeltaPtr( static_cast< PdxManagedCacheableKey* >(
          SafeGenericM2UMConvert( Mclone ) ) );
      }
      else {
        return Delta::clone( );
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
}
