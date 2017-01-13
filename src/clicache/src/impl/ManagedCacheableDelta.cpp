/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "../gf_includes.hpp"
#include "ManagedCacheableDelta.hpp"
#include "../DataInput.hpp"
#include "../DataOutput.hpp"
#include "../CacheableString.hpp"
#include <GemfireTypeIdsImpl.hpp>
#include "../ExceptionTypes.hpp"
#include "SafeConvert.hpp"


using namespace System;

namespace gemfire
{

  void ManagedCacheableDeltaGeneric::toData( DataOutput& output ) const
  {
    try {
      uint32 pos = (int)output.getBufferLength();
      GemStone::GemFire::Cache::Generic::DataOutput mg_output( &output, true );
      m_managedSerializableptr->ToData( %mg_output );
      //this will move the cursor in c++ layer
      mg_output.WriteBytesToUMDataOutput();
      ManagedCacheableDeltaGeneric* tmp = const_cast<ManagedCacheableDeltaGeneric*>(this);
      tmp->m_objectSize = (int)(output.getBufferLength() - pos);
    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
  }

  Serializable* ManagedCacheableDeltaGeneric::fromData( DataInput& input )
  {
    try {
      int pos = input.getBytesRead();
      GemStone::GemFire::Cache::Generic::DataInput mg_input( &input, true );
      m_managedSerializableptr->FromData( %mg_input );

       //this will move the cursor in c++ layer
      input.advanceCursor(mg_input.BytesReadInternally);

      m_objectSize = input.getBytesRead() - pos;

      if(m_hashcode == 0)
        m_hashcode = m_managedptr->GetHashCode();
        
    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
    return this;
  }

  uint32_t ManagedCacheableDeltaGeneric::objectSize( ) const
  {
    try {
      int ret = m_managedSerializableptr->ObjectSize;
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

  int32_t ManagedCacheableDeltaGeneric::classId() const
  {
    uint32_t classId;
    try {
      classId = m_managedSerializableptr->ClassId;
    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
    return (classId >= 0x80000000 ? 0 : classId);
  }

  int8_t ManagedCacheableDeltaGeneric::typeId() const
  {
    try {
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
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
    return 0;
  }

  int8_t ManagedCacheableDeltaGeneric::DSFID() const
  {
    // convention that [0x8000000, 0xa0000000) is for FixedIDDefault,
    // [0xa000000, 0xc0000000) is for FixedIDByte,
    // [0xc0000000, 0xe0000000) is for FixedIDShort
    // and [0xe0000000, 0xffffffff] is for FixedIDInt
    // Note: depends on fact that FixedIDByte is 1, FixedIDShort is 2
    // and FixedIDInt is 3; if this changes then correct this accordingly
    uint32_t classId = m_managedSerializableptr->ClassId;
    if (classId >= 0x80000000) {
      return (int8_t)((classId - 0x80000000) / 0x20000000);
    }
    return 0;
  }

  bool ManagedCacheableDeltaGeneric::hasDelta()
  {
    return m_managedptr->HasDelta();
  }

  void ManagedCacheableDeltaGeneric::toDelta( DataOutput& output) const
  {
    try {
      GemStone::GemFire::Cache::Generic::DataOutput mg_output( &output, true );
      m_managedptr->ToDelta( %mg_output );
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

  void ManagedCacheableDeltaGeneric::fromDelta( DataInput& input )
  {
    try {
      GemStone::GemFire::Cache::Generic::DataInput mg_input( &input, true );
      m_managedptr->FromDelta( %mg_input );

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

  DeltaPtr ManagedCacheableDeltaGeneric::clone()
  {
    try {
      ICloneable^ cloneable = dynamic_cast< ICloneable^ >( (
        GemStone::GemFire::Cache::Generic::IGFDelta^ ) m_managedptr );
      if ( cloneable ) {
        GemStone::GemFire::Cache::Generic::IGFSerializable^ Mclone =
          dynamic_cast< GemStone::GemFire::Cache::Generic::IGFSerializable^ >( cloneable->Clone( ) );
        return DeltaPtr( static_cast< ManagedCacheableDeltaGeneric* >(
          SafeMSerializableConvertGeneric( Mclone ) ) );
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

  bool ManagedCacheableDeltaGeneric::operator ==(const gemfire::CacheableKey& other) const
  {
    try {
      // now checking classId(), typeId(), DSFID() etc. will be much more
      // expensive than just a dynamic_cast
      const ManagedCacheableDeltaGeneric* p_other =
        dynamic_cast<const ManagedCacheableDeltaGeneric*>(&other);
      if (p_other != NULL) {
        return m_managedptr->Equals(p_other->ptr());
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

  bool ManagedCacheableDeltaGeneric::operator == ( const ManagedCacheableDeltaGeneric& other ) const
  {
    try {
      return m_managedptr->Equals(other.ptr());
    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
    return false;
    
  }

  uint32_t ManagedCacheableDeltaGeneric::hashcode( ) const
  {
    throw gcnew System::NotSupportedException;
  }

  size_t ManagedCacheableDeltaGeneric::logString( char* buffer, size_t maxLength ) const
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
