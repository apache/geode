/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "../gf_includes.hpp"
#include "ManagedCacheableDelta.hpp"
#include "../DataInputM.hpp"
#include "../DataOutputM.hpp"
#include "../CacheableStringM.hpp"
#include <cppcache/impl/GemfireTypeIdsImpl.hpp>
#include "SafeConvert.hpp"

using namespace System;

namespace gemfire
{

  void ManagedCacheableDelta::toData( DataOutput& output ) const
  {
    try {
      GemStone::GemFire::Cache::DataOutput mg_output( &output, true );
      m_managedSerializableptr->ToData( %mg_output );
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

  Serializable* ManagedCacheableDelta::fromData( DataInput& input )
  {
    try {
      GemStone::GemFire::Cache::DataInput mg_input( &input, true );
      m_managedSerializableptr->FromData( %mg_input );

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

  uint32_t ManagedCacheableDelta::objectSize( ) const
  {
    try {
      return m_managedSerializableptr->ObjectSize;
    }
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
    return 0;
  }

  int32_t ManagedCacheableDelta::classId() const
  {
    uint32_t classId;
    try {
      classId = m_managedSerializableptr->ClassId;
    }
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
    return (classId >= 0x80000000 ? 0 : classId);
  }

  int8_t ManagedCacheableDelta::typeId() const
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
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
    return 0;
  }

  int8_t ManagedCacheableDelta::DSFID() const
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

  bool ManagedCacheableDelta::hasDelta()
  {
    return m_managedptr->HasDelta();
  }

  void ManagedCacheableDelta::toDelta( DataOutput& output) const
  {
    try {
      GemStone::GemFire::Cache::DataOutput mg_output( &output, true );
      m_managedptr->ToDelta( %mg_output );
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

  void ManagedCacheableDelta::fromDelta( DataInput& input )
  {
    try {
      GemStone::GemFire::Cache::DataInput mg_input (&input, true);
      m_managedptr->FromDelta( %mg_input );

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
  }

  DeltaPtr ManagedCacheableDelta::clone()
  {
    try {
      ICloneable^ cloneable = dynamic_cast< ICloneable^ >( ( GemStone::GemFire::Cache::IGFDelta^ ) m_managedptr );
      if ( cloneable ) {
        GemStone::GemFire::Cache::IGFSerializable^ Mclone = dynamic_cast< GemStone::GemFire::Cache::IGFSerializable^ >( cloneable->Clone( ) );
        return DeltaPtr( static_cast< ManagedCacheableDelta* >( SafeMSerializableConvert( Mclone ) ) );
      }
      else {
        return Delta::clone( );
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
}
