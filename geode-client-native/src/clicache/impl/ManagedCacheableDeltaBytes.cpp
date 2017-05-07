/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "../gf_includes.hpp"
#include "ManagedCacheableDeltaBytes.hpp"
#include "../DataInputM.hpp"
#include "../DataOutputM.hpp"
#include "../CacheableStringM.hpp"
#include <cppcache/impl/GemfireTypeIdsImpl.hpp>
#include "SafeConvert.hpp"

using namespace System;

namespace gemfire
{

  void ManagedCacheableDeltaBytes::toData( DataOutput& output ) const
  {
    GemStone::GemFire::Cache::Log::Debug("ManagedCacheableDeltaBytes::toData: current domain ID: " + System::Threading::Thread::GetDomainID() + " for object: " + System::Convert::ToString((int)this) + " with its domain ID: " + m_domainId );
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

  Serializable* ManagedCacheableDeltaBytes::fromData( DataInput& input )
  {
    try {
       GemStone::GemFire::Cache::Log::Debug("ManagedCacheableDeltaBytes::fromData: classid " + m_classId);
       GemStone::GemFire::Cache::DataInput mg_input( &input, true );
       const uint8_t* objStartPos = input.currentBufferPosition();
         
       GemStone::GemFire::Cache::IGFSerializable^ obj = GemStone::GemFire::Cache::Serializable::GetTypeFactoryMethod(m_classId)();
       obj->FromData(%mg_input);
       input.advanceCursor(mg_input.BytesReadInternally);
    
       m_hashCode = obj->GetHashCode();

       const uint8_t* objEndPos = input.currentBufferPosition();
       
       //m_size = mg_input.BytesRead;
       m_size = (uint32_t)(objEndPos - objStartPos);
       GemStone::GemFire::Cache::Log::Debug("ManagedCacheableDeltaBytes::fromData: objectSize = " + m_size + " m_hashCode = " + m_hashCode);
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

  uint32_t ManagedCacheableDeltaBytes::objectSize( ) const
  {
    try {
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

  int32_t ManagedCacheableDeltaBytes::classId() const
  {
    uint32_t classId;
    try {
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

  int8_t ManagedCacheableDeltaBytes::typeId() const
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

  int8_t ManagedCacheableDeltaBytes::DSFID() const
  {
    // convention that [0x8000000, 0xa0000000) is for FixedIDDefault,
    // [0xa000000, 0xc0000000) is for FixedIDByte,
    // [0xc0000000, 0xe0000000) is for FixedIDShort
    // and [0xe0000000, 0xffffffff] is for FixedIDInt
    // Note: depends on fact that FixedIDByte is 1, FixedIDShort is 2
    // and FixedIDInt is 3; if this changes then correct this accordingly
    uint32_t classId = m_classId;
    if (classId >= 0x80000000) {
      return (int8_t)((classId - 0x80000000) / 0x20000000);
    }
    return 0;
  }

  bool ManagedCacheableDeltaBytes::hasDelta()
  {
    //GemStone::GemFire::Cache::IGFDelta^ deltaObj = this->getManagedObject();
    //return deltaObj->HasDelta();
    return m_hasDelta;
  }

  void ManagedCacheableDeltaBytes::toDelta( DataOutput& output) const
  {
    try {
      GemStone::GemFire::Cache::Log::Debug("ManagedCacheableDeltaBytes::toDelta: current domain ID: " + System::Threading::Thread::GetDomainID() + " for object: " + System::Convert::ToString((int)this) + " with its domain ID: " + m_domainId);
      GemStone::GemFire::Cache::IGFDelta^ deltaObj = this->getManagedObject();
      
      GemStone::GemFire::Cache::DataOutput mg_output( &output, true );
      deltaObj->ToDelta( %mg_output );
      mg_output.WriteBytesToUMDataOutput();
    }
    catch (GemStone::GemFire::Cache::GemFireException^ ex) { 
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
  }

  void ManagedCacheableDeltaBytes::fromDelta( DataInput& input )
  {
    try {
      GemStone::GemFire::Cache::Log::Debug("ManagedCacheableDeltaBytes::fromDelta:");
      GemStone::GemFire::Cache::IGFDelta^ deltaObj = this->getManagedObject();
      
      GemStone::GemFire::Cache::DataInput mg_input (&input, true);
      deltaObj->FromDelta( %mg_input );

      input.advanceCursor(mg_input.BytesReadInternally);
      
      GemStone::GemFire::Cache::IGFSerializable^ managedptr = dynamic_cast <GemStone::GemFire::Cache::IGFSerializable^> ( deltaObj );
      if(managedptr != nullptr)
      {
        GemStone::GemFire::Cache::Log::Debug("ManagedCacheableDeltaBytes::fromDelta: current domain ID: " + System::Threading::Thread::GetDomainID() + " for object: " + System::Convert::ToString((int)this) + " with its domain ID: " + m_domainId);
        GemStone::GemFire::Cache::Log::Debug("ManagedCacheableDeltaBytes::fromDelta: classid " + managedptr->ClassId + " : " + managedptr->ToString());
        gemfire::DataOutput dataOut;
        GemStone::GemFire::Cache::DataOutput mg_output( &dataOut, true);
        managedptr->ToData( %mg_output );

        //move cursor
        // dataOut.advanceCursor(mg_output.BufferLength);
        mg_output.WriteBytesToUMDataOutput();
        
        GF_SAFE_DELETE(m_bytes);
        m_bytes = dataOut.getBufferCopy();
        m_size = dataOut.getBufferLength();
        GemStone::GemFire::Cache::Log::Debug("ManagedCacheableDeltaBytes::fromDelta objectSize = " + m_size + " m_hashCode = " + m_hashCode);
        m_hashCode = managedptr->GetHashCode(); 
      }
    }
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
  }

  DeltaPtr ManagedCacheableDeltaBytes::clone()
  {
    try {
      GemStone::GemFire::Cache::IGFDelta^ deltaObj = this->getManagedObject();
      ICloneable^ cloneable = dynamic_cast< ICloneable^ >( ( GemStone::GemFire::Cache::IGFDelta^ ) deltaObj );
      if ( cloneable ) {
        GemStone::GemFire::Cache::IGFSerializable^ Mclone = dynamic_cast< GemStone::GemFire::Cache::IGFSerializable^ >( cloneable->Clone( ) );
        return DeltaPtr( static_cast< ManagedCacheableDeltaBytes* >( SafeMSerializableConvert( Mclone ) ) );
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

  GemStone::GemFire::Cache::IGFDelta^
    ManagedCacheableDeltaBytes::getManagedObject() const
  {

    GemStone::GemFire::Cache::Log::Debug("ManagedCacheableDeltaBytes::getManagedObject");
    
    gemfire::DataInput dinp(m_bytes, m_size);
    GemStone::GemFire::Cache::DataInput mg_dinp(&dinp, true);
    GemStone::GemFire::Cache::TypeFactoryMethod^ creationMethod =
        GemStone::GemFire::Cache::Serializable::GetTypeFactoryMethod(m_classId);
    GemStone::GemFire::Cache::IGFSerializable^ newObj = creationMethod();

    GemStone::GemFire::Cache::IGFDelta^ managedDeltaptr = dynamic_cast <GemStone::GemFire::Cache::IGFDelta^> ( newObj->FromData(%mg_dinp) );
    return managedDeltaptr;
  }
}
