/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "../gf_includes.hpp"
#include "ManagedCacheableDeltaBytes.hpp"
#include "../DataInput.hpp"
#include "../DataOutput.hpp"
#include "../CacheableString.hpp"
#include <GemfireTypeIdsImpl.hpp>
#include "../ExceptionTypes.hpp"
#include "SafeConvert.hpp"


using namespace System;

namespace gemfire
{

  void ManagedCacheableDeltaBytesGeneric::toData( DataOutput& output ) const
  {
    GemStone::GemFire::Cache::Generic::Log::Debug("ManagedCacheableDeltaBytesGeneric::toData: current domain ID: " + System::Threading::Thread::GetDomainID() + " for object: " + System::Convert::ToString((int)this) + " with its domain ID: " + m_domainId );
    try {
      output.writeBytesOnly(m_bytes, m_size);
    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
  }

  Serializable* ManagedCacheableDeltaBytesGeneric::fromData( DataInput& input )
  {
    try {
       GemStone::GemFire::Cache::Generic::Log::Debug("ManagedCacheableDeltaBytes::fromData: classid " + m_classId);
       GemStone::GemFire::Cache::Generic::DataInput mg_input( &input, true );
       const uint8_t* objStartPos = input.currentBufferPosition();
         
       GemStone::GemFire::Cache::Generic::IGFSerializable^ obj =
         GemStone::GemFire::Cache::Generic::Serializable::GetTypeFactoryMethodGeneric(m_classId)();
       obj->FromData(%mg_input);
       input.advanceCursor(mg_input.BytesReadInternally);
    
       m_hashCode = obj->GetHashCode();

       const uint8_t* objEndPos = input.currentBufferPosition();
       
       //m_size = mg_input.BytesRead;
       m_size = (uint32_t)(objEndPos - objStartPos);
       GemStone::GemFire::Cache::Generic::Log::Debug("ManagedCacheableDeltaBytes::fromData: objectSize = " + m_size + " m_hashCode = " + m_hashCode);
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

  uint32_t ManagedCacheableDeltaBytesGeneric::objectSize( ) const
  {
    try {
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

  int32_t ManagedCacheableDeltaBytesGeneric::classId() const
  {
    uint32_t classId;
    try {
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

  int8_t ManagedCacheableDeltaBytesGeneric::typeId() const
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

  int8_t ManagedCacheableDeltaBytesGeneric::DSFID() const
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

  bool ManagedCacheableDeltaBytesGeneric::hasDelta()
  {
    //GemStone::GemFire::Cache::IGFDelta^ deltaObj = this->getManagedObject();
    //return deltaObj->HasDelta();
    return m_hasDelta;
  }

  void ManagedCacheableDeltaBytesGeneric::toDelta( DataOutput& output) const
  {
    try {
      GemStone::GemFire::Cache::Generic::Log::Debug("ManagedCacheableDeltaBytes::toDelta: current domain ID: " + System::Threading::Thread::GetDomainID() + " for object: " + System::Convert::ToString((int)this) + " with its domain ID: " + m_domainId);
      GemStone::GemFire::Cache::Generic::IGFDelta^ deltaObj = this->getManagedObject();
      GemStone::GemFire::Cache::Generic::DataOutput mg_output( &output, true );
      deltaObj->ToDelta( %mg_output );
      mg_output.WriteBytesToUMDataOutput();
    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) { 
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
  }

  void ManagedCacheableDeltaBytesGeneric::fromDelta( DataInput& input )
  {
    try {
      GemStone::GemFire::Cache::Generic::Log::Debug("ManagedCacheableDeltaBytes::fromDelta:");
      GemStone::GemFire::Cache::Generic::IGFDelta^ deltaObj = this->getManagedObject();
      GemStone::GemFire::Cache::Generic::DataInput mg_input( &input, true );
      deltaObj->FromDelta( %mg_input );

      GemStone::GemFire::Cache::Generic::IGFSerializable^ managedptr =
        dynamic_cast <GemStone::GemFire::Cache::Generic::IGFSerializable^> ( deltaObj );
      if(managedptr != nullptr)
      {
        GemStone::GemFire::Cache::Generic::Log::Debug("ManagedCacheableDeltaBytes::fromDelta: current domain ID: " + System::Threading::Thread::GetDomainID() + " for object: " + System::Convert::ToString((int)this) + " with its domain ID: " + m_domainId);
        GemStone::GemFire::Cache::Generic::Log::Debug("ManagedCacheableDeltaBytes::fromDelta: classid " + managedptr->ClassId + " : " + managedptr->ToString());
        gemfire::DataOutput dataOut;
        GemStone::GemFire::Cache::Generic::DataOutput mg_output( &dataOut, true);
        managedptr->ToData( %mg_output );

        //move cursor
        //dataOut.advanceCursor(mg_output.BufferLength);
        mg_output.WriteBytesToUMDataOutput();

        GF_SAFE_DELETE(m_bytes);
        m_bytes = dataOut.getBufferCopy();
        m_size = dataOut.getBufferLength();
        GemStone::GemFire::Cache::Generic::Log::Debug("ManagedCacheableDeltaBytes::fromDelta objectSize = " + m_size + " m_hashCode = " + m_hashCode);
        m_hashCode = managedptr->GetHashCode(); 
      }
    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
  }

  DeltaPtr ManagedCacheableDeltaBytesGeneric::clone()
  {
    try {
      GemStone::GemFire::Cache::Generic::IGFDelta^ deltaObj = this->getManagedObject();
      ICloneable^ cloneable = dynamic_cast< ICloneable^ >( ( GemStone::GemFire::Cache::Generic::IGFDelta^ ) deltaObj );
      if ( cloneable ) {
        GemStone::GemFire::Cache::Generic::IGFSerializable^ Mclone =
          dynamic_cast< GemStone::GemFire::Cache::Generic::IGFSerializable^ >( cloneable->Clone( ) );
        return DeltaPtr( static_cast< ManagedCacheableDeltaBytesGeneric* >(
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

  GemStone::GemFire::Cache::Generic::IGFDelta^
    ManagedCacheableDeltaBytesGeneric::getManagedObject() const
  {

    GemStone::GemFire::Cache::Generic::Log::Debug("ManagedCacheableDeltaBytes::getManagedObject");
    
    gemfire::DataInput dinp(m_bytes, m_size);
    GemStone::GemFire::Cache::Generic::DataInput mg_dinp(&dinp, true);
    GemStone::GemFire::Cache::Generic::TypeFactoryMethodGeneric^ creationMethod =
        GemStone::GemFire::Cache::Generic::Serializable::GetTypeFactoryMethodGeneric(m_classId);
    GemStone::GemFire::Cache::Generic::IGFSerializable^ newObj = creationMethod();

    GemStone::GemFire::Cache::Generic::IGFDelta^ managedDeltaptr =
      dynamic_cast <GemStone::GemFire::Cache::Generic::IGFDelta^> ( newObj->FromData(%mg_dinp) );
    return managedDeltaptr;
  }

  bool ManagedCacheableDeltaBytesGeneric::operator ==(const gemfire::CacheableKey& other) const
  {
    try {
      GemStone::GemFire::Cache::Generic::Log::Debug("ManagedCacheableDeltaBytesGeneric::equal");
      // now checking classId(), typeId(), DSFID() etc. will be much more
      // expensive than just a dynamic_cast
      const ManagedCacheableDeltaBytesGeneric* p_other =
        dynamic_cast<const ManagedCacheableDeltaBytesGeneric*>(&other);
      if (p_other != NULL) {
       gemfire::DataInput di(m_bytes, m_size);
        GemStone::GemFire::Cache::Generic::DataInput mg_input(&di, true);
        GemStone::GemFire::Cache::Generic::IGFSerializable^ obj =
          GemStone::GemFire::Cache::Generic::Serializable::GetTypeFactoryMethodGeneric(m_classId)();
        obj->FromData(%mg_input);
        bool ret = obj->Equals(p_other->ptr());
        GemStone::GemFire::Cache::Generic::Log::Debug("ManagedCacheableDeltaBytesGeneric::equal return VAL = " + ret);
        return ret;
      }
    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
    GemStone::GemFire::Cache::Generic::Log::Debug("ManagedCacheableDeltaBytesGeneric::equal returns false");
    return false;
  }

  bool ManagedCacheableDeltaBytesGeneric::operator ==(const ManagedCacheableDeltaBytesGeneric& other) const
  {
    try {
        GemStone::GemFire::Cache::Generic::Log::Debug("ManagedCacheableDeltaBytesGeneric::equal. ");
        gemfire::DataInput di(m_bytes, m_size);
        GemStone::GemFire::Cache::Generic::DataInput mg_input(&di, true);
        GemStone::GemFire::Cache::Generic::IGFSerializable^ obj =
          GemStone::GemFire::Cache::Generic::Serializable::GetTypeFactoryMethodGeneric(m_classId)();
        obj->FromData(%mg_input);
        bool ret = obj->Equals(other.ptr());
        GemStone::GemFire::Cache::Generic::Log::Debug("ManagedCacheableDeltaBytesGeneric::equal return VAL = " + ret);
        return ret;
        //return obj->Equals(other.ptr());
    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
    GemStone::GemFire::Cache::Generic::Log::Debug("ManagedCacheableDeltaBytesGeneric::equal return false");
    return false;
  }

  uint32_t ManagedCacheableDeltaBytesGeneric::hashcode( ) const
  {
    throw gcnew System::NotSupportedException;
  }

  size_t ManagedCacheableDeltaBytesGeneric::logString( char* buffer, size_t maxLength ) const
  {
    try {
       GemStone::GemFire::Cache::Generic::IGFDelta^ manageObject = getManagedObject();
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
}
