/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "../gf_defs.hpp"
#include <vcclr.h>
#include <gfcpp/Delta.hpp>
#include "../Log.hpp"
#include "../DataOutput.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      interface class IGFSerializable;
      interface class IGFDelta;
    }
  }
}
}

namespace gemfire
{

  /// <summary>
  /// Wraps the managed <see cref="GemStone.GemFire.Cache.IGFDelta" />
  /// object and implements the native <c>gemfire::CacheableKey</c> interface.
  /// </summary>
  class ManagedCacheableDeltaBytesGeneric
    : public CacheableKey, public Delta
  {
  public:

    /// <summary>
    /// Constructor to initialize with the provided managed object.
    /// </summary>
    /// <param name="managedDeltaptr">
    /// The managed object.
    /// </param>
    inline ManagedCacheableDeltaBytesGeneric(
      GemStone::GemFire::Cache::Generic::IGFDelta^ managedDeltaptr,  bool storeBytes )
      : m_domainId(System::Threading::Thread::GetDomainID()),
        m_classId(0),
        m_bytes(NULL),
        m_size(0),
        m_hasDelta(false),
        m_hashCode(0)
    {
      if(storeBytes)
        m_hasDelta = managedDeltaptr->HasDelta();
      GemStone::GemFire::Cache::Generic::IGFSerializable^ managedptr =
        dynamic_cast <GemStone::GemFire::Cache::Generic::IGFSerializable^> ( managedDeltaptr );
      if(managedptr != nullptr)
      {
        m_classId = managedptr->ClassId;
        GemStone::GemFire::Cache::Generic::Log::Finer("ManagedCacheableDeltaBytes::Constructor: current AppDomain ID: " + System::Threading::Thread::GetDomainID() + " for object: " + System::Convert::ToString((int)this) + " with its AppDomain ID: " + m_domainId);
        GemStone::GemFire::Cache::Generic::Log::Finer("ManagedCacheableDeltaBytes::Constructor: class ID " + managedptr->ClassId + " : " + managedptr->ToString() + " storeBytes:" + storeBytes);
        if(storeBytes)
        {
          gemfire::DataOutput dataOut;
          GemStone::GemFire::Cache::Generic::DataOutput mg_output( &dataOut, true);
          managedptr->ToData( %mg_output );
          
          //move cursor
          //dataOut.advanceCursor(mg_output.BufferLength);
          mg_output.WriteBytesToUMDataOutput();

          m_bytes = dataOut.getBufferCopy();
          m_size = dataOut.getBufferLength();
          m_hashCode = managedptr->GetHashCode(); 
          GemStone::GemFire::Cache::Generic::Log::Finer("ManagedCacheableDeltaBytes::Constructor objectSize = " + m_size + " m_hashCode = " + m_hashCode);
        }
      }
    }
/*
    inline ManagedCacheableDeltaBytes(
      GemStone::GemFire::Cache::IGFDelta^ managedDeltaptr,  bool storeBytes)
      : m_domainId(System::Threading::Thread::GetDomainID()),
        m_classId(0),
        m_bytes(NULL),
        m_size(0),
        m_hashCode(0)
    {
      GemStone::GemFire::Cache::Log::Fine("ManagedCacheableDeltaBytes::Constructor: not storing bytes ");
      GemStone::GemFire::Cache::IGFSerializable^ managedptr = dynamic_cast <GemStone::GemFire::Cache::IGFSerializable^> ( managedDeltaptr );
      if(managedptr != nullptr)
      {
        m_classId = managedptr->ClassId;
        GemStone::GemFire::Cache::Log::Fine("ManagedCacheableDeltaBytes::Constructor: current AppDomain ID: " + System::Threading::Thread::GetDomainID() + " for object: " + System::Convert::ToString((int)this) + " with its AppDomain ID: " + m_domainId);
        GemStone::GemFire::Cache::Log::Fine("ManagedCacheableDeltaBytes::Constructor: class ID " + managedptr->ClassId + " : " + managedptr->ToString());
        gemfire::DataOutput dataOut;
        GemStone::GemFire::Cache::DataOutput mg_output( &dataOut);
        managedptr->ToData( %mg_output );
        m_bytes = dataOut.getBufferCopy();
        m_size = dataOut.getBufferLength();
        GemStone::GemFire::Cache::Log::Fine("ManagedCacheableDeltaBytes::Constructor objectSize = " + m_size + " m_hashCode = " + m_hashCode);
        m_hashCode = managedptr->GetHashCode(); 
      }
    }*/


    /// <summary>
    /// serialize this object
    /// </summary>
    virtual void toData( gemfire::DataOutput& output ) const;

    /// <summary>
    /// deserialize this object, typical implementation should return
    /// the 'this' pointer.
    /// </summary>
    virtual gemfire::Serializable* fromData( gemfire::DataInput& input );

    virtual void toDelta( gemfire::DataOutput& output) const;

    virtual void fromDelta( gemfire::DataInput& input );

    /// <summary>
    /// return the size of this object in bytes
    /// </summary>
    virtual uint32_t objectSize() const;

    /// <summary>
    /// return the classId of the instance being serialized.
    /// This is used by deserialization to determine what instance
    /// type to create and deserialize into.
    /// </summary>
    virtual int32_t classId( ) const;

    /// <summary>
    /// return the typeId of the instance being serialized.
    /// This is used by deserialization to determine what instance
    /// type to create and deserialize into.
    /// </summary>
    virtual int8_t typeId( ) const;

    /// <summary>
    /// return the Data Serialization Fixed ID type.
    /// This is used to determine what instance type to create
    /// and deserialize into.
    ///
    /// Note that this should not be overridden by custom implementations
    /// and is reserved only for builtin types.
    /// </summary>
    virtual int8_t DSFID() const;

    virtual bool hasDelta();

    virtual gemfire::DeltaPtr clone();

    /// <summary>
    /// return the hashcode for this key.
    /// </summary>
    virtual uint32_t hashcode( ) const;

    /// <summary>
    /// return true if this key matches other CacheableKey
    /// </summary>
    virtual bool operator == ( const CacheableKey& other ) const;

    /// <summary>
    /// return true if this key matches other ManagedCacheableDeltaBytesGeneric
    /// </summary>
    virtual bool operator == ( const ManagedCacheableDeltaBytesGeneric& other ) const;

    /// <summary>
    /// Copy the string form of a key into a char* buffer for logging purposes.
    /// implementations should only generate a string as long as maxLength chars,
    /// and return the number of chars written. buffer is expected to be large 
    /// enough to hold at least maxLength chars.
    /// The default implementation renders the classname and instance address.
    /// </summary>
    virtual size_t logString( char* buffer, size_t maxLength ) const;

    /// <summary>
    /// Returns the wrapped managed object reference.
    /// </summary>
    inline GemStone::GemFire::Cache::Generic::IGFDelta^ ptr( ) const
    {
      return getManagedObject();
    }

    inline ~ManagedCacheableDeltaBytesGeneric()
    {
      GemStone::GemFire::Cache::Generic::Log::Finer("ManagedCacheableDeltaBytes::Destructor current AppDomain ID: " + System::Threading::Thread::GetDomainID() + " for object: " + System::Convert::ToString((int)this) + " with its AppDomain ID: " + m_domainId);
      GF_SAFE_DELETE(m_bytes);
    }

  private:
    GemStone::GemFire::Cache::Generic::IGFDelta^ getManagedObject() const;
    /// <summary>
    /// Using gcroot to hold the managed delegate pointer (since it cannot be stored directly).
    /// Note: not using auto_gcroot since it will result in 'Dispose' of the IGFDelta
    /// to be called which is not what is desired when this object is destroyed. Normally this
    /// managed object may be created by the user and will be handled automatically by the GC.
    /// </summary>
    //gcroot<GemStone::GemFire::Cache::IGFDelta^> m_managedptr;
    //gcroot<GemStone::GemFire::Cache::IGFSerializable^> m_managedSerializableptr;

    int m_domainId;
    UInt32 m_classId;
    uint8_t * m_bytes;
    uint32_t m_size;
    uint32_t m_hashCode;
    bool m_hasDelta;

    // Disable the copy and assignment constructors
    ManagedCacheableDeltaBytesGeneric( const ManagedCacheableDeltaBytesGeneric& );
    ManagedCacheableDeltaBytesGeneric& operator = ( const ManagedCacheableDeltaBytesGeneric& );
  };

}
