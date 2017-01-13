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
#include <gfcpp/CacheableKey.hpp>
#include "../Log.hpp"
#include "../DataOutput.hpp"
#include "PdxHelper.hpp"
#include <gfcpp/Delta.hpp>

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      interface class IPdxSerializable;
    }
  }
}
}

namespace gemfire
{

  /// <summary>
  /// Wraps the managed <see cref="GemStone.GemFire.Cache.IGFSerializable" />
  /// object and implements the native <c>gemfire::CacheableKey</c> interface.
  /// </summary>
  class PdxManagedCacheableKeyBytes
    : public gemfire::CacheableKey, public Delta
  {
  public:

    /// <summary>
    /// Constructor to initialize with the provided managed object.
    /// </summary>
    /// <param name="managedptr">
    /// The managed object.
    /// </param>
    inline PdxManagedCacheableKeyBytes(
      GemStone::GemFire::Cache::Generic::IPdxSerializable^ managedptr, bool storeBytes )
      : m_domainId(System::Threading::Thread::GetDomainID()),
        m_bytes(NULL),
        m_size(0),
        m_hashCode(0)
    {
      m_hasDelta = false;
      if(storeBytes)
      {
        GemStone::GemFire::Cache::Generic::IGFDelta^ deltaObj = dynamic_cast<GemStone::GemFire::Cache::Generic::IGFDelta^>(managedptr);

        if(deltaObj != nullptr)
          m_hasDelta = deltaObj->HasDelta();
      }
      if(managedptr != nullptr)
      {
        if(storeBytes)//if value is from app 
        {
          gemfire::DataOutput dataOut;
          GemStone::GemFire::Cache::Generic::DataOutput mg_output( &dataOut, true);
					 GemStone::GemFire::Cache::Generic::Internal::PdxHelper::SerializePdx(%mg_output, managedptr);
        //  managedptr->ToData( %mg_output );
          
          //move cursor
          //dataOut.advanceCursor(mg_output.BufferLength);
					mg_output.WriteBytesToUMDataOutput();

          m_bytes = dataOut.getBufferCopy();
          m_size = dataOut.getBufferLength();
          
          m_hashCode = managedptr->GetHashCode(); 
          GemStone::GemFire::Cache::Generic::Log::Fine(
           "PdxManagedCacheableKeyBytes::Constructor objectSize = " + m_size + " m_hashCode = " + m_hashCode);
        }
      }
    }

		inline PdxManagedCacheableKeyBytes( )
      : m_domainId(System::Threading::Thread::GetDomainID()),
        m_bytes(NULL),
        m_size(0),
        m_hashCode(0)
    {
      m_hasDelta = false;
		}

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

    virtual bool hasDelta();

    virtual gemfire::DeltaPtr clone();

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

    /// <summary>
    /// Display this object as 'string', which depends on the implementation in
    /// the managed class
    /// </summary>
    virtual gemfire::CacheableStringPtr toString( ) const;

    /// <summary>
    /// return true if this key matches other CacheableKey
    /// </summary>
    virtual bool operator == ( const gemfire::CacheableKey& other ) const;
    /// <summary>
    /// return true if this key matches other ManagedCacheableKeyBytes
    /// </summary>
    virtual bool operator == ( const PdxManagedCacheableKeyBytes& other ) const;

    /// <summary>
    /// return the hashcode for this key.
    /// </summary>
    virtual uint32_t hashcode( ) const;

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
    inline GemStone::GemFire::Cache::Generic::IPdxSerializable^ ptr( ) const
    {
      return getManagedObject();
    }
    
		static Serializable* CreateDeserializable()
    {
      return new PdxManagedCacheableKeyBytes();
    }

    inline ~PdxManagedCacheableKeyBytes()
    {
      GemStone::GemFire::Cache::Generic::Log::Fine(
        "ManagedCacheableKeyBytes::Destructor current AppDomain ID: " +
        System::Threading::Thread::GetDomainID() + " for object: " +
        System::Convert::ToString((int)this) + " with its AppDomain ID: " + m_domainId);
      GF_SAFE_DELETE(m_bytes);
    }

  private:

    GemStone::GemFire::Cache::Generic::IPdxSerializable^ getManagedObject() const;
    
    /// <summary>
    /// Using gcroot to hold the managed delegate pointer (since it cannot be stored directly).
    /// Note: not using auto_gcroot since it will result in 'Dispose' of the IGFSerializable
    /// to be called which is not what is desired when this object is destroyed. Normally this
    /// managed object may be created by the user and will be handled automatically by the GC.
    /// </summary>
//    gcroot<IGFSerializable^> m_managedptr;
    int m_domainId;
    UInt32 m_classId;
    uint8_t * m_bytes;
    uint32_t m_size;
    bool m_hasDelta;
    uint32_t m_hashCode;
    // Disable the copy and assignment constructors
    PdxManagedCacheableKeyBytes( const PdxManagedCacheableKeyBytes& );
    PdxManagedCacheableKeyBytes& operator = ( const PdxManagedCacheableKeyBytes& );
  };

}
