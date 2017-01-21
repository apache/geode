/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      interface class IPdxSerializable;
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache


namespace apache
{
  namespace geode
  {
    namespace client
    {

  /// <summary>
  /// Wraps the managed <see cref="Apache.Geode.Client.IGFSerializable" />
  /// object and implements the native <c>apache::geode::client::CacheableKey</c> interface.
  /// </summary>
  class PdxManagedCacheableKeyBytes
    : public apache::geode::client::CacheableKey, public Delta
  {
  public:

    /// <summary>
    /// Constructor to initialize with the provided managed object.
    /// </summary>
    /// <param name="managedptr">
    /// The managed object.
    /// </param>
    inline PdxManagedCacheableKeyBytes(
      Apache::Geode::Client::IPdxSerializable^ managedptr, bool storeBytes )
      : m_domainId(System::Threading::Thread::GetDomainID()),
        m_bytes(NULL),
        m_size(0),
        m_hashCode(0)
    {
      m_hasDelta = false;
      if(storeBytes)
      {
        Apache::Geode::Client::IGFDelta^ deltaObj = dynamic_cast<Apache::Geode::Client::IGFDelta^>(managedptr);

        if(deltaObj != nullptr)
          m_hasDelta = deltaObj->HasDelta();
      }
      if(managedptr != nullptr)
      {
        if(storeBytes)//if value is from app 
        {
          apache::geode::client::DataOutput dataOut;
          Apache::Geode::Client::DataOutput mg_output( &dataOut, true);
					 Apache::Geode::Client::Internal::PdxHelper::SerializePdx(%mg_output, managedptr);
        //  managedptr->ToData( %mg_output );
          
          //move cursor
          //dataOut.advanceCursor(mg_output.BufferLength);
					mg_output.WriteBytesToUMDataOutput();

          m_bytes = dataOut.getBufferCopy();
          m_size = dataOut.getBufferLength();
          
          m_hashCode = managedptr->GetHashCode(); 
          Apache::Geode::Client::Log::Fine(
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
    virtual void toData( apache::geode::client::DataOutput& output ) const;

    /// <summary>
    /// deserialize this object, typical implementation should return
    /// the 'this' pointer.
    /// </summary>
    virtual apache::geode::client::Serializable* fromData( apache::geode::client::DataInput& input );
    
    virtual void toDelta( apache::geode::client::DataOutput& output) const;

    virtual void fromDelta( apache::geode::client::DataInput& input );

    virtual bool hasDelta();

    virtual apache::geode::client::DeltaPtr clone();

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
    virtual apache::geode::client::CacheableStringPtr toString( ) const;

    /// <summary>
    /// return true if this key matches other CacheableKey
    /// </summary>
    virtual bool operator == ( const apache::geode::client::CacheableKey& other ) const;
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
    inline Apache::Geode::Client::IPdxSerializable^ ptr( ) const
    {
      return getManagedObject();
    }
    
		static Serializable* CreateDeserializable()
    {
      return new PdxManagedCacheableKeyBytes();
    }

    inline ~PdxManagedCacheableKeyBytes()
    {
      Apache::Geode::Client::Log::Fine(
        "ManagedCacheableKeyBytes::Destructor current AppDomain ID: " +
        System::Threading::Thread::GetDomainID() + " for object: " +
        System::Convert::ToString((int)this) + " with its AppDomain ID: " + m_domainId);
      GF_SAFE_DELETE(m_bytes);
    }

  private:

    Apache::Geode::Client::IPdxSerializable^ getManagedObject() const;
    
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

    }  // namespace client
  }  // namespace geode
}  // namespace apache

