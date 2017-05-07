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
#include <cppcache/CacheableKey.hpp>
#include "cppcache/impl/GemfireTypeIdsImpl.hpp"
#include "../IGFSerializable.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      interface class IGFSerializable;
    }
  }
}

namespace gemfire
{

  /// <summary>
  /// Wraps the managed <see cref="GemStone.GemFire.Cache.IGFSerializable" />
  /// object and implements the native <c>gemfire::CacheableKey</c> interface.
  /// </summary>
  class ManagedCacheableKey
    : public CacheableKey
  {
  private :
    int m_hashcode;
    int m_classId;
  public:

    inline ManagedCacheableKey(
      GemStone::GemFire::Cache::IGFSerializable^ managedptr, int hashcode, int classId )
      : m_managedptr( managedptr ) { 
        m_hashcode = hashcode;
        m_classId = classId;
    }
    /// <summary>
    /// Constructor to initialize with the provided managed object.
    /// </summary>
    /// <param name="managedptr">
    /// The managed object.
    /// </param>
    inline ManagedCacheableKey(
      GemStone::GemFire::Cache::IGFSerializable^ managedptr )
      : m_managedptr( managedptr ) { 
        m_hashcode = 0;//it can be zero while initializing the object
        uint32_t classId = managedptr->ClassId;       
    }

    /// <summary>
    /// serialize this object
    /// </summary>
    virtual void toData( DataOutput& output ) const;

    /// <summary>
    /// deserialize this object, typical implementation should return
    /// the 'this' pointer.
    /// </summary>
    virtual Serializable* fromData( DataInput& input );
    
    
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
    virtual CacheableStringPtr toString( ) const;

    /// <summary>
    /// return true if this key matches other CacheableKey
    /// </summary>
    virtual bool operator == ( const CacheableKey& other ) const;
    /// <summary>
    /// return true if this key matches other ManagedCacheableKey
    /// </summary>
    virtual bool operator == ( const ManagedCacheableKey& other ) const;

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
    inline GemStone::GemFire::Cache::IGFSerializable^ ptr( ) const
    {
      return m_managedptr;
    }


  private:

    /// <summary>
    /// Using gcroot to hold the managed delegate pointer (since it cannot be stored directly).
    /// Note: not using auto_gcroot since it will result in 'Dispose' of the IGFSerializable
    /// to be called which is not what is desired when this object is destroyed. Normally this
    /// managed object may be created by the user and will be handled automatically by the GC.
    /// </summary>
    gcroot<GemStone::GemFire::Cache::IGFSerializable^> m_managedptr;

    // Disable the copy and assignment constructors
    ManagedCacheableKey( const ManagedCacheableKey& );
    ManagedCacheableKey& operator = ( const ManagedCacheableKey& );
  };

}
