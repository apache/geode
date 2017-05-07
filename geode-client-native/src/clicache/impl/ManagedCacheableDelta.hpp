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
#include <cppcache/Delta.hpp>
#include "../IGFDelta.hpp"
#include "../IGFSerializable.hpp"

using namespace System;

//namespace GemStone
//{
//  namespace GemFire
//  {
//    namespace Cache
//    {
//      interface class IGFSerializable;
//      interface class IGFDelta;
//    }
//  }
//}

namespace gemfire
{

  /// <summary>
  /// Wraps the managed <see cref="GemStone.GemFire.Cache.IGFDelta" />
  /// object and implements the native <c>gemfire::CacheableKey</c> interface.
  /// </summary>
  class ManagedCacheableDelta
    : public Cacheable, public Delta
  {
  private :
    int m_hashcode;
    int m_classId;
  public:

    /// <summary>
    /// Constructor to initialize with the provided managed object.
    /// </summary>
    /// <param name="managedptr">
    /// The managed object.
    /// </param>
    inline ManagedCacheableDelta(
      GemStone::GemFire::Cache::IGFDelta^ managedptr )
      : m_managedptr( managedptr )
    {
      m_hashcode = 0;
      m_managedSerializableptr = dynamic_cast <GemStone::GemFire::Cache::IGFSerializable^> ( managedptr );
      m_classId = m_managedSerializableptr->ClassId;
    }

    inline ManagedCacheableDelta(
      GemStone::GemFire::Cache::IGFDelta^ managedptr, int hashcode, int classId )
      : m_managedptr( managedptr ) { 
        m_hashcode = hashcode;
        m_classId = classId;
        m_managedSerializableptr = dynamic_cast <GemStone::GemFire::Cache::IGFSerializable^> ( managedptr );
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

    virtual void toDelta( DataOutput& output) const;

    virtual void fromDelta( DataInput& input );

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

    virtual DeltaPtr clone();


    /// <summary>
    /// Returns the wrapped managed object reference.
    /// </summary>
    inline GemStone::GemFire::Cache::IGFDelta^ ptr( ) const
    {
      return m_managedptr;
    }


  private:

    /// <summary>
    /// Using gcroot to hold the managed delegate pointer (since it cannot be stored directly).
    /// Note: not using auto_gcroot since it will result in 'Dispose' of the IGFDelta
    /// to be called which is not what is desired when this object is destroyed. Normally this
    /// managed object may be created by the user and will be handled automatically by the GC.
    /// </summary>
    gcroot<GemStone::GemFire::Cache::IGFDelta^> m_managedptr;
    gcroot<GemStone::GemFire::Cache::IGFSerializable^> m_managedSerializableptr;
    // Disable the copy and assignment constructors
    ManagedCacheableDelta( const ManagedCacheableDelta& );
    ManagedCacheableDelta& operator = ( const ManagedCacheableDelta& );
  };

}
