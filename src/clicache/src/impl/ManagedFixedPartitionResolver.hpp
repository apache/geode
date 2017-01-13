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
#include <gfcpp/FixedPartitionResolver.hpp>

#include "FixedPartitionResolver.hpp"

namespace gemfire {

  /// <summary>
  /// Wraps the managed <see cref="GemStone.GemFire.Cache.IFixedPartitionResolver" />
  /// object and implements the native <c>gemfire::FixedPartitionResolver</c> interface.
  /// </summary>
  class ManagedFixedPartitionResolverGeneric
    : public FixedPartitionResolver
  {
  public:

    /// <summary>
    /// Constructor to initialize with the provided managed object.
    /// </summary>
    /// <param name="userptr">
    /// The managed object.
    /// </param>
    inline ManagedFixedPartitionResolverGeneric( Object^ userptr ) : m_userptr( userptr ) { }

    /// <summary>
    /// Destructor -- does nothing.
    /// </summary>
    virtual ~ManagedFixedPartitionResolverGeneric( ) { }

    /// <summary>
    /// Static function to create a <c>ManagedFixedPartitionResolver</c> using given
    /// managed assembly path and given factory function.
    /// </summary>
    /// <param name="assemblyPath">
    /// The path of the managed assembly that contains the <c>IFixedPartitionResolver</c>
    /// factory function.
    /// </param>
    /// <param name="factoryFunctionName">
    /// The name of the factory function of the managed class for creating
    /// an object that implements <c>IFixedPartitionResolver</c>.
    /// This should be a static function of the format
    /// {Namespace}.{Class Name}.{Method Name}.
    /// </param>
    /// <exception cref="IllegalArgumentException">
    /// If the managed library cannot be loaded or the factory function fails.
    /// </exception>
    static PartitionResolver* create( const char* assemblyPath,
      const char* factoryFunctionName );

    /// <summary>
    /// return object associated with entry event which allows the Partitioned Region to store associated data together.
    /// </summary>
    /// <remarks>
    /// throws RuntimeException - any exception thrown will terminate the operation and the exception will be passed to the
    /// calling thread.
    /// </remarks>
    /// <param name="key">
    /// key the detail of the entry event.
    /// </param>

    virtual CacheableKeyPtr getRoutingObject(const EntryEvent& key); 

    /// <summary>
    /// Returns the name of the FixedPartitionResolver.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This function does not throw any exception.
    /// </para>
    /// <returns>
    /// the name of the FixedPartitionResolver
    /// </returns>
    /// </remarks>
    virtual const char* getName(); 


    /// <summary>
    /// This method is used to get the name of the partition for the given entry
    /// operation.
    /// <param name="opDetails">
    /// the details of the entry event e.g. {@link Region#get(Object)}
    /// </param>
    /// <returns>
    /// partition-name associated with node which allows mapping of given data to user defined partition.
    /// </returns>
    virtual const char* getPartitionName(const EntryEvent& opDetails);

  
    /// <summary>
    /// Returns the wrapped managed object reference.
    /// </summary>
    inline GemStone::GemFire::Cache::Generic::IFixedPartitionResolverProxy^ ptr( ) const
    {
      return m_managedptr;
    }

    inline void setptr(GemStone::GemFire::Cache::Generic::IFixedPartitionResolverProxy^ managedptr)
    {
      m_managedptr = managedptr;
    }

    inline Object^ userptr( ) const
    {
      return m_userptr;
    }

  private:

    /// <summary>
    /// Using gcroot to hold the managed delegate pointer (since it cannot be stored directly).
    /// Note: not using auto_gcroot since it will result in 'Dispose' of the IFixedPartitionResolver
    /// to be called which is not what is desired when this object is destroyed. Normally this
    /// managed object may be created by the user and will be handled automatically by the GC.
    /// </summary>
    gcroot<GemStone::GemFire::Cache::Generic::IFixedPartitionResolverProxy^> m_managedptr;

    gcroot<Object^> m_userptr;
  };

}
