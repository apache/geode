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
#include <gfcpp/PartitionResolver.hpp>

#include "PartitionResolver.hpp"

using namespace System;

namespace gemfire {

  /// <summary>
  /// Wraps the managed <see cref="GemStone.GemFire.Cache.IPartitionResolver" />
  /// object and implements the native <c>gemfire::PartitionResolver</c> interface.
  /// </summary>
  class ManagedPartitionResolverGeneric
    : public PartitionResolver
  {
  public:

    /// <summary>
    /// Constructor to initialize with the provided managed object.
    /// </summary>
    /// <param name="userptr">
    /// The managed object.
    /// </param>
    inline ManagedPartitionResolverGeneric( Object^ userptr ) : m_userptr( userptr ) { }

    /// <summary>
    /// Static function to create a <c>ManagedPartitionResolver</c> using given
    /// managed assembly path and given factory function.
    /// </summary>
    /// <param name="assemblyPath">
    /// The path of the managed assembly that contains the <c>IPartitionResolver</c>
    /// factory function.
    /// </param>
    /// <param name="factoryFunctionName">
    /// The name of the factory function of the managed class for creating
    /// an object that implements <c>IPartitionResolver</c>.
    /// This should be a static function of the format
    /// {Namespace}.{Class Name}.{Method Name}.
    /// </param>
    /// <exception cref="IllegalArgumentException">
    /// If the managed library cannot be loaded or the factory function fails.
    /// </exception>
    static PartitionResolver* create( const char* assemblyPath,
      const char* factoryFunctionName );

    /// <summary>
    /// Destructor -- does nothing.
    /// </summary>
    virtual ~ManagedPartitionResolverGeneric( ) { }

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
    /// Returns the name of the PartitionResolver.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This function does not throw any exception.
    /// </para>
     /// <returns>
    /// the name of the PartitionResolver
    /// </returns>
    /// </remarks>
    virtual const char* getName();       

  
    /// <summary>
    /// Returns the wrapped managed object reference.
    /// </summary>
    inline GemStone::GemFire::Cache::Generic::IPartitionResolverProxy^ ptr() const
    {
      return m_managedptr;
    }

    inline void setptr(GemStone::GemFire::Cache::Generic::IPartitionResolverProxy^ managedptr)
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
    /// Note: not using auto_gcroot since it will result in 'Dispose' of the IPartitionResolver
    /// to be called which is not what is desired when this object is destroyed. Normally this
    /// managed object may be created by the user and will be handled automatically by the GC.
    /// </summary>
    gcroot<GemStone::GemFire::Cache::Generic::IPartitionResolverProxy^> m_managedptr;

    gcroot<Object^> m_userptr;
  };

}
