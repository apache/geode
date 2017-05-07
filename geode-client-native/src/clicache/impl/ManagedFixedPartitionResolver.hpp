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
#include <cppcache/FixedPartitionResolver.hpp>
#include "ManagedPartitionResolver.hpp"


namespace GemStone {

  namespace GemFire {

    namespace Cache {

      interface class IFixedPartitionResolver;
    }
  }
}

namespace gemfire {

  /// <summary>
  /// Wraps the managed <see cref="GemStone.GemFire.Cache.IFixedPartitionResolver" />
  /// object and implements the native <c>gemfire::FixedPartitionResolver</c> interface.
  /// </summary>
  class ManagedFixedPartitionResolver
    : public FixedPartitionResolver
  {
  public:

    /// <summary>
    /// Constructor to initialize with the provided managed object.
    /// </summary>
    /// <param name="managedptr">
    /// The managed object.
    /// </param>
    inline ManagedFixedPartitionResolver(
      GemStone::GemFire::Cache::IFixedPartitionResolver^ managedptr )
      : m_managedptr( managedptr ) { }

    /// <summary>
    /// Destructor -- does nothing.
    /// </summary>
    virtual ~ManagedFixedPartitionResolver( ) { }

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
    /// <param name="targetPartitions">
    /// represents all the available primary partitions on the nodes
    /// </param> 
    /// <returns>
    /// partition-name associated with node which allows mapping of given data to user defined partition.
    /// </returns>
    virtual const char* getPartitionName(const EntryEvent& opDetails,
      CacheableHashSetPtr targetPartitions);

  
    /// <summary>
    /// Returns the wrapped managed object reference.
    /// </summary>
    inline GemStone::GemFire::Cache::IFixedPartitionResolver^ ptr( ) const
    {
      return m_managedptr;
    }


  private:

    /// <summary>
    /// Using gcroot to hold the managed delegate pointer (since it cannot be stored directly).
    /// Note: not using auto_gcroot since it will result in 'Dispose' of the IFixedPartitionResolver
    /// to be called which is not what is desired when this object is destroyed. Normally this
    /// managed object may be created by the user and will be handled automatically by the GC.
    /// </summary>
    gcroot<GemStone::GemFire::Cache::IFixedPartitionResolver^> m_managedptr;
  };

}
