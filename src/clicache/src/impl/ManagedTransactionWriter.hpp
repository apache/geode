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
#ifdef CSTX_COMMENTED
#pragma once

#include "../gf_defs.hpp"
#include <vcclr.h>
#include <cppcache/TransactionWriter.hpp>
#include "../ITransactionWriter.hpp"

namespace GemStone {

  namespace GemFire {

    namespace Cache {

      interface class ITransactionWriter;
    }
  }
}

namespace gemfire {

  /// <summary>
  /// Wraps the managed <see cref="GemStone.GemFire.Cache.ITransactionWriter" />
  /// object and implements the native <c>gemfire::TransactionWriter</c> interface.
  /// </summary>
  class ManagedTransactionWriterGeneric
    : public gemfire::TransactionWriter
  {
  public:

    /// <summary>
    /// Constructor to initialize with the provided managed object.
    /// </summary>
    /// <param name="userptr">
    /// The managed object.
    /// </param>
    inline ManagedTransactionWriterGeneric(Object^ userptr )
      : m_userptr( userptr ) { }

    static gemfire::TransactionWriter* create( const char* assemblyPath,
      const char* factoryFunctionName );

    virtual ~ManagedTransactionWriterGeneric( ) { }
    
	  virtual void beforeCommit(gemfire::TransactionEventPtr& te);

	  inline GemStone::GemFire::Cache::ITransactionWriter^ ptr( ) const
    {
      return m_managedptr;
    }

    inline void setptr( GemStone::GemFire::Cache::ITransactionWriter^ managedptr )
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
    /// Note: not using auto_gcroot since it will result in 'Dispose' of the ITransactionWriter
    /// to be called which is not what is desired when this object is destroyed. Normally this
    /// managed object may be created by the user and will be handled automatically by the GC.
    /// </summary>
    gcroot<GemStone::GemFire::Cache::ITransactionWriter^> m_managedptr;

    gcroot<Object^> m_userptr;
  };

}
#endif