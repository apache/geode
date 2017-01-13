/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifdef CSTX_COMMENTED
#pragma once

#include "../gf_defs.hpp"
#include <vcclr.h>
#include <cppcache/TransactionListener.hpp>
#include "../ITransactionListener.hpp"



namespace gemfire {

  /// <summary>
  /// Wraps the managed <see cref="GemStone.GemFire.Cache.ITransactionListener" />
  /// object and implements the native <c>gemfire::TransactionListener</c> interface.
  /// </summary>
  class ManagedTransactionListenerGeneric
    : public gemfire::TransactionListener
  {
  public:

    /// <summary>
    /// Constructor to initialize with the provided managed object.
    /// </summary>
    /// <param name="userptr">
    /// The managed object.
    /// </param>
    inline ManagedTransactionListenerGeneric(Object^ userptr )
      : m_userptr( userptr ) { }

    static gemfire::TransactionListener* create( const char* assemblyPath,
      const char* factoryFunctionName );

    virtual ~ManagedTransactionListenerGeneric( ) { }
    
	  virtual void afterCommit(gemfire::TransactionEventPtr& te);

	  virtual void afterFailedCommit(gemfire::TransactionEventPtr& te);

	  virtual void afterRollback(gemfire::TransactionEventPtr& te);

	  virtual void close();

    inline GemStone::GemFire::Cache::ITransactionListener^ ptr( ) const
    {
      return m_managedptr;
    }

    inline void setptr( GemStone::GemFire::Cache::ITransactionListener^ managedptr )
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
    /// Note: not using auto_gcroot since it will result in 'Dispose' of the ITransactionListener
    /// to be called which is not what is desired when this object is destroyed. Normally this
    /// managed object may be created by the user and will be handled automatically by the GC.
    /// </summary>
    gcroot<GemStone::GemFire::Cache::ITransactionListener^> m_managedptr;

    gcroot<Object^> m_userptr;
  };

}
#endif