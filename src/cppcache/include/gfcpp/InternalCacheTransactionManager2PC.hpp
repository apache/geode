#ifndef INTERNALCACHETRANSACTIONMANAGER2PC_H_
#define INTERNALCACHETRANSACTIONMANAGER2PC_H_

/*=========================================================================
 * Copyright (c) 2002-2015 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *
 * The specification of function behaviors is found in the corresponding .cpp
 *file.
 *
 *========================================================================
 */

#include "CacheTransactionManager.hpp"

namespace gemfire {

/**
 * Extension of the gemfire::CacheTransactionManager that enables client
 * application
 * to use Gemfire transaction as part of the global XA transaction.
 *
 * The prepare method of this class corresponds to the prepare phases of the
 * 2 phase commit protocol driven by a global transaction manager.
 *
 * The implementation of the gemfire::CacheTransactionManager commit() and
 * rollback()
 * methods must be 2 phase commit process aware.
 *
 * Methods of this class are expected to be called by a custom XA Resource
 * Manager
 * that is wrapping and adapting Gemfire client to XA specification
 * requirements.
 *
 * @since 8.3
 *
 */
class CPPCACHE_EXPORT InternalCacheTransactionManager2PC
    : public virtual gemfire::CacheTransactionManager {
 public:
  /**
   * Performs prepare during 2 phase commit completion.
   * Locks of the entries modified in the current transaction on the server
   * side.
   *
   * Calls to subsequent commit() or rollback() methods overridden by this class
   * are
   * expected to succeed after prepare() has returned successfully.
   * Gemfire commits internal transaction irreversibly on commit() call.
   *
   */
  virtual void prepare() = 0;

 protected:
  InternalCacheTransactionManager2PC();
  virtual ~InternalCacheTransactionManager2PC();
};
}

#endif /* INTERNALCACHETRANSACTIONMANAGER2PC_H_ */
