#ifndef CACHETRANSACTIONMANAGER_H_
#define CACHETRANSACTIONMANAGER_H_

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *
 * The specification of function behaviors is found in the corresponding .cpp
 *file.
 *
 *========================================================================
 */

//#### Warning: DO NOT directly include Region.hpp, include Cache.hpp instead.

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"

namespace gemfire {

class CPPCACHE_EXPORT CacheTransactionManager : public gemfire::SharedBase {
 public:
  /** Creates a new transaction and associates it with the current thread.
   *
   * @throws IllegalStateException if the thread is already associated with a
   * transaction
   *
   * @since 3.6
   */
  virtual void begin() = 0;

  /** Commit the transaction associated with the current thread. If
   *  the commit operation fails due to a conflict it will destroy
   *  the transaction state and throw a {@link
   *  CommitConflictException}.  If the commit operation succeeds,
   *  it returns after the transaction state has been merged with
   *  committed state.  When this method completes, the thread is no
   *  longer associated with a transaction.
   *
   * @throws IllegalStateException if the thread is not associated with a
   * transaction
   *
   * @throws CommitConflictException if the commit operation fails due to
   *   a write conflict.
   *
   * @throws TransactionDataNodeHasDepartedException if the node hosting the
   * transaction data has departed. This is only relevant for transaction that
   * involve PartitionedRegions.
   *
   * @throws TransactionDataNotColocatedException if at commit time, the data
   * involved in the transaction has moved away from the transaction hosting
   * node. This can only happen if rebalancing/recovery happens during a
   * transaction that involves a PartitionedRegion.
   *
   * @throws TransactionInDoubtException when GemFire cannot tell which nodes
   * have applied the transaction and which have not. This only occurs if nodes
   * fail mid-commit, and only then in very rare circumstances.
   */
  virtual void commit() = 0;

  /** Roll back the transaction associated with the current thread. When
   *  this method completes, the thread is no longer associated with a
   *  transaction and the transaction context is destroyed.
   *
   * @since 3.6
   *
   * @throws IllegalStateException if the thread is not associated with a
   * transaction
   */
  virtual void rollback() = 0;

  /**
   * Suspends the transaction on the current thread. All subsequent operations
   * performed by this thread will be non-transactional. The suspended
   * transaction can be resumed by calling {@link #resume(TransactionId)}
   *
   * @return the transaction identifier of the suspended transaction or null if
   *         the thread was not associated with a transaction
   * @since 3.6.2
   */
  virtual TransactionIdPtr suspend() = 0;

  /**
  * On the current thread, resumes a transaction that was previously suspended
  * using {@link #suspend()}
  *
  * @param transactionId
  *          the transaction to resume
  * @throws IllegalStateException
  *           if the thread is associated with a transaction or if
  *           {@link #isSuspended(TransactionId)} would return false for the
  *           given transactionId
  * @since 3.6.2
  */
  virtual void resume(TransactionIdPtr transactionId) = 0;

  /**
  * This method can be used to determine if a transaction with the given
  * transaction identifier is currently suspended locally. This method does not
  * check other members for transaction status.
  *
  * @param transactionId
  * @return true if the transaction is in suspended state, false otherwise
  * @since 3.6.2
  * @see #exists(TransactionId)
  */
  virtual bool isSuspended(TransactionIdPtr transactionId) = 0;

  /**
   * On the current thread, resumes a transaction that was previously suspended
   * using {@link #suspend()}.
   *
   * This method is equivalent to
   * <pre>
   * if (isSuspended(txId)) {
   *   resume(txId);
   * }
   * </pre>
   * except that this action is performed atomically
   *
   * @param transactionId
   *          the transaction to resume
   * @return true if the transaction was resumed, false otherwise
   * @since 3.6.2
   */
  virtual bool tryResume(TransactionIdPtr transactionId) = 0;

  /**
   * On the current thread, resumes a transaction that was previously suspended
   * using {@link #suspend()}, or waits for the specified timeout interval if
   * the transaction has not been suspended. This method will return if:
   * <ul>
   * <li>Another thread suspends the transaction</li>
   * <li>Another thread calls commit/rollback on the transaction</li>
   * <li>This thread has waited for the specified timeout</li>
   * </ul>
   *
   * This method returns immediately if {@link #exists(TransactionId)} returns
   * false.
   *
   * @param transactionId
   *          the transaction to resume
   * @param waitTimeInMilliSec
   *          the maximum milliseconds to wait
   * @return true if the transaction was resumed, false otherwise
   * @since 3.6.2
   * @see #tryResume(TransactionId)
   */
  virtual bool tryResume(TransactionIdPtr transactionId,
                         int32_t waitTimeInMilliSec) = 0;

  /**
   * Reports the existence of a transaction for the given transactionId. This
   * method can be used to determine if a transaction with the given transaction
   * identifier is currently in progress locally.
   *
   * @param transactionId
   *          the given transaction identifier
   * @return true if the transaction is in progress, false otherwise.
   * @since 3.6.2
   * @see #isSuspended(TransactionId)
   */
  virtual bool exists(TransactionIdPtr transactionId) = 0;

  /** Returns the transaction identifier for the current thread
  *
  * @return the transaction identifier or null if no transaction exists
  *
  * @since 3.6.2
  */
  virtual TransactionIdPtr getTransactionId() = 0;

  /** Reports the existence of a Transaction for this thread
   *
   * @return true if a transaction exists, false otherwise
   *
   * @since 3.6
   */
  virtual bool exists() = 0;

 protected:
  CacheTransactionManager();
  virtual ~CacheTransactionManager();
};
}

#endif /* CACHETRANSACTIONMANAGER_H_ */
