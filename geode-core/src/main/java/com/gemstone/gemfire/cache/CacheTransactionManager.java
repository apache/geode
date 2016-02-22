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

package com.gemstone.gemfire.cache;

import java.util.concurrent.TimeUnit;


/** <p>The CacheTransactionManager interface allows applications to manage
 * transactions on a per {@link Cache} basis.  
 * 
 * <p>The life cycle of a GemFire transaction starts with a begin
 * operation. The life cycle ends with either a commit or rollback
 * operation.  Between the begin and the commit/rollback are typically
 * {@link Region} operations.  In general, those that either create,
 * destroy, invalidate or update {@link Region.Entry} are considered
 * transactional, that is they modify transactional state.
 * 
 * <p>A GemFire transaction may involve operations on multiple regions,
 * each of which may have different attributes.
 * 
 * <p>While a GemFire transaction and its operations are invoked in
 * the local VM, the resulting transaction state is distributed to
 * other VM's at commit time as per the attributes of each
 * participant Region.
 * 
 * <p>A transaction can have no more than one thread associated with
 * it and conversely a thread can only operate on one transaction at
 * any given time. Child threads will not inherit the existing
 * transaction.
 *
 * <p>Each of the following methods operate on the current thread.  All
 * methods throw {@link CacheClosedException} if the Cache is closed.
 * 
 * <p>GemFire Transactions currently only support Read Committed
 * isolation.  In addition, they are optimistic transactions in that
 * write locking and conflict checks are performed as part of the
 * commit operation.
 *
 * <p>For guaranteed Read Committed isolation, avoid making "in place"
 * changes, because such changes will be "seen" by other transactions
 * and break the Read Committed isolation guarantee. e.g.
 *
 *  <pre>
 *    CacheTransactionManager txMgr = cache.getCacheTransactionManager();
 *    txMgr.begin();
 *    StringBuffer s = (StringBuffer) r.get("stringBuf");
 *    s.append("Changes seen before commit. NOT Read Committed!");
 *    r.put("stringBuf", s);
 *    txMgr.commit();
 *  </pre>
 * 
 *  <p>To aid in creating copies, the "copy on read"
 *  <code>Cache</code> attribute and the {@link
 *  com.gemstone.gemfire.CopyHelper#copy} method are provided.
 *  The following is a Read Committed safe example using the
 *  <code>CopyHelper.copy</code> method.
 * 
 *  <pre>
 *    CacheTransactionManager txMgr = cache.getCacheTransactionManager();
 *    txMgr.begin();
 *    Object o = r.get("stringBuf");
 *    StringBuffer s = (StringBuffer) CopyHelper.copy(o);
 *    s.append("Changes unseen before commit. Read Committed.");
 *    r.put("stringBuf", s);
 *    txMgr.commit();
 *  </pre>
 *
 *  <p>Its important to note that creating copies can negatively
 *  impact both performance and memory consumption.
 *
 * <p>Partitioned Regions, Distributed No Ack and Distributed Ack Regions are supported
 * (see {@link AttributesFactory} for Scope).  For both scopes, a
 * consistent configuration (per VM) is enforced.
 * 
 * <p>Global Regions, client Regions (see com.gemstone.gemfire.cache.client package)
 * and persistent Regions (see {@link DiskWriteAttributes}) do not
 * support transactions.
 * 
 * <p>When PartitionedRegions are involved in a transaction, all data in the 
 * transaction must be colocated together on one data node. See the GemFire 
 * Developer Guide for details on using transactions with Partitioned Regions.
 * 
 * @author Mitch Thomas
 * 
 * @since 4.0
 * 
 * @see Cache
 * 
 */
public interface CacheTransactionManager {
    /** Creates a new transaction and associates it with the current thread.
     *
     * @throws IllegalStateException if the thread is already associated with a transaction
     *
     * @since 4.0
     */
    public void begin();

    /** Commit the transaction associated with the current thread. If
     *  the commit operation fails due to a conflict it will destroy
     *  the transaction state and throw a {@link
     *  CommitConflictException}.  If the commit operation succeeds,
     *  it returns after the transaction state has been merged with
     *  committed state.  When this method completes, the thread is no
     *  longer associated with a transaction.
     *
     * @throws IllegalStateException if the thread is not associated with a transaction
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
    public void commit() throws CommitConflictException;

    /** Roll back the transaction associated with the current thread. When
     *  this method completes, the thread is no longer associated with a
     *  transaction and the transaction context is destroyed.
     *
     * @since 4.0
     * 
     * @throws IllegalStateException if the thread is not associated with a transaction
     */
    public void rollback();

  /**
   * Suspends the transaction on the current thread. All subsequent operations
   * performed by this thread will be non-transactional. The suspended
   * transaction can be resumed by calling {@link #resume(TransactionId)}
   * 
   * @return the transaction identifier of the suspended transaction or null if
   *         the thread was not associated with a transaction
   * @since 6.6.2
   */
  public TransactionId suspend();

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
   * @since 6.6.2
   * @see #tryResume(TransactionId)
   */
  public void resume(TransactionId transactionId);

  /**
   * This method can be used to determine if a transaction with the given
   * transaction identifier is currently suspended locally. This method does not
   * check other members for transaction status.
   * 
   * @param transactionId
   * @return true if the transaction is in suspended state, false otherwise
   * @since 6.6.2
   * @see #exists(TransactionId)
   */
  public boolean isSuspended(TransactionId transactionId);

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
   * @since 6.6.2
   */
  public boolean tryResume(TransactionId transactionId);

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
   * This method returns immediately if {@link #exists(TransactionId)} returns false.
   * 
   * @param transactionId
   *          the transaction to resume
   * @param time
   *          the maximum time to wait
   * @param unit
   *          the time unit of the <code>time</code> argument
   * @return true if the transaction was resumed, false otherwise
   * @since 6.6.2
   * @see #tryResume(TransactionId)
   */
  public boolean tryResume(TransactionId transactionId, long time, TimeUnit unit);

  /**
   * Reports the existence of a transaction for the given transactionId. This
   * method can be used to determine if a transaction with the given transaction
   * identifier is currently in progress locally.
   * 
   * @param transactionId
   *          the given transaction identifier
   * @return true if the transaction is in progress, false otherwise.
   * @since 6.6.2
   * @see #isSuspended(TransactionId)
   */
  public boolean exists(TransactionId transactionId);
  
    /** Reports the existence of a Transaction for this thread
     *
     * @return true if a transaction exists, false otherwise
     *
     * @since 4.0
     */
    public boolean exists();

    /** Returns the transaction identifier for the current thread
     *
     * @return the transaction identifier or null if no transaction exists
     *
     * @since 4.0
     */
    public TransactionId getTransactionId();

    /**
     * Gets the transaction listener for this Cache.
     *
     * @return The TransactionListener instance or null if no listener.
     * @throws IllegalStateException if more than one listener exists on this cache
     * @deprecated as of GemFire 5.0, use {@link #getListeners} instead
     */
    @Deprecated
    public TransactionListener getListener();

    /** Returns an array of all the transaction listeners on this cache.
     * Modifications to the returned array will not effect what listeners are on this cache.
     * @return the cache's <code>TransactionListener</code>s; an empty array if no listeners
     * @since 5.0
     */
    public TransactionListener[] getListeners();

    /**
     * Sets the transaction listener for this Cache.
     *
     * @param newListener the TransactionListener to register with the Cache.
     *   Use a <code>null</code> to deregister the current listener without
     *   registering a new one.
     * @return the previous TransactionListener
     * @throws IllegalStateException if more than one listener exists on this cache
     * @deprecated as of GemFire 5.0, use {@link #addListener} or {@link #initListeners} instead.
     */
    @Deprecated
    public TransactionListener setListener(TransactionListener newListener);
    /**
     * Adds a transaction listener to the end of the list of transaction listeners on this cache.
     * @param aListener the user defined transaction listener to add to the cache.
     * @throws IllegalArgumentException if <code>aListener</code> is null
     * @since 5.0
     */
    public void addListener(TransactionListener aListener);
    /**
     * Removes a transaction listener from the list of transaction listeners on this cache.
     * Does nothing if the specified listener has not been added.
     * If the specified listener has been added then {@link CacheCallback#close} will
     * be called on it; otherwise does nothing.
     * @param aListener the transaction listener to remove from the cache.
     * @throws IllegalArgumentException if <code>aListener</code> is null
     * @since 5.0
     */
    public void removeListener(TransactionListener aListener);
    /**
     * Removes all transaction listeners, calling {@link CacheCallback#close} on each of them, and then adds each listener in the specified array.
     * @param newListeners a possibly null or empty array of listeners to add to this cache.
     * @throws IllegalArgumentException if the <code>newListeners</code> array has a null element
     * @since 5.0
     */
    public void initListeners(TransactionListener[] newListeners);

    /**
     * Set the TransactionWriter for the cache
     * @param writer
     * @see TransactionWriter
     * @since 6.5
     */
    public void setWriter(TransactionWriter writer);

    /**
     * Returns the current {@link TransactionWriter}
     * @see CacheTransactionManager#setWriter(TransactionWriter)
     * @return the current {@link TransactionWriter}
     * @since 6.5
     */
    public TransactionWriter getWriter();
    
    /**
     * Sets whether transactions should be executed in distributed or
     * non-distributed mode.  Once set this mode should not be changed during
     * the course of transactions.
     * 
     * @throws IllegalStateException if a transaction is already in progress
     * and this method sets the distributed mode to a different value.
     * @since 9.0
     */
    public void setDistributed(boolean distributed);
    
    /**
     * Returns the execution mode of transactions
     * @return true if distributed,
     * false otherwise.
     * @since 9.0
     */
    public boolean isDistributed();
}
