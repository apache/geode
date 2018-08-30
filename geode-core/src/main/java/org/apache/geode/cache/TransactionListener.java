/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.cache;

/**
 * <p>
 * A listener that can be implemented to handle transaction related events. The methods on
 * <code>TransactionListener</code> are invoked synchronously after the operation, commit or
 * rollback, completes. The transaction that causes the listener to be called will no longer exist
 * at the time the listener code executes. The thread that performed the transaction operation will
 * not see that operation complete until the listener method completes its execution.
 *
 * <p>
 * Multiple transactions, on the same cache, can cause concurrent invocation of
 * <code>TransactionListener</code> methods. Any exceptions thrown by the listener are caught and
 * logged.
 *
 * <p>
 * Rollback and failed commit operations are local.
 *
 * <p>
 * WARNING: To avoid risk of deadlock, do not invoke CacheFactory.getAnyInstance() from within any
 * callback methods. Instead use TransactionEvent.getCache().
 *
 * @see CacheTransactionManager#setListener
 * @see CacheTransactionManager#getListener
 * @since GemFire 4.0
 */
public interface TransactionListener<K, V> extends CacheCallback {

  /**
   * Called after a successful commit of a transaction.
   *
   * @param event the TransactionEvent
   * @see CacheTransactionManager#commit
   */
  void afterCommit(TransactionEvent<K, V> event);

  /**
   * Called after an unsuccessful commit operation.
   *
   * @param event the TransactionEvent
   * @see CacheTransactionManager#commit
   */
  void afterFailedCommit(TransactionEvent<K, V> event);

  /**
   * Called after an explicit rollback of a transaction.
   *
   * @param event the TransactionEvent
   * @see CacheTransactionManager#rollback
   * @see CacheTransactionManager#commit
   */
  void afterRollback(TransactionEvent<K, V> event);
}
