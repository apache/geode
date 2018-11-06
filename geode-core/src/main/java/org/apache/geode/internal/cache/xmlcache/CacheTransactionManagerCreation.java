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
package org.apache.geode.internal.cache.xmlcache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.TransactionListener;
import org.apache.geode.cache.TransactionWriter;

/**
 * Represents a {@link CacheTransactionManager} that is created declaratively.
 *
 * @since GemFire 4.0
 */
public class CacheTransactionManagerCreation implements CacheTransactionManager {

  /** The TransactionListener instance set using the cache's CacheTransactionManager */
  private final ArrayList txListeners = new ArrayList();

  private TransactionWriter writer = null;

  /**
   * Creates a new <code>CacheTransactionManagerCreation</code>
   */
  public CacheTransactionManagerCreation() {}

  public TransactionListener setListener(TransactionListener newListener) {
    TransactionListener result = getListener();
    this.txListeners.clear();
    if (newListener != null) {
      this.txListeners.add(newListener);
    }
    return result;
  }

  public void initListeners(TransactionListener[] newListeners) {
    this.txListeners.clear();
    if (newListeners != null && newListeners.length > 0) {
      this.txListeners.addAll(Arrays.asList(newListeners));
    }
  }

  public void addListener(TransactionListener newListener) {
    if (!this.txListeners.contains(newListener)) {
      this.txListeners.add(newListener);
    }
  }

  public void removeListener(TransactionListener newListener) {
    this.txListeners.remove(newListener);
  }

  public TransactionListener[] getListeners() {
    TransactionListener[] result = new TransactionListener[this.txListeners.size()];
    this.txListeners.toArray(result);
    return result;
  }

  public TransactionListener getListener() {
    if (this.txListeners.isEmpty()) {
      return null;
    } else if (this.txListeners.size() == 1) {
      return (TransactionListener) this.txListeners.get(0);
    } else {
      throw new IllegalStateException(
          "more than one transaction listener exists");
    }
  }

  public TransactionId getTransactionId() {
    throw new UnsupportedOperationException(
        "Getting a TransactionId not supported");
  }

  public void begin() {
    throw new UnsupportedOperationException(
        "Transactions not supported");
  }

  public void commit() throws CommitConflictException {
    throw new UnsupportedOperationException(
        "Transactions not supported");
  }

  public void rollback() {
    throw new UnsupportedOperationException(
        "Transactions not supported");
  }

  public boolean exists() {
    throw new UnsupportedOperationException(
        "Transactions not supported");
  }

  public void setWriter(TransactionWriter writer) {
    this.writer = writer;
  }

  public TransactionWriter getWriter() {
    return writer;
  }

  public TransactionId suspend() {
    throw new UnsupportedOperationException(
        "Transactions not supported");
  }

  public void resume(TransactionId transactionId) {
    throw new UnsupportedOperationException(
        "Transactions not supported");
  }

  public boolean isSuspended(TransactionId transactionId) {
    throw new UnsupportedOperationException(
        "Transactions not supported");
  }

  public boolean tryResume(TransactionId transactionId) {
    throw new UnsupportedOperationException(
        "Transactions not supported");
  }

  public boolean tryResume(TransactionId transactionId, long time, TimeUnit unit) {
    throw new UnsupportedOperationException(
        "Transactions not supported");
  }

  public boolean exists(TransactionId transactionId) {
    throw new UnsupportedOperationException(
        "Transactions not supported");
  }

  public void setDistributed(boolean distributed) {
    throw new UnsupportedOperationException(
        "Transactions not supported");
  }

  public boolean isDistributed() {
    throw new UnsupportedOperationException(
        "Transactions not supported");
  }
}
