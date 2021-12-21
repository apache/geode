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

  @Override
  public TransactionListener setListener(TransactionListener newListener) {
    TransactionListener result = getListener();
    txListeners.clear();
    if (newListener != null) {
      txListeners.add(newListener);
    }
    return result;
  }

  @Override
  public void initListeners(TransactionListener[] newListeners) {
    txListeners.clear();
    if (newListeners != null && newListeners.length > 0) {
      txListeners.addAll(Arrays.asList(newListeners));
    }
  }

  @Override
  public void addListener(TransactionListener newListener) {
    if (!txListeners.contains(newListener)) {
      txListeners.add(newListener);
    }
  }

  @Override
  public void removeListener(TransactionListener newListener) {
    txListeners.remove(newListener);
  }

  @Override
  public TransactionListener[] getListeners() {
    TransactionListener[] result = new TransactionListener[txListeners.size()];
    txListeners.toArray(result);
    return result;
  }

  @Override
  public TransactionListener getListener() {
    if (txListeners.isEmpty()) {
      return null;
    } else if (txListeners.size() == 1) {
      return (TransactionListener) txListeners.get(0);
    } else {
      throw new IllegalStateException(
          "more than one transaction listener exists");
    }
  }

  @Override
  public TransactionId getTransactionId() {
    throw new UnsupportedOperationException(
        "Getting a TransactionId not supported");
  }

  @Override
  public void begin() {
    throw new UnsupportedOperationException(
        "Transactions not supported");
  }

  @Override
  public void commit() throws CommitConflictException {
    throw new UnsupportedOperationException(
        "Transactions not supported");
  }

  @Override
  public void rollback() {
    throw new UnsupportedOperationException(
        "Transactions not supported");
  }

  @Override
  public boolean exists() {
    throw new UnsupportedOperationException(
        "Transactions not supported");
  }

  @Override
  public void setWriter(TransactionWriter writer) {
    this.writer = writer;
  }

  @Override
  public TransactionWriter getWriter() {
    return writer;
  }

  @Override
  public TransactionId suspend() {
    throw new UnsupportedOperationException(
        "Transactions not supported");
  }

  @Override
  public void resume(TransactionId transactionId) {
    throw new UnsupportedOperationException(
        "Transactions not supported");
  }

  @Override
  public boolean isSuspended(TransactionId transactionId) {
    throw new UnsupportedOperationException(
        "Transactions not supported");
  }

  @Override
  public boolean tryResume(TransactionId transactionId) {
    throw new UnsupportedOperationException(
        "Transactions not supported");
  }

  @Override
  public boolean tryResume(TransactionId transactionId, long time, TimeUnit unit) {
    throw new UnsupportedOperationException(
        "Transactions not supported");
  }

  @Override
  public boolean exists(TransactionId transactionId) {
    throw new UnsupportedOperationException(
        "Transactions not supported");
  }

  @Override
  public void setDistributed(boolean distributed) {
    throw new UnsupportedOperationException(
        "Transactions not supported");
  }

  @Override
  public boolean isDistributed() {
    throw new UnsupportedOperationException(
        "Transactions not supported");
  }
}
