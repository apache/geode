/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.xmlcache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.CommitConflictException;
import com.gemstone.gemfire.cache.TransactionId;
import com.gemstone.gemfire.cache.TransactionListener;
import com.gemstone.gemfire.cache.TransactionWriter;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * Represents a {@link CacheTransactionManager} that is created declaratively.  
 *
 * @author Mitch Thomas
 *
 * @since 4.0
 */
public class CacheTransactionManagerCreation implements CacheTransactionManager {

  ///////////////////////  Instance Fields  ///////////////////////

  /** The TransactionListener instance set using the cache's CacheTransactionManager */
  private final ArrayList txListeners = new ArrayList();
  private TransactionWriter writer = null;

  ///////////////////////  Constructors  ///////////////////////
  /**
   * Creates a new <code>CacheTransactionManagerCreation</code>
   */
  public CacheTransactionManagerCreation() {
  }

  //////////////////////  Instance Methods  //////////////////////
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
  public final TransactionListener getListener() {
    if (this.txListeners.isEmpty()) {
      return null;
    } else if (this.txListeners.size() == 1) {
      return (TransactionListener)this.txListeners.get(0);
    } else {
      throw new IllegalStateException(LocalizedStrings.CacheTransactionManagerCreation_MORE_THAN_ONE_TRANSACTION_LISTENER_EXISTS.toLocalizedString());
    }
  }

  public TransactionId getTransactionId() {
    throw new UnsupportedOperationException(LocalizedStrings.CacheTransactionManagerCreation_GETTING_A_TRANSACTIONID_NOT_SUPPORTED.toLocalizedString());
  }
  public void begin() {
    throw new UnsupportedOperationException(LocalizedStrings.CacheTransactionManagerCreation_TRANSACTIONS_NOT_SUPPORTED.toLocalizedString());
  }
  public void commit() throws CommitConflictException {
    throw new UnsupportedOperationException(LocalizedStrings.CacheTransactionManagerCreation_TRANSACTIONS_NOT_SUPPORTED.toLocalizedString());
  }
  public void rollback() {
    throw new UnsupportedOperationException(LocalizedStrings.CacheTransactionManagerCreation_TRANSACTIONS_NOT_SUPPORTED.toLocalizedString());
  }
  public boolean exists() {
    throw new UnsupportedOperationException(LocalizedStrings.CacheTransactionManagerCreation_TRANSACTIONS_NOT_SUPPORTED.toLocalizedString());
  }

  public final void setWriter(TransactionWriter writer) {
    this.writer = writer;
  }

  public TransactionWriter getWriter() {
    return writer;
  }

  public TransactionId suspend() {
    throw new UnsupportedOperationException(LocalizedStrings.CacheTransactionManagerCreation_TRANSACTIONS_NOT_SUPPORTED.toLocalizedString());
  }

  public void resume(TransactionId transactionId) {
    throw new UnsupportedOperationException(LocalizedStrings.CacheTransactionManagerCreation_TRANSACTIONS_NOT_SUPPORTED.toLocalizedString());
  }

  public boolean isSuspended(TransactionId transactionId) {
    throw new UnsupportedOperationException(LocalizedStrings.CacheTransactionManagerCreation_TRANSACTIONS_NOT_SUPPORTED.toLocalizedString());
  }
  public boolean tryResume(TransactionId transactionId) {
    throw new UnsupportedOperationException(LocalizedStrings.CacheTransactionManagerCreation_TRANSACTIONS_NOT_SUPPORTED.toLocalizedString());
  }

  public boolean tryResume(TransactionId transactionId, long time, TimeUnit unit) {
    throw new UnsupportedOperationException(LocalizedStrings.CacheTransactionManagerCreation_TRANSACTIONS_NOT_SUPPORTED.toLocalizedString());
  }

  public boolean exists(TransactionId transactionId) {
    throw new UnsupportedOperationException(LocalizedStrings.CacheTransactionManagerCreation_TRANSACTIONS_NOT_SUPPORTED.toLocalizedString());
  }
}
