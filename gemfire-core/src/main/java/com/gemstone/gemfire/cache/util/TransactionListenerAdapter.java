/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.util;

import com.gemstone.gemfire.cache.TransactionListener;
import com.gemstone.gemfire.cache.TransactionEvent;

/**
 * Utility class that implements all methods in <code>TransactionListener</code>
 * with empty implementations. Applications can subclass this class and only
 * override the methods for the events of interest.
 * 
 * @author Darrel Schneider
 * 
 * @since 5.0
 */
public abstract class TransactionListenerAdapter implements TransactionListener {
  public void afterCommit(TransactionEvent event) {
  }

  public void afterFailedCommit(TransactionEvent event) {
  }

  public void afterRollback(TransactionEvent event) {
  }

  public void close() {
  }
}
