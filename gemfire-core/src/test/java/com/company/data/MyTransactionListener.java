/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.company.data;

import com.gemstone.gemfire.cache.*;

/**
 * A <code>TransactionListener</code> that is <code>Declarable</code>
 *
 * @author Mitch Thomas
 * @since 4.0
 */
public class MyTransactionListener implements TransactionListener, Declarable {

  public void afterCommit(TransactionEvent event) {}
    
  public void afterFailedCommit(TransactionEvent event) {}

  public void afterRollback(TransactionEvent event) {}

  public void init(java.util.Properties props) {}

  public void close() {}

}
