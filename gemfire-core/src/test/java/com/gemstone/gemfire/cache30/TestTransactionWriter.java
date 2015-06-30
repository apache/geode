/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache30;

import java.util.Properties;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.TransactionEvent;
import com.gemstone.gemfire.cache.TransactionWriter;
import com.gemstone.gemfire.cache.TransactionWriterException;

public class TestTransactionWriter implements TransactionWriter,Declarable {

  public void beforeCommit(TransactionEvent event)
      throws TransactionWriterException {
    // TODO Auto-generated method stub

  }

  public void close() {
    // TODO Auto-generated method stub

  }

  public void init(Properties props) {
    // TODO Auto-generated method stub
    
  }

}
