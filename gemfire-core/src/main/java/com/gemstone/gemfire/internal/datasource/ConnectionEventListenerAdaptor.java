/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.datasource;

/**
 * @author mitulb
 * 
 * To change the template for this generated type comment go to Window -
 * Preferences - Java - Code Generation - Code and Comments
 */
public class ConnectionEventListenerAdaptor implements 
    javax.resource.spi.ConnectionEventListener,
    javax.sql.ConnectionEventListener {

  /**
   * @see javax.resource.spi.ConnectionEventListener#connectionClosed(javax.resource.spi.ConnectionEvent)
   */
  public void connectionClosed(javax.resource.spi.ConnectionEvent arg0) {
  }

  /**
   * @see javax.resource.spi.ConnectionEventListener#localTransactionStarted(javax.resource.spi.ConnectionEvent)
   */
  public void localTransactionStarted(javax.resource.spi.ConnectionEvent arg0) {
  }

  /**
   * @see javax.resource.spi.ConnectionEventListener#localTransactionCommitted(javax.resource.spi.ConnectionEvent)
   */
  public void localTransactionCommitted(javax.resource.spi.ConnectionEvent arg0) {
  }

  /**
   * @see javax.resource.spi.ConnectionEventListener#localTransactionRolledback(javax.resource.spi.ConnectionEvent)
   */
  public void localTransactionRolledback(javax.resource.spi.ConnectionEvent arg0) {
  }

  /**
   * @see javax.resource.spi.ConnectionEventListener#connectionErrorOccurred(javax.resource.spi.ConnectionEvent)
   */
  public void connectionErrorOccurred(javax.resource.spi.ConnectionEvent arg0) {
  }

  /**
   * Implementation of call back function from ConnectionEventListener
   * interface. This callback will be invoked on connection close event.
   * 
   * @param event Connection event object
   */
  public void connectionClosed(javax.sql.ConnectionEvent event) {
  }

  /**
   * Implementation of call back function from ConnectionEventListener
   * interface. This callback will be invoked on connection error event.
   * 
   * @param event Connection event object
   */
  public void connectionErrorOccurred(javax.sql.ConnectionEvent event) {
  }
}
