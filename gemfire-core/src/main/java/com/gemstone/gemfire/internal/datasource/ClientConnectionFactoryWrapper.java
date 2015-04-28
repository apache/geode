/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.datasource;

/**
 * This class wraps the client connection factory and the corresponding
 * connection manager Object.
 * 
 * @author rreja
 */
public class ClientConnectionFactoryWrapper  {

  private Object clientConnFac;
  private Object manager;

  /**
   * Constructor.
   */
  public ClientConnectionFactoryWrapper(Object connFac, Object man) {
    this.clientConnFac = connFac;
    this.manager = man;
  }

  public void clearUp() {
    if (manager instanceof JCAConnectionManagerImpl) {
      ((JCAConnectionManagerImpl) this.manager).clearUp();
    }
    else if (manager instanceof FacetsJCAConnectionManagerImpl) {
      ((FacetsJCAConnectionManagerImpl) this.manager).clearUp();
    }
  }

  public Object getClientConnFactory() {
    return clientConnFac;
  }

  public Object getConnectionManager() {
    return this.manager;
  }
}
