/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.datasource;

/**
 * @author rreja
 */
import javax.resource.ResourceException;
import javax.resource.spi.ConnectionEventListener;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionFactory;
import javax.security.auth.Subject;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * This class implements a connection pool for Managed connection. Extends the
 * AbstractPoolCache to inherit the pool bahavior.
 * 
 * @author rreja
 */
public class ManagedPoolCacheImpl extends AbstractPoolCache  {

  private static final Logger logger = LogService.getLogger();
  
  private static final long serialVersionUID = 1064642271736399718L;
  private ManagedConnectionFactory connFactory;
  private Subject sub;
  private ConnectionRequestInfo connReqInfo;

  /**
   * Constructor initializes the ConnectionPoolCacheImpl properties.
   */
  public ManagedPoolCacheImpl(ManagedConnectionFactory connFac,
      Subject subject, ConnectionRequestInfo connReq,
      javax.resource.spi.ConnectionEventListener eventListner,
      ConfiguredDataSourceProperties configs) throws PoolException {
    super(eventListner, configs);
    connFactory = connFac;
    sub = subject;
    connReqInfo = connReq;
    initializePool();
  }

  /**
   * Creates a new connection for the managed connection pool.
   * 
   * @return the managed connection from the EIS as ManagedConnection object.
   * @throws PoolException
   */
  @Override
  public Object getNewPoolConnection() throws PoolException {
    ManagedConnection manConn = null;
    try {
      manConn = connFactory.createManagedConnection(sub, connReqInfo);
    }
    catch (ResourceException rex) {
      rex.printStackTrace();
      throw new PoolException(LocalizedStrings.ManagedPoolCacheImpl_MANAGEDPOOLCACHEIMPLGETNEWCONNECTION_EXCEPTION_IN_CREATING_NEW_MANAGED_POOLEDCONNECTION.toLocalizedString(), rex);
    }
    manConn
        .addConnectionEventListener((javax.resource.spi.ConnectionEventListener) connEventListner);
    return manConn;
  }

  /**
   * Destroys the underline physical connection to EIS.
   * 
   * @param connectionObject connection Object.
   */
  @Override
  void destroyPooledConnection(Object connectionObject) {
    try {
      ((ManagedConnection) connectionObject)
          .removeConnectionEventListener((ConnectionEventListener) connEventListner);
      ((ManagedConnection) connectionObject).destroy();
      connectionObject = null;
    }
    catch (ResourceException rex) {
      if (logger.isTraceEnabled()) {
        logger.trace("ManagedPoolcacheImpl::destroyPooledConnection:Exception in closing the connection.Ignoring it. The exeption is {}",
            rex.getMessage(), rex);
      }
    }
  }
}
