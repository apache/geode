/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.ra.spi;

import java.io.PrintWriter;
import java.util.Set;

import javax.resource.ResourceException;
import javax.resource.spi.ConnectionManager;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionFactory;
import javax.security.auth.Subject;

import com.gemstone.gemfire.internal.ra.GFConnectionFactoryImpl;
/**
 * 
 * @author asif
 *
 */
public class JCAManagedConnectionFactory implements ManagedConnectionFactory
{
  private String productName;

  private String version;

  private String user;

  private PrintWriter logger;

  public Object createConnectionFactory() throws ResourceException
  {

    return new GFConnectionFactoryImpl(this);
  }

  public Object createConnectionFactory(ConnectionManager cm)
      throws ResourceException
  {

    return new GFConnectionFactoryImpl(cm, this);
  }

  public ManagedConnection createManagedConnection(Subject arg0,
      ConnectionRequestInfo arg1) throws ResourceException
  {
    return new JCAManagedConnection(this);

  }

  public PrintWriter getLogWriter() throws ResourceException
  {

    return this.logger;
  }

  public ManagedConnection matchManagedConnections(Set arg0, Subject arg1,
      ConnectionRequestInfo arg2) throws ResourceException
  {
    // TODO Auto-generated method stub
    return null;
  }

  public void setLogWriter(PrintWriter logger) throws ResourceException
  {
    this.logger = logger;

  }

  public boolean equals(Object obj)
  {
    if (obj instanceof JCAManagedConnectionFactory) {
      return true;
    }
    else {
      return false;
    }
  }

  public int hashCode()
  {
    return 0;
  }

  public void setUserName(String user)
  {

    if (this.logger != null) {
      logger.println("JCAManagedConnectionFactory::setUserName:. user name is="
          + user);
    }
    this.user = user;
  }

  public String getUserName()
  {
    return this.user;
  }

  public void setProductName(String name)
  {

    if (this.logger != null) {
      logger
          .println("JCAManagedConnectionFactory::setProductName:. Product name is="
              + name);
    }
    this.productName = name;
  }

  public String getProductName()
  {
    return this.productName;
  }

  public void setVersion(String version)
  {

    if (this.logger != null) {
      logger.println("JCAManagedConnectionFactory::setVersion:. version is="
          + version);
    }
    this.version = version;
  }

  public String getVersion()
  {
    return this.version;
  }

}
