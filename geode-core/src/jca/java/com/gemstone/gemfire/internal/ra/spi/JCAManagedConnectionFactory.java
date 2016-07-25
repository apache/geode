/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
