/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.ra.spi;

import javax.resource.ResourceException;
import javax.resource.spi.ManagedConnectionMetaData;
/**
 * 
 * @author asif
 *
 */
public class JCAManagedConnectionMetaData implements ManagedConnectionMetaData
{
  private final String prodName;

  private final String version;

  private final String user;

  public JCAManagedConnectionMetaData(String prodName, String version,
      String user) {
    this.prodName = prodName;
    this.version = version;
    this.user = user;
  }

  
  public String getEISProductName() throws ResourceException
  {
    return this.prodName;
  }

  
  public String getEISProductVersion() throws ResourceException
  {

    return this.version;
  }

  
  public int getMaxConnections() throws ResourceException
  {

    return 0;
  }

  
  public String getUserName() throws ResourceException
  {
    return this.user;
  }

}
