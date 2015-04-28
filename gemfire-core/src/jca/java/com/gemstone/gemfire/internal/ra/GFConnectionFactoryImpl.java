/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.ra;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.resource.ResourceException;
import javax.resource.spi.ConnectionManager;
import javax.resource.spi.ManagedConnectionFactory;

import com.gemstone.gemfire.ra.GFConnectionFactory;
/**
 * 
 * @author asif
 *
 */
public class GFConnectionFactoryImpl implements GFConnectionFactory
{
  final private ConnectionManager cm;

  final private ManagedConnectionFactory mcf;

  private Reference ref;

  public GFConnectionFactoryImpl(ManagedConnectionFactory mcf) {
    this.cm = null;
    this.mcf = mcf;
  }

  public GFConnectionFactoryImpl(ConnectionManager cm,
      ManagedConnectionFactory mcf) {
    this.cm = cm;
    this.mcf = mcf;
  }

  public GFConnectionImpl getConnection() throws ResourceException
  {
    return (GFConnectionImpl)cm.allocateConnection(mcf, null);
  }

  public void setReference(Reference ref)
  {
    this.ref = ref;

  }

  public Reference getReference() throws NamingException
  {
    return this.ref;
  }

}
