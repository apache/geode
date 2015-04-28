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

import com.gemstone.gemfire.internal.ra.spi.JCAManagedConnection;
import com.gemstone.gemfire.ra.GFConnection;

/**
 * 
 * @author asif
 *
 */
public class GFConnectionImpl implements GFConnection
{
  private JCAManagedConnection mc;

  private Reference ref;

  public GFConnectionImpl(JCAManagedConnection mc) {
    this.mc = mc;
  }

  public void resetManagedConnection(JCAManagedConnection mc)
  {
    this.mc = mc;
  }

  public void close() throws ResourceException
  {
    // Check if the connection is associated with a JTA. If yes, then
    // we should throw an exception on close being invoked.
    if (this.mc != null) {
      this.mc.onClose(this);
    }
  }

  public void invalidate()
  {
    this.mc = null;
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
