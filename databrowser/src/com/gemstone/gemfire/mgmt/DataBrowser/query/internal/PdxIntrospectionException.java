/*=========================================================================
 * (c)Copyright 2002-2011, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.query.internal;

import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionException;

/**
 * Exception used while introspecting PdxInstance GemFire type.
 * 
 * @author abhishek
 */
public class PdxIntrospectionException extends IntrospectionException {

  private static final long serialVersionUID = 85788300245998987L;

  public PdxIntrospectionException() {
    super();
  }

  public PdxIntrospectionException(String message, Throwable throwable) {
    super(message, throwable);
  }

  public PdxIntrospectionException(String message) {
    super(message);
  }

  public PdxIntrospectionException(Throwable throwable) {
    super(throwable);
  }
}