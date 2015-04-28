/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal.direct;

import com.gemstone.gemfire.GemFireCheckedException;

/**
 * Exception thrown when the TCPConduit is unable to acquire a stub
 * for the given recipient.
 * 
 * @author jpenney
 *
 */
public class MissingStubException extends GemFireCheckedException
{

  private static final long serialVersionUID = -6455664684151074915L;

  public MissingStubException(String msg) {
    super(msg);
  }
  
}
