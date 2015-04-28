/*=========================================================================
 * Copyright (c) 2010-2011 VMware, Inc. All rights reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. VMware products are covered by
 * one or more patents listed at http://www.vmware.com/go/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.pdx;

import com.gemstone.gemfire.GemFireException;

/**
 * Thrown when a an attempt is made to reuse a PDX Type. This
 * can occur if the PDX registry files are deleted from the sending
 * side of a WAN Gateway. 
 */
public class PdxRegistryMismatchException extends GemFireException {

  private static final long serialVersionUID = -2329989020829052537L;

  /**
   * Constructs a new exception with the given message
   * @param message the message of the new exception
   */
  public PdxRegistryMismatchException(String message) {
    super(message);
  }
}
