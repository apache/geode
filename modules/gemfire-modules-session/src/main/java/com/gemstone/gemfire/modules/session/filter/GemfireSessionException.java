/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.gemstone.gemfire.modules.session.filter;

/**
 * Exception class for Gemfire Session Cache specific exceptions.
 */
public class GemfireSessionException extends Exception {

  public GemfireSessionException() {
    super();
  }

  public GemfireSessionException(String message) {
    super(message);
  }

  public GemfireSessionException(String message, Throwable cause) {
    super(message, cause);
  }

  public GemfireSessionException(Throwable cause) {
    super(cause);
  }

}
