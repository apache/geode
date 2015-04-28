/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.result;

import com.gemstone.gemfire.management.cli.Result;

/**
 * Exception wrapper around a command result.
 * 
 * @author David Hoots
 * @since 7.0
 */
public class CommandResultException extends Exception {
  private static final long serialVersionUID = 1L;

  private final Result result;

  public CommandResultException(final Result result) {
    this.result = result;
  }

  public Result getResult() {
    return this.result;
  }
}
