/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.session.installer.args;

/**
 * Interface specifying the requirements for objects wiching to be able to
 * examine arguments (potentially tweaking parameters) at the time of parsing,
 * thereby allowing for usage display to occur automatically.
 */
public interface ArgumentHandler {

  /**
   * Process the argument values specified.
   *
   * @param arg    argument definition
   * @param form   form which was used on the command line
   * @param params parameters supplied to the argument
   * @throws UsageException when usage was suboptimal
   */
  void handleArgument(Argument arg, String form, String[] params)
      throws UsageException;

}
