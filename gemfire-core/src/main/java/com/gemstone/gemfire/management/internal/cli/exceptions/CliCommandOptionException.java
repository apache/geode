/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.exceptions;

import com.gemstone.gemfire.management.internal.cli.parser.CommandTarget;
import com.gemstone.gemfire.management.internal.cli.parser.Option;
import com.gemstone.gemfire.management.internal.cli.parser.OptionSet;

public class CliCommandOptionException extends CliCommandException {

  private Option option;

  /**
   * @return option for which the exception occured
   */
  public Option getOption() {
    return option;
  }

  /**
   * @param option
   *          the option to set
   */
  public void setOption(Option option) {
    this.option = option;
  }

  public CliCommandOptionException(CommandTarget commandTarget, Option option) {
    super(commandTarget);
    this.setOption(option);
  }

  public CliCommandOptionException(CommandTarget commandTarget, Option option,
      OptionSet optionSet) {
    super(commandTarget, optionSet);
    this.setOption(option);
  }

}
