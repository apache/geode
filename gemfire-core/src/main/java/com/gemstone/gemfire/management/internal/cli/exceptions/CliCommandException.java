/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.exceptions;

import java.util.List;

import com.gemstone.gemfire.management.internal.cli.parser.CommandTarget;
import com.gemstone.gemfire.management.internal.cli.parser.OptionSet;

public class CliCommandException extends CliException {
  private CommandTarget commandTarget;
  private OptionSet optionSet;

  public OptionSet getOptionSet() {
    return optionSet;
  }

  public CommandTarget getCommandTarget() {
    return commandTarget;
  }

  public CliCommandException(CommandTarget commandTarget) {
    this.setCommandTarget(commandTarget);
  }

  public CliCommandException(CommandTarget commandTarget, OptionSet optionSet) {
    this(commandTarget);
    this.setOptionSet(optionSet);
  }

  public CliCommandException(CommandTarget commandTarget2,
      List<String> nonOptionArguments) {
  }

  /**
   * @param commandTarget
   *          the commandTarget to set
   */
  public void setCommandTarget(CommandTarget commandTarget) {
    this.commandTarget = commandTarget;
  }

  /**
   * @param optionSet
   *          the optionSet to set
   */
  public void setOptionSet(OptionSet optionSet) {
    this.optionSet = optionSet;
  }

}
