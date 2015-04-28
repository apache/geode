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

public class CliCommandOptionNotApplicableException extends
    CliCommandOptionException {

  public CliCommandOptionNotApplicableException(CommandTarget commandTarget,
      Option option) {
    super(commandTarget, option);
  }

  public CliCommandOptionNotApplicableException(CommandTarget commandTarget,
      Option option, OptionSet optionSet) {
    super(commandTarget, option, optionSet);
  }

}
