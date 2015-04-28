/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.exceptions;

import joptsimple.MissingRequiredOptionException;
import joptsimple.MultipleArgumentsForOptionException;
import joptsimple.OptionException;
import joptsimple.OptionMissingRequiredArgumentException;
import joptsimple.UnrecognizedOptionException;

/**
 * @author njadhav
 * 
 *         Converts joptsimple exceptions into corresponding exceptions for cli
 * 
 */
public class ExceptionGenerator {

  public static CliException generate(OptionException oe) {
    if (oe instanceof MissingRequiredOptionException) {
      return new CliCommandOptionMissingException(null, null, null);
    } else if (oe instanceof OptionMissingRequiredArgumentException) {
      return new CliCommandOptionValueMissingException(null, null, null, null);
    } else if (oe instanceof UnrecognizedOptionException) {
      return new CliCommandOptionNotApplicableException(null, null, null);
    } else if (oe instanceof MultipleArgumentsForOptionException) {
      return new CliCommandOptionHasMultipleValuesException(null, null, null);
    } else {
      return null;
    }
  }
}
