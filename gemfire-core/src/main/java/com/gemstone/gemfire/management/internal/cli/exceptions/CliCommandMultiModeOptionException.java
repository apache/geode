package com.gemstone.gemfire.management.internal.cli.exceptions;

import com.gemstone.gemfire.management.internal.cli.parser.CommandTarget;
import com.gemstone.gemfire.management.internal.cli.parser.Option;
import com.gemstone.gemfire.management.internal.cli.parser.OptionSet;

public class CliCommandMultiModeOptionException extends CliCommandOptionException {
  
  public static final int MULTIPLE_LEAD_OPTIONS=1;
  public static final int OPTIONS_FROM_MULTIPLE_MODES=2;
  
  private String leadOptionString;
  private int code;

  public CliCommandMultiModeOptionException(CommandTarget commandTarget,
      Option option, String string, int code) {
    super(commandTarget, option);
    this.leadOptionString = string;
    this.code = code;
  }

  public String getLeadOptionString() {
    return leadOptionString;
  }

  public int getCode() {
    return code;
  }
  
  
}

