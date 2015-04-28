/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.exceptions;

import java.util.logging.Logger;

import com.gemstone.gemfire.management.internal.cli.parser.SyntaxConstants;
import com.gemstone.gemfire.management.internal.cli.util.CLIConsoleBufferUtil;
/**
 * @author njadhav
 * 
 *         Prints the warning according the CliException
 * 
 */
public class ExceptionHandler {

  private static Logger LOGGER = Logger.getLogger(ExceptionHandler.class
      .getCanonicalName());
  

  //FIXME define handling when no match is present
  public static void handleException(CliException ce) {    
    if (ce instanceof CliCommandException) {
      if (ce instanceof CliCommandNotAvailableException) {
        handleCommandNotAvailableException((CliCommandNotAvailableException) ce);
      } else if (ce instanceof CliCommandInvalidException) {
        handleCommandInvalidException((CliCommandInvalidException) ce);
      } else if (ce instanceof CliCommandOptionException) {
        handleOptionException((CliCommandOptionException) ce);
      } 
    }
  }

  private static void handleMultiModeOptionException(
      CliCommandMultiModeOptionException ce) {
    switch(ce.getCode()){
    case CliCommandMultiModeOptionException.MULTIPLE_LEAD_OPTIONS :
      LOGGER.warning(CLIConsoleBufferUtil.processMessegeForExtraCharactersFromConsoleBuffer(
          "Input command contains multiple lead-options from modes : " + ce.getLeadOptionString()));
      break;
    case CliCommandMultiModeOptionException.OPTIONS_FROM_MULTIPLE_MODES :
      LOGGER.warning(CLIConsoleBufferUtil.processMessegeForExtraCharactersFromConsoleBuffer(
          "Input command contains options from multilpe modes : " + ce.getLeadOptionString()));
      break;
    }
    
  }

  private static void handleCommandInvalidException(
      CliCommandInvalidException ccie) {   
    LOGGER.warning(CLIConsoleBufferUtil.processMessegeForExtraCharactersFromConsoleBuffer(ccie.getCommandTarget().getGfshMethodTarget().getKey()
        + " is not a valid Command"));   
  }

  private static void handleCommandNotAvailableException(
      CliCommandNotAvailableException ccnae) {    
    LOGGER.warning(CLIConsoleBufferUtil.processMessegeForExtraCharactersFromConsoleBuffer(ccnae.getCommandTarget().getGfshMethodTarget().getKey()
        + " is not available at the moment"));    
  }

  private static void handleOptionException(CliCommandOptionException ccoe) {
    if (ccoe instanceof CliCommandOptionNotApplicableException) {
      handleOptionInvalidExcpetion((CliCommandOptionNotApplicableException) ccoe);
    } else if (ccoe instanceof CliCommandOptionValueException) {
      handleOptionValueException((CliCommandOptionValueException) ccoe);
    } else if (ccoe instanceof CliCommandMultiModeOptionException) {      
      handleMultiModeOptionException((CliCommandMultiModeOptionException) ccoe);
    } 
  }

  private static void handleOptionInvalidExcpetion(
      CliCommandOptionNotApplicableException cconae) {  
    String messege = "Parameter " + cconae.getOption().getLongOption()
    + " is not applicable for "
    + cconae.getCommandTarget().getGfshMethodTarget().getKey();    
    LOGGER.warning(CLIConsoleBufferUtil.processMessegeForExtraCharactersFromConsoleBuffer(messege));  
  }

  private static void handleOptionValueException(
      CliCommandOptionValueException ccove) {
    if (ccove instanceof CliCommandOptionHasMultipleValuesException) {
      String messege = "Parameter " + ccove.getOption().getLongOption() + " can only be specified once";
      LOGGER.warning(CLIConsoleBufferUtil.processMessegeForExtraCharactersFromConsoleBuffer(messege));
    }
  }

  public static Logger getExceptionHanlderLogger(){
    return LOGGER;   
  }


}
