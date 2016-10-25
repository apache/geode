/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal.cli.exceptions;

import java.util.logging.Logger;

import org.apache.geode.management.internal.cli.util.CLIConsoleBufferUtil;

/**
 * Prints the warning according the CliException
 */
public class ExceptionHandler {

  private static Logger LOGGER = Logger.getLogger(ExceptionHandler.class.getCanonicalName());
  
  //FIXME define handling when no match is present
  public static void handleException(CliException ce) {    
    if (ce instanceof CliCommandNotAvailableException) {
      handleCommandNotAvailableException((CliCommandNotAvailableException) ce);
    } else if (ce instanceof CliCommandInvalidException) {
      handleCommandInvalidException((CliCommandInvalidException) ce);
    } else if (ce instanceof CliCommandOptionException) {
      handleOptionException((CliCommandOptionException) ce);
    }
  }

  private static void handleMultiModeOptionException(CliCommandMultiModeOptionException ce) {
    switch(ce.getCode()){
      case CliCommandMultiModeOptionException.MULTIPLE_LEAD_OPTIONS :
        LOGGER.warning(CLIConsoleBufferUtil.processMessegeForExtraCharactersFromConsoleBuffer("Input command contains multiple lead-options from modes : " + ce.getLeadOptionString()));
        break;
      case CliCommandMultiModeOptionException.OPTIONS_FROM_MULTIPLE_MODES :
        LOGGER.warning(CLIConsoleBufferUtil.processMessegeForExtraCharactersFromConsoleBuffer("Input command contains options from multilpe modes : " + ce.getLeadOptionString()));
        break;
    }
  }

  private static void handleCommandInvalidException(CliCommandInvalidException ccie) {
    LOGGER.warning(CLIConsoleBufferUtil.processMessegeForExtraCharactersFromConsoleBuffer(ccie.getCommandTarget().getGfshMethodTarget().getKey() + " is not a valid Command"));
  }

  private static void handleCommandNotAvailableException(CliCommandNotAvailableException ccnae) {
    LOGGER.warning(CLIConsoleBufferUtil.processMessegeForExtraCharactersFromConsoleBuffer(ccnae.getCommandTarget().getGfshMethodTarget().getKey() + " is not available at the moment"));
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

  private static void handleOptionInvalidExcpetion(CliCommandOptionNotApplicableException cconae) {
    String messege = "Parameter " + cconae.getOption().getLongOption() + " is not applicable for " + cconae.getCommandTarget().getGfshMethodTarget().getKey();
    LOGGER.warning(CLIConsoleBufferUtil.processMessegeForExtraCharactersFromConsoleBuffer(messege));  
  }

  private static void handleOptionValueException(CliCommandOptionValueException ccove) {
    if (ccove instanceof CliCommandOptionHasMultipleValuesException) {
      // unfortunately by changing from geode-joptsimple to jopt-simple we will lose ALL such debugging info from exceptions
      //String parameter = ccove != null && ccove.getOption() != null ? ccove.getOption().getLongOption() : "<null>";
      String parameter = ccove.getOption().getLongOption();
      String message = "Parameter " + parameter + " can only be specified once";
      LOGGER.warning(CLIConsoleBufferUtil.processMessegeForExtraCharactersFromConsoleBuffer(message));
    }
  }
}
