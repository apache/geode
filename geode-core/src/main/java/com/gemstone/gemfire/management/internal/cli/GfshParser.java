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
package com.gemstone.gemfire.management.internal.cli;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.springframework.shell.core.AbstractShell;
import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;
import org.springframework.shell.core.Parser;
import org.springframework.shell.event.ParseResult;

import com.gemstone.gemfire.management.cli.CommandProcessingException;
import com.gemstone.gemfire.management.internal.cli.exceptions.CliCommandMultiModeOptionException;
import com.gemstone.gemfire.management.internal.cli.exceptions.CliCommandOptionException;
import com.gemstone.gemfire.management.internal.cli.exceptions.CliException;
import com.gemstone.gemfire.management.internal.cli.exceptions.ExceptionHandler;
import com.gemstone.gemfire.management.internal.cli.help.format.NewHelp;
import com.gemstone.gemfire.management.internal.cli.help.utils.HelpUtils;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.modes.CommandModes;
import com.gemstone.gemfire.management.internal.cli.modes.CommandModes.CommandMode;
import com.gemstone.gemfire.management.internal.cli.parser.Argument;
import com.gemstone.gemfire.management.internal.cli.parser.AvailabilityTarget;
import com.gemstone.gemfire.management.internal.cli.parser.CommandTarget;
import com.gemstone.gemfire.management.internal.cli.parser.GfshMethodTarget;
import com.gemstone.gemfire.management.internal.cli.parser.MethodParameter;
import com.gemstone.gemfire.management.internal.cli.parser.Option;
import com.gemstone.gemfire.management.internal.cli.parser.OptionSet;
import com.gemstone.gemfire.management.internal.cli.parser.Parameter;
import com.gemstone.gemfire.management.internal.cli.parser.ParserUtils;
import com.gemstone.gemfire.management.internal.cli.parser.SyntaxConstants;
import com.gemstone.gemfire.management.internal.cli.parser.preprocessor.PreprocessorUtils;
import com.gemstone.gemfire.management.internal.cli.parser.preprocessor.TrimmedInput;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;
import com.gemstone.gemfire.management.internal.cli.util.CLIConsoleBufferUtil;

/**
 * Implementation of the {@link Parser} interface for GemFire SHell (gfsh)
 * requirements.
 *
 * @since GemFire 7.0
 */
public class GfshParser implements Parser {
  public static final String LINE_SEPARATOR = System.getProperty("line.separator");

  // Constants used while finding command targets for help
  private final static Short EXACT_TARGET     = (short)0;
  private final static Short MATCHING_TARGETS = (short)1;

  // Make use of LogWrapper
  private static final LogWrapper logWrapper = LogWrapper.getInstance();

  // private CliStringResourceBundle cliStringBundle;
  private CommandManager commandManager;

  /**
   * Used for warning messages
   */
  // TODO Investigating using GemFire logging.
  private final Logger consoleLogger;

  public GfshParser(CommandManager commandManager) {
    // cliStringBundle = new
    // CliStringResourceBundle("com/gemstone/gemfire/management/internal/cli/i18n/CliStringResourceBundle");
    this.commandManager = commandManager;
    if (CliUtil.isGfshVM()) {
      consoleLogger = Logger.getLogger(this.getClass().getCanonicalName());
    } else {
      consoleLogger = logWrapper.getLogger();
    }
  }

  // ///////////////// Parser interface Methods Start //////////////////////////
  // ////////////////////// Implemented Methods ////////////////////////////////
  /**
   * Populates a list of completion candidates. See
   * {@link Parser#complete(String, int, List)} for details.
   *
   * @param buffer
   * @param cursor
   * @param completionCandidates
   * @return new cursor position
   */
  public int complete(String buffer, int cursor,
      List<String> completionCandidates) {
    final List<Completion> candidates = new ArrayList<Completion>();
    final int result = completeAdvanced(buffer, cursor, candidates);
    for (final Completion completion : candidates) {
      completionCandidates.add(completion.getValue());
    }
    return result;
  }

  /**
   * Populates a list of completion candidates.
   *
   * @param buffer
   * @param cursor
   * @param completionCandidates
   * @return new cursor position
   */
  public int completeAdvanced(String buffer, int cursor,
      List<Completion> completionCandidates) {
    // Currently, support for auto-completion
    // in between is not supported, only if the
    // cursor is at the end

    if (cursor <= buffer.length() - 1
        && !PreprocessorUtils.containsOnlyWhiteSpaces(buffer.substring(cursor))
        || (ParserUtils.contains(buffer, SyntaxConstants.COMMAND_DELIMITER))) {
      return cursor;
    }

    int desiredCursorPosition = 0;

    try {
      TrimmedInput simpleTrim = PreprocessorUtils.simpleTrim(buffer);
      desiredCursorPosition += simpleTrim.getNoOfSpacesRemoved();
      List<CommandTarget> targets = locateTargets(simpleTrim.getString());
      if (targets.size() > 1) {
        String padding = desiredCursorPosition != 0 ? ParserUtils.getPadding(desiredCursorPosition) : "";
        // This means that what the user has entered matches
        // the beginning of many commands
        for (CommandTarget commandTarget : targets) {
          completionCandidates.add(new Completion(padding + commandTarget
              .getGfshMethodTarget().getKey()));
        }
      } else {
        if (targets.size() == 1) {
          CommandTarget commandTarget = targets.get(0);
          // Only one command matches but we still have to check
          // whether the user has properly entered it or not
          if (simpleTrim.getString().length() >= commandTarget
              .getGfshMethodTarget().getKey().length()) {
            /* int position = */
            return completeParameters(commandTarget, desiredCursorPosition
                + commandTarget.getGfshMethodTarget().getKey().length(),
                commandTarget.getGfshMethodTarget().getRemainingBuffer(),
                cursor, completionCandidates);
            /*
             * updateCompletionCandidates(completionCandidates, buffer,
             * position); return 0;
             */
          } else {
            String padding = desiredCursorPosition != 0 ? ParserUtils.getPadding(desiredCursorPosition) : "";
            // User has still not entered the command name properly,
            // we need to populate the completionCandidates list
            completionCandidates.add(new Completion(padding + commandTarget
                .getGfshMethodTarget().getKey()));
          }
        }
      }

    } catch (IllegalArgumentException e) {
      logWrapper.warning(CliUtil.stackTraceAsString(e));
    } catch (IllegalAccessException e) {
      logWrapper.warning(CliUtil.stackTraceAsString(e));
    } catch (InvocationTargetException e) {
      logWrapper.warning(CliUtil.stackTraceAsString(e));
    } catch (RuntimeException e) {
      logWrapper.warning(CliUtil.stackTraceAsString(e));
    }
    // Returning 0 for exceptions too. This will break the completors' loop in
    // jline.ConsoleReader.complete() & will return false
    return 0;
  }

  @SuppressWarnings("unused")
  private void updateCompletionCandidates(
      List<Completion> completionCandidates, String buffer, int position) {
    List<Completion> temp = new ArrayList<Completion>();
    while (completionCandidates.size() > 0) {
      temp.add(completionCandidates.remove(0));
    }
    for (Completion completion : temp) {
      completionCandidates.add(new Completion(buffer.substring(0, position)
          + completion.getValue(), completion.getFormattedValue(), completion
          .getHeading(), completion.getOrder()));
    }
  }

  private int completeParameters(CommandTarget commandTarget, int cursorStart,
      String remainingBuffer, int cursor, List<Completion> completionCandidates) {
    int desiredCursorPosition = cursorStart;
    // Factor for remainingBuffer
    boolean sizeReduced = false;
    // We need to work modify the flow according to the CliException
    // generated. For that we will need a reference to the Exception
    // CliException reference
    CliCommandOptionException coe = null;
    OptionSet userOptionSet = null;
    try {
      // We need to remove the space which separates command from the
      // parameters
      if (remainingBuffer.length() > 0) {
        remainingBuffer = remainingBuffer.substring(1);
        sizeReduced = true;
      }

      userOptionSet = commandTarget.getOptionParser().parse(remainingBuffer);
    } catch (CliException ce) {
      if (ce instanceof CliCommandOptionException) {
        coe = (CliCommandOptionException) ce;
        coe.setCommandTarget(commandTarget);
        userOptionSet = coe.getOptionSet();
      }
    }

    // Contains mandatory options which have not been specified
    List<Option> mandatoryOptions = new ArrayList<Option>();
    // Contains non-mandatory options which have not been specified
    List<Option> unspecifiedOptions = new ArrayList<Option>();
    // First we need a list to create a list of all the options specified
    Map<String, Option> optionsPresentMap = new LinkedHashMap<String, Option>();
    if (userOptionSet != null) {

      // Start with the arguments
      String argumentSeparator = " ";
      for (Argument argument : commandTarget.getOptionParser().getArguments()) {
        if (completionCandidates.size() == 0) {
          boolean warning = false;
          if (userOptionSet.hasArgument(argument)) {
            boolean incrementCursor = true;
            // Here we need to get all the possible values for this
            // argument
            if (getAllPossibleValuesForParameter(completionCandidates,
                argument, userOptionSet.getValue(argument),
                commandTarget.getGfshMethodTarget())) {
              // Check whether the list of completionCandidates is
              // not empty
              if (completionCandidates.size() > 0) {
                // First check whether the argument value
                // matches with any
                // of the completionCandidates
                if (perfectMatch(completionCandidates, userOptionSet.getValue(argument))) {
                  // Remove all the completionCandidates
                  completionCandidates.clear();
                } else {
                  modifyCompletionCandidates(completionCandidates,
                      argumentSeparator,
                      userOptionSet.getValue(argument));
                  // For this case also we should not
                  // increment the
                  // cursorPosition
                  if (completionCandidates.size() > 0) {
                    incrementCursor = false;
                  }
                }
              }
            }else{
              // The completion candidates should be cleared if the Converter has
              // populated it with some values
              completionCandidates.clear();
            }
            if (incrementCursor) {
              desiredCursorPosition += userOptionSet.getValue(argument).length()
                  + argumentSeparator.length();
            }
          } else {
            if (argument.isRequired()) {
              // Here the converter will come in handy
              // to get suggestion for arguments
              if (getAllPossibleValuesForParameter(completionCandidates,
                  argument, null, commandTarget.getGfshMethodTarget())) {
                if (completionCandidates.size() == 0) {
                  // Enable warning if nothing is returned
                  warning = true;
                }
              } else {
                // The completion candidates should be cleared if the Converter has
                // populated it with some values
                completionCandidates.clear();
                warning = true;
              }
            } else {
              boolean checkForPossibleValues = true;
              // This means that the argument is not mandatory
              // Now here we need to check whether user wants to
              // enter an option.
              if (endsWithOptionSpecifiers(userOptionSet.getUserInput()) || hasOptionSpecified(userOptionSet.getUserInput())) {
                // This means options have started, and we
                // cannot have arguments after options
                // So, we just skip checking for possible
                // values
                checkForPossibleValues = false;
              }
              // Just try getting the PossibleValues without
              // aiming
              if (checkForPossibleValues) {
                getAllPossibleValuesForParameter(completionCandidates,
                    argument, null, commandTarget.getGfshMethodTarget());
              }
            }
            if (completionCandidates.size() > 0) {
              modifyCompletionCandidates(completionCandidates,
                  argumentSeparator, (String[]) null);
            }
          }
          if (warning) {
            String argMessage = argument.getArgumentName() +
                ((argument.getHelp() != null && !argument.getHelp()
                    .equals("")) ? ": " + argument.getHelp() : "");
            logWarning(CliStrings.format(CliStrings.GFSHPARSER__MSG__REQUIRED_ARGUMENT_0, argMessage));
            return desiredCursorPosition + userOptionSet.getNoOfSpacesRemoved();
          }
        }
        argumentSeparator = SyntaxConstants.ARGUMENT_SEPARATOR;
      }
      if (completionCandidates.size() > 0) {
        return desiredCursorPosition + userOptionSet.getNoOfSpacesRemoved();
      }

      // Now process options

      boolean warningValueRequired = false;
      Option warningOption = null;

      for (Option option : commandTarget.getOptionParser().getOptions()) {
        if (userOptionSet.hasOption(option)) {
          // We are supporting option synonyms,
          // so we need to check that here
          for (String string : userOptionSet.getSplit()) {
            if (string.startsWith(SyntaxConstants.LONG_OPTION_SPECIFIER)) {
              // Remove option prefix
              string = StringUtils.removeStart(string,
                  SyntaxConstants.LONG_OPTION_SPECIFIER);
              // Remove value specifier
              string = StringUtils.removeEnd(string,
                  SyntaxConstants.OPTION_VALUE_SPECIFIER);
              if (!string.equals("")) {
                if (option.getLongOption().equals(string)) {
                  // This means that user has entered the whole option and
                  // Increment desiredCursorPostion by the length of the
                  // option and the option specifier, including space
                  desiredCursorPosition += /* space */1
                      + SyntaxConstants.LONG_OPTION_SPECIFIER.length()
                      + option.getLongOption().length();
                  break;

                } else {
                  // This is only possible if the user has
                  // entered one of the synonyms of the options
                  // which wasn't displayed initially
                  for (String optionSynonym : option.getSynonyms()) {
                    if (optionSynonym.equals(string)) {
                      // This means that what the user has
                      // entered is actually a
                      // synonym for the option
                      desiredCursorPosition += /* space */1
                          + SyntaxConstants.LONG_OPTION_SPECIFIER.length()
                          + optionSynonym.length();
                      break;
                    }
                  }
                }
              }
            }
          }

          optionsPresentMap.put(option.getLongOption(), option);

          // For option value

          if (userOptionSet.hasValue(option)) {
            String value = userOptionSet.getValue(option);
            boolean valueActuallySpecified = false;

            String valueSeparator = SyntaxConstants.VALUE_SEPARATOR;
            if (option.getValueSeparator() != null) {
              valueSeparator = option.getValueSeparator();
            }

            // JOpt doesn't maintain trailing comma (separator), hence reading it from buffer.
            boolean bufferEndsWithValueSeparator = remainingBuffer.endsWith(valueSeparator);
            // Check whether the value assigned to the option is
            // actually part of the split array or has been
            // assigned using the specifiedDefaultValue attribute

            // userOptionElement can be option name or value of that option.
            // E.g. "--opt=val" has elements "opt" & "val"
            for (String userOptionElement : userOptionSet.getSplit()) {
              if (userOptionElement.equals(value) || (userOptionElement).equals(value + valueSeparator)) {
                valueActuallySpecified = true;
              }
            }
            if (!valueActuallySpecified) {
              continue;
            }
            boolean incrementCursor = true;
            boolean considerLastValue = false;
            int lengthToBeAdded = 0;
            int lastIndexOf = 0;

            // This should only be invoked if we don't have any
            // completionCandidates beforeHand
            if (completionCandidates.size() == 0) {
              // Here also we might need to invoke converter to
              // get values apt for the option
              if (!endsWithOptionSpecifiers(userOptionSet.getUserInput())
                  && getAllPossibleValuesForParameter(completionCandidates,
                      option, value, commandTarget.getGfshMethodTarget())) {

                // If the value returned by getAllPossibleValues
                // is the same as that entered by the
                // user we need to remove it from the
                // completionCandidates and move forward

                String prefix = "";
                String[] split = ParserUtils.splitValues(value, valueSeparator);

                if (completionCandidates.size() > 0) {
                  if (PreprocessorUtils.isSyntaxValid(value)
                      && bufferEndsWithValueSeparator) {
                    // This means that the user wants to
                    // enter more values,
                    prefix = valueSeparator;
                  } else if (perfectMatch(completionCandidates, split)) {
                    // If the user does not want to enter
                    // more values, and it matches one
                    // of the values then we do not
                    // need to suggest anything for
                    // this option
                    completionCandidates.clear();
                    considerLastValue = true;
                  } else if (ParserUtils.contains(value, valueSeparator)) {
                    prefix = valueSeparator;
                  } else {
                    incrementCursor = false;
                    if (value.startsWith(" ")) {
                      prefix = "  ";
                    } else if(value.startsWith("\n")){
                      prefix = "\n";
                    }else{
                      prefix = SyntaxConstants.OPTION_VALUE_SPECIFIER;
                    }
                  }
                }
                modifyCompletionCandidates(completionCandidates, prefix, bufferEndsWithValueSeparator, split);
                if (completionCandidates.size() == 0) {
                  incrementCursor = true;
                  considerLastValue = true;
                }
              } else {
                // The completion candidates should be cleared if the Converter has
                // populated it with some values
                completionCandidates.clear();
                considerLastValue = true;
              }
            } else {
              // Make everything true
              considerLastValue = true;
            }
            // FIX for: 46265
            // if bufferEndsWithValueSeparator, append a valueSeparator to get the real lastIndexOd
            // e.g. Let's say remainingBuffer is: cmd --opt1=val1,val2,
            //                value would be:  cmd --opt1=val1,val2  ---> not there's no comma in the end.
            // This doesn't give us the real last index of valueSeparator, hence add extra valueSeparator.
            lastIndexOf = ParserUtils.lastIndexOf(bufferEndsWithValueSeparator ? value + valueSeparator : value, valueSeparator);
            lengthToBeAdded = value.substring(0,
                (lastIndexOf > 0 ? lastIndexOf : value.length())).length();
            // Increment desiredCursorPosition
            if (incrementCursor) {
              desiredCursorPosition += /* value specifier length */SyntaxConstants.OPTION_VALUE_SPECIFIER
                  .length()
                  + lengthToBeAdded
                  + ((considerLastValue) ? value.length() - lengthToBeAdded : 0);
              if (value.endsWith(" ") && considerLastValue) {
                desiredCursorPosition--;
              }
            }
            if (completionCandidates.size() == 0) {
              if (!PreprocessorUtils.isSyntaxValid(value)) {
                return desiredCursorPosition + userOptionSet.getNoOfSpacesRemoved();
              } else {
                // Check whether the value ends with
                // VALUE_SEPARATOR,
                // if yes then we need to return
                if (value.endsWith(valueSeparator)) {
                  return desiredCursorPosition + userOptionSet.getNoOfSpacesRemoved();
                }
              }
            }
          } else {
            // Here the converter is useful to invoke
            // auto-suggestion, get Values from Converter
            if (completionCandidates.size() == 0) {
              if (getAllPossibleValuesForParameter(completionCandidates,
                  option, null, commandTarget.getGfshMethodTarget())) {
                if (completionCandidates.size() == 0) {
                  warningValueRequired = true;
                } else {
                  modifyCompletionCandidates(completionCandidates,
                      SyntaxConstants.OPTION_VALUE_SPECIFIER,
                      new String[] { null });
                }
              }else{
                // The completion candidates should be cleared if the Converter
                // has populated it with some values
                completionCandidates.clear();
                warningValueRequired = true;
              }
            }
          }
        } else {

          // As we have reached here, the OptionParser was not able to
          // detect anything which proves that the option is present
          // So, we check with what the user provided and add only
          // that to the list of options to prompt for

          for (String userOptString : userOptionSet.getSplit()) {
            // Now to determine whether what the user specified was
            // an option, we need to check whether it starts
            // with an option specifier
            if (userOptString.startsWith(SyntaxConstants.LONG_OPTION_SPECIFIER)) {
              // Now remove the option specifier part
              userOptString = StringUtils.removeStart(userOptString,
                  SyntaxConstants.LONG_OPTION_SPECIFIER);
              if (option.getLongOption().startsWith(userOptString)
                  && !userOptString.equals("")
                  && !option.getLongOption().equals(userOptString)
                  && !optionsPresentMap.containsKey(userOptString)) {
                                
                completionCandidates.add(new Completion(" "
                    + SyntaxConstants.LONG_OPTION_SPECIFIER
                    + option.getLongOption(), option.getLongOption(), "", 0));
              }else{
                for (String optionSynonym : option.getSynonyms()) {
                  if (optionSynonym.startsWith(userOptString)
                      && !userOptString.equals("")
                      && !optionSynonym.equals(userOptString)) {
                    completionCandidates.add(new Completion(" "
                        + SyntaxConstants.LONG_OPTION_SPECIFIER
                        + optionSynonym, optionSynonym, "", 0));
                    break;
                  }
                }
              }
            }
          }

          if (completionCandidates.size() == 0) {
            if (option.isRequired()) {
              mandatoryOptions.add(option);
            } else {
              unspecifiedOptions.add(option);
            }
          }
        }
        if (warningValueRequired/* || warningMultipleValuesNotSupported */) {
          warningOption = option;
          warningValueRequired = false;
        }
      }

      // Display warning if something not specified
      if (warningOption != null) {
        String optionMsg = warningOption.getLongOption() +
            ((warningOption.getHelp() != null && !warningOption.getHelp()
            .equals("")) ? ": " + warningOption.getHelp() : "");
        logWarning(CliStrings.format(CliStrings.GFSHPARSER__MSG__VALUE_REQUIRED_FOR_OPTION_0, optionMsg));

        desiredCursorPosition += userOptionSet.getNoOfSpacesRemoved();
        completionCandidates.add(new Completion(
            SyntaxConstants.OPTION_VALUE_SPECIFIER, "", null, 0));
        return desiredCursorPosition;
      }

    }
    // Calculate the cursor position
    int newCursor = desiredCursorPosition
        + ((userOptionSet != null) ? userOptionSet.getNoOfSpacesRemoved() : 0);

    String subString = remainingBuffer;
    if (newCursor != cursorStart) {
      subString = remainingBuffer.substring(
          newCursor + (sizeReduced ? -1 : 0) - cursorStart).trim();
    }
    

    // Exception handling
    if (coe != null
        && newCursor < cursor
        && completionCandidates.size() == 0
        && !(PreprocessorUtils.containsOnlyWhiteSpaces(subString) || ((subString
            .endsWith(SyntaxConstants.LONG_OPTION_SPECIFIER) && subString
            .startsWith(SyntaxConstants.LONG_OPTION_SPECIFIER)) || (subString
            .startsWith(SyntaxConstants.SHORT_OPTION_SPECIFIER) && subString
            .endsWith(SyntaxConstants.SHORT_OPTION_SPECIFIER))))) {
      ExceptionHandler.handleException(coe);
      return cursor;
    }

    // If nothing has been specified for auto-completion then we need to suggest options
    if (completionCandidates.size() == 0) {
      if (mandatoryOptions.size() > 0) {
        
        for (Option option : mandatoryOptions) {
          completionCandidates.add(new Completion(" " + SyntaxConstants.LONG_OPTION_SPECIFIER + option.getLongOption(),
              option.getLongOption(), "", 0));
        }
      } else {
        // As all the mandatory options have been specified we can prompt the
        // user for optional options.
        unspecifiedOptions = getUnspecifiedOptionsWithMode(unspecifiedOptions, commandTarget, optionsPresentMap);
        for (Option option : unspecifiedOptions) {
          completionCandidates.add(new Completion(" " + SyntaxConstants.LONG_OPTION_SPECIFIER + option.getLongOption(),
              option.getLongOption(), "", 0));
        }
      }
    }
    return newCursor;
  }

  private List<Option> getUnspecifiedOptionsWithMode(List<Option> unspecifiedOptions, CommandTarget commandTarget,
      Map<String, Option> optionsPresentMap) {
                 
    Collection<CommandMode> cmodes = CommandModes.getInstance().getCommandModes(commandTarget.getCommandName());
    if (cmodes != null) {
      List<Option> filteredList = new ArrayList<Option>();
      
      //Populate with default options
      CommandMode defaultMode = CommandModes.getInstance().getCommandMode(commandTarget.getCommandName(),
          CommandModes.DEFAULT_MODE);      
      for (String opt : defaultMode.options) {
        for (Option option : unspecifiedOptions) {
          if (option.getLongOption().equals(opt))
            filteredList.add(option);
        }
      }
      
      //Now add options only for detected command mode
      boolean leadOptionFound = false;
      for (CommandMode cmd : cmodes) {
        if (optionsPresentMap.containsKey(cmd.leadOption)) {
          leadOptionFound = true;
          for (String opt : cmd.options) {
            if (!optionsPresentMap.containsKey(opt)) {
              for (Option option : unspecifiedOptions) {
                if (option.getLongOption().equals(opt))
                  filteredList.add(option);
              }
            }
          }
          break;          
        }
      }
      
      if(leadOptionFound)
        return filteredList;
      
      if(optionsPresentMap.isEmpty()) {
        //Here return only lead-option of the command-modes
        filteredList.clear();
        for (CommandMode cmd2 : cmodes) {
          for (Option option2 : unspecifiedOptions) {
            if (option2.getLongOption().equals(cmd2.leadOption))
              filteredList.add(option2);
          }
        }
        return filteredList;
      }
      return unspecifiedOptions;
    } else
        return unspecifiedOptions;           
  }

  private void checkOptionSetForValidCommandModes(OptionSet userOptionSet,
 CommandTarget commandTarget)
      throws CliCommandMultiModeOptionException {
    CommandModes modes = CommandModes.getInstance();
    Collection<CommandMode> cmodes = modes.getCommandModes(commandTarget.getCommandName());    
    
    if (cmodes != null) {
      CommandMode defaultMode = modes.getCommandMode(commandTarget.getCommandName(), CommandModes.DEFAULT_MODE);
      Map<String, Option> userOptions = new HashMap<String, Option>();
      Map<String, CommandMode> loToModeMap = new HashMap<String, CommandMode>();
      for (Option option : commandTarget.getOptionParser().getOptions()) {
        if (userOptionSet.hasOption(option)) {
          userOptions.put(option.getLongOption(), option);
        }
      }

      List<String> leadOptionList = new ArrayList<String>();
      for (CommandMode cmd : cmodes) {
        loToModeMap.put(cmd.leadOption, cmd);
        if (userOptions.containsKey(cmd.leadOption))
          leadOptionList.add(cmd.leadOption);

        if (leadOptionList.size() > 1) {

          StringBuilder sb = new StringBuilder();
          for (String leadOption : leadOptionList) {
            sb.append(loToModeMap.get(leadOption).name).append(",");
          }
          throw new CliCommandMultiModeOptionException(commandTarget, userOptions.get(cmd.leadOption), sb.toString(),
              CliCommandMultiModeOptionException.MULTIPLE_LEAD_OPTIONS);
        }
      }

      if (leadOptionList.size() == 1) {
        CommandMode modeDetected = loToModeMap.get(leadOptionList.get(0));
        for (Option opt : userOptions.values()) {
          //Check only for non-default options, default options are allowed with any other mode
          if (!isDefaultOption(opt.getLongOption(),defaultMode)) {
            boolean isOptionFromDetectedMode = false;
            if(modeDetected.options.length>0) {
              for (String commandOpt : modeDetected.options) {
                if (commandOpt.equals(opt.getLongOption())) {
                  isOptionFromDetectedMode = true;                
                }
              }
              if (!isOptionFromDetectedMode)
                throw new CliCommandMultiModeOptionException(commandTarget, opt, opt.getLongOption(),
                    CliCommandMultiModeOptionException.OPTIONS_FROM_MULTIPLE_MODES);
            }            
          }
        }
      }
    }
  }

  private boolean isDefaultOption(String longOption, CommandMode commandMode) {
    for(String str : commandMode.options){
      if(longOption.equals(str))
        return true;
    }
    return false;
  }

  private boolean endsWithOptionSpecifiers(String userInput) {
    userInput = userInput.trim();
    if (userInput.endsWith(" "+SyntaxConstants.LONG_OPTION_SPECIFIER)
        || userInput.endsWith(" "+SyntaxConstants.SHORT_OPTION_SPECIFIER)) {
      return true;
    } else {
      return false;
    }
  }

  /*
   * Verifies whether the userInput has any one of the following:
   * --some-opt
   * --s
   * --some-opt=some-val --something-else
   */
  private boolean hasOptionSpecified(String userInput) {
    userInput = userInput.trim();
    return Pattern.matches("^(.*)(-+)(\\w+)(.*)$", userInput);
  }

  private String getSystemProvidedValue(Parameter parameter) {
    if (parameter.isSystemProvided()) {
      //TODO fetch from system properties
      // Assume value is null for now.
      return null;
    } else {
      return null;
    }
  }

  private boolean perfectMatch(List<Completion> completionCandidates,
      String... argumentValue) {
    // Here only the last value should match one of the
    // completionCandidates
    if (argumentValue.length > 0) {
      for (Completion completion : completionCandidates) {
        if (completion.getValue().equals(
            argumentValue[argumentValue.length - 1])) {
          return true;
        }
      }
    }
    return false;
  }

  private void modifyCompletionCandidates(
      List<Completion> completionCandidates, String prefix,
      String... existingData) {
    modifyCompletionCandidates(completionCandidates, prefix, false, existingData);
  }

  private void modifyCompletionCandidates(
      List<Completion> completionCandidates, String prefix, boolean endsWithValueSeparator,
      String... existingData) {
    List<Completion> temp = new ArrayList<Completion>();
    while (completionCandidates.size() > 0) {
      temp.add(completionCandidates.remove(0));
    }
    for (Completion completion : temp) {
      boolean includeCompletion = true;
      String value = completion.getValue();
      if (existingData != null) {
        for (String string : existingData) {
          if (string != null) {
            // Check whether that value matches any of the
            // existingData
            // If it matches any one of existing data then we do not
            // need to include it in the list of completion
            // candidates
            if (value.equals(string)) {
              includeCompletion = false;
            }
          }
        }
        if (includeCompletion) {
          if (existingData[existingData.length - 1] != null
              && (!value.startsWith(existingData[existingData.length - 1])
                  && !endsWithValueSeparator)) {
            includeCompletion = false;
          }
        }
      }
      if (includeCompletion) {
        // Also we only need to check with the last string of
        // existingData
        // whether the completion value starts with it.
        completionCandidates.add(new Completion(prefix + completion.getValue(),
            completion.getValue(), "", 0));
      }
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private boolean getAllPossibleValuesForParameter(
      List<Completion> completionCandidates, Parameter parameter,
      String existingData, GfshMethodTarget gfshMethodTarget) {
    Converter<?> converter = parameter.getConverter();
    // Check if any new converter is available which
    // satisfies the requirements for this argument
    if (converter == null) {
      parameter.setConverter(commandManager.getConverter(
          parameter.getDataType(), parameter.getContext()));
      converter = parameter.getConverter();
    }
    // If still we do not have any matching converters, we return
    if (converter == null) {
      return false;
    } else {
      // Now pass the getAllPossibleValues function of Converter interface
      // all the required parameters

      // Check whether it is a MultipleValueConverter
      String valueSeparator = SyntaxConstants.VALUE_SEPARATOR;
      if (parameter instanceof Option && ((Option) parameter).getValueSeparator() != null) {
        valueSeparator = ((Option) parameter).getValueSeparator();
      }
      if (converter instanceof MultipleValueConverter) {
        ((MultipleValueConverter) converter).getAllPossibleValues(
            completionCandidates,
            parameter.getDataType(),
            ParserUtils.splitValues(existingData, valueSeparator),
            parameter.getContext(),
            new MethodTarget(gfshMethodTarget.getMethod(), gfshMethodTarget
                .getTarget(), gfshMethodTarget.getRemainingBuffer(),
                gfshMethodTarget.getKey()));
      } else {
        converter.getAllPossibleValues(
            completionCandidates,
            parameter.getDataType(),
            existingData,
            parameter.getContext(),
            new MethodTarget(gfshMethodTarget.getMethod(), gfshMethodTarget
                .getTarget(), gfshMethodTarget.getRemainingBuffer(),
                gfshMethodTarget.getKey()));
      }
    }
    if (completionCandidates.size() > 0) {
      return true;
    } else {
      return false;
    }
  }

  /**
   *
   */
  public ParseResult parse(String userInput) {
    GfshParseResult parseResult = null;
    // First remove the trailing white spaces
    userInput = StringUtils.stripEnd(userInput, null);
    if ((ParserUtils.contains(userInput, SyntaxConstants.COMMAND_DELIMITER) && StringUtils.endsWithIgnoreCase(
        userInput, SyntaxConstants.COMMAND_DELIMITER))) {
      userInput = StringUtils.removeEnd(userInput, SyntaxConstants.COMMAND_DELIMITER);
    }
    
    try {
      boolean error = false;
      CliCommandOptionException coe = null;
      List<CommandTarget> targets = locateTargets(ParserUtils.trimBeginning(userInput), false);
      if (targets.size() > 1) {
        if (userInput.length() > 0) {
          handleCondition(CliStrings.format(
              CliStrings.GFSHPARSER__MSG__AMBIGIOUS_COMMAND_0_FOR_ASSISTANCE_USE_1_OR_HINT_HELP, new Object[] {
                  userInput, AbstractShell.completionKeys }), CommandProcessingException.COMMAND_NAME_AMBIGUOUS,
              userInput);
        }
      } else {
        if (targets.size() == 1) {
          
          OptionSet parse = null;
          List<MethodParameter> parameters = new ArrayList<MethodParameter>();
          Map<String, String> paramValMap = new HashMap<String, String>();
          CommandTarget commandTarget = targets.get(0);
          GfshMethodTarget gfshMethodTarget = commandTarget.getGfshMethodTarget();
          preConfigureConverters(commandTarget);
          
          try {
            parse = commandTarget.getOptionParser().parse(
                gfshMethodTarget.getRemainingBuffer());
          } catch (CliException ce) {            
            if (ce instanceof CliCommandOptionException) {
              coe = (CliCommandOptionException) ce;
              coe.setCommandTarget(commandTarget);
              parse = coe.getOptionSet();
              error = true;
            }
          }
          
          try {
            checkOptionSetForValidCommandModes(parse, commandTarget);
          } catch (CliCommandMultiModeOptionException ce) {
            error = true;
            coe = ce;
          }
          
          error = processArguments(parse, commandTarget, paramValMap, parameters, error);
          error = processOptions(parse, commandTarget, paramValMap, parameters, error);
          
          if (!error) {
            Object[] methodParameters = new Object[parameters.size()];
            for (MethodParameter parameter : parameters) {
              methodParameters[parameter.getParameterNo()] = parameter.getParameter();
            }
            parseResult = new GfshParseResult(gfshMethodTarget.getMethod(), gfshMethodTarget.getTarget(),
                methodParameters, userInput, commandTarget.getCommandName() , paramValMap);
          } else {
            if (coe != null) {              
              logWrapper.fine("Handling exception: " + coe.getMessage());
              ExceptionHandler.handleException(coe);
              // ExceptionHandler.handleException() only logs it on console.
              // When on member, we need to handle this.
              if (!CliUtil.isGfshVM()) {
                handleCondition(CliStrings.format(CliStrings.GFSHPARSER__MSG__INVALID_COMMAND_STRING_0, userInput),
                    coe, CommandProcessingException.COMMAND_INVALID, userInput);
              }              
            }
          }
          
        } else {
          String message = CliStrings.format(CliStrings.GFSHPARSER__MSG__COMMAND_0_IS_NOT_VALID, userInput);
          CommandTarget commandTarget = locateExactMatchingTarget(userInput);
          if (commandTarget != null) {
            String commandName = commandTarget.getCommandName();
            AvailabilityTarget availabilityIndicator = commandTarget.getAvailabilityIndicator();
            message = CliStrings.format(CliStrings.GFSHPARSER__MSG__0_IS_NOT_AVAILABLE_REASON_1, new Object[] {
                commandName, availabilityIndicator.getAvailabilityDescription() });
          }
          handleCondition(message, CommandProcessingException.COMMAND_INVALID_OR_UNAVAILABLE, userInput);
        }
      }
    } catch (IllegalArgumentException e1) {
      logWrapper.warning(CliUtil.stackTraceAsString(e1));
    } catch (IllegalAccessException e1) {
      logWrapper.warning(CliUtil.stackTraceAsString(e1));
    } catch (InvocationTargetException e1) {
      logWrapper.warning(CliUtil.stackTraceAsString(e1));
    }
    return parseResult;
  }

  // Pre-configure the converters so that we can test against them when parsing the command line
  private void preConfigureConverters(CommandTarget commandTarget) {
    for (Option option : commandTarget.getOptionParser().getOptions()) {
      Converter<?> converter = option.getConverter();
      if (converter == null) {
        option.setConverter(commandManager.getConverter(option.getDataType(), option.getContext()));
        converter = option.getConverter();
      }
    }

    for (Argument argument : commandTarget.getOptionParser().getArguments()) {
      Converter<?> converter = argument.getConverter();
      if (converter == null) {
        argument.setConverter(commandManager.getConverter(argument.getDataType(), argument.getContext()));
        converter = argument.getConverter();
      }
    }
  }

  private boolean processOptions(OptionSet parse, CommandTarget commandTarget, Map<String, String> paramValMap,
      List<MethodParameter> parameters, boolean errorState) {
    boolean error = errorState;
    for (Option option : commandTarget.getOptionParser().getOptions()) {
      String value = null;
      if (parse.hasOption(option)) {
        if (parse.hasValue(option)) {
          value = parse.getValue(option);
        }
        if (value == null) {
          handleCondition(
              CliStrings.format(CliStrings.GFSHPARSER__MSG__VALUE_REQUIRED_FOR_OPTION_0, option.getLongOption()),
              CommandProcessingException.OPTION_VALUE_REQUIRED, option.getLongOption());
          logWrapper.fine("Value required for Parameter " + option.getLongOption());
          error = true;
        }
      } else {
        if (option.isRequired()) {
          handleCondition(
              CliStrings.format(CliStrings.GFSHPARSER__MSG__COMMAND_OPTION_0_IS_REQUIRED_USE_HELP,
                  option.getLongOption()), CommandProcessingException.REQUIRED_OPTION_MISSING,
              option.getLongOption());
          logWrapper.fine("Required Parameter " + option.getLongOption());
          error = true;
        } else {
          // Try to get the unspecifiedDefaultValue for the
          // option
          value = option.getUnspecifiedDefaultValue();
          if (value == null) {
            // Now try the system provide value
            value = getSystemProvidedValue(option);
          }
        }
      }

      String valueSeparator = SyntaxConstants.VALUE_SEPARATOR;
      if (option.getValueSeparator() != null) {
        valueSeparator = option.getValueSeparator();
      }

      Object object = getConversionObject(option.getConverter(), value, option.getDataType(),
          option.getContext(), valueSeparator);
      // Check if conversion fails
      if (value != null && object == null) {
        handleCondition(
            CliStrings.format(CliStrings.GFSHPARSER__MSG__VALUE_0_IS_NOT_APPLICABLE_FOR_1,
                new Object[] { value.trim(), option.getLongOption() }),
            CommandProcessingException.OPTION_VALUE_INVALID, option.getLongOption() + "=" + value);
        logWrapper.fine("Value \"" + value.trim() + "\" is not applicable for " + option.getLongOption());
        error = true;
      }
      parameters.add(new MethodParameter(object, option.getParameterNo()));
      paramValMap.put(option.getLongOption(), value);
    }
    return error;
  }

  private boolean processArguments(OptionSet parse, CommandTarget commandTarget, Map<String, String> paramValMap,
      List<MethodParameter> parameters, boolean errorState) {
    boolean error = errorState;
    for (Argument argument : commandTarget.getOptionParser().getArguments()) {
      String value = null;
      
      if (parse.hasArgument(argument)) {
        value = parse.getValue(argument);
      } else {
        if (argument.isRequired()) {
          handleCondition(
              CliStrings.format(CliStrings.GFSHPARSER__MSG__COMMAND_ARGUMENT_0_IS_REQUIRED_USE_HELP,
                  argument.getArgumentName()), CommandProcessingException.REQUIRED_ARGUMENT_MISSING,
              argument.getArgumentName());
          logWrapper.fine("Required Argument " + argument.getArgumentName());
          error = true;
        } else {
          // try to get unspecifiedDefaultValue for
          // the argument
          value = argument.getUnspecifiedDefaultValue();
          if (value == null) {
            // Now try the system provided value
            value = getSystemProvidedValue(argument);
          }
        }

      }

      Object conversionObject = getConversionObject(argument.getConverter(), value, argument.getDataType(),
          argument.getContext(), SyntaxConstants.VALUE_SEPARATOR);
      if (value != null && conversionObject == null) {
        handleCondition(
            CliStrings.format(CliStrings.GFSHPARSER__MSG__VALUE_0_IS_NOT_APPLICABLE_FOR_1,
                new Object[] { value.trim(), argument.getArgumentName() }),
            CommandProcessingException.ARGUMENT_INVALID, argument.getArgumentName() + "=" + value);
        logWrapper
            .fine("Value '" + value.trim() + "' not applicable for argument: " + argument.getArgumentName());
        error = true;
      } else {
        parameters.add(new MethodParameter(conversionObject, argument.getParameterNo()));
        paramValMap.put(argument.getArgumentName(), value);
      }
    }
    return error;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private Object getConversionObject(Converter<?> converter, String string,
      Class<?> dataType, String context, String valueSeparator) {

    try {
      if (converter != null && converter instanceof MultipleValueConverter) {
          return ((MultipleValueConverter) converter).convertFromText(
              ParserUtils.splitValues(
                  ((string != null) ? string.trim() : null),
                  valueSeparator), dataType, context);
      }

      // Remove outer single or double quotes if found
      if (string != null && ((string.endsWith("\"") && string.endsWith("\""))
          || (string.startsWith("\'") && string.endsWith("\'")))) {
        string = string.substring(1, string.length() - 1);
      }

      if (converter != null) {
      return converter.convertFromText((string != null) ? string.trim()
          : null, dataType, context);
      }

      //TODO consider multiple value case for primitives
      if (string != null) {
        if (String.class.isAssignableFrom(dataType)) {
          return string.trim();
        } else if (Byte.class.isAssignableFrom(dataType)
            || byte.class.isAssignableFrom(dataType)) {
          return Integer.parseInt(string);
        } else if (Short.class.isAssignableFrom(dataType)
            || short.class.isAssignableFrom(dataType)) {
          return Integer.parseInt(string);
        } else if (Boolean.class.isAssignableFrom(dataType)
            || boolean.class.isAssignableFrom(dataType)) {
          return Integer.parseInt(string);
        } else if (Integer.class.isAssignableFrom(dataType)
            || int.class.isAssignableFrom(dataType)) {
          return Integer.parseInt(string);
        } else if (Long.class.isAssignableFrom(dataType)
            || long.class.isAssignableFrom(dataType)) {
          return Long.parseLong(string);
        } else if (Float.class.isAssignableFrom(dataType)
            || float.class.isAssignableFrom(dataType)) {
          return Float.parseFloat(string);
        } else if (Double.class.isAssignableFrom(dataType)
            || double.class.isAssignableFrom(dataType)) {
          return Double.parseDouble(string);
        } else if (Character.class.isAssignableFrom(dataType)
            || char.class.isAssignableFrom(dataType)) {
          if (string.length() == 1) {
            string.charAt(0);
          } else {
            // FIXME Use a constant here
            return '0';
          }
        }
      }
    } catch (Exception e) {
      // TODO add logging
      // Do nothing, just return null
    }
    return null;
  }

  private List<CommandTarget> locateTargets(String userInput)
      throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
    return locateTargets(userInput, true);
  }


  private List<CommandTarget> locateTargets(String userInput, boolean matchIncomplete)
      throws IllegalArgumentException, IllegalAccessException,
      InvocationTargetException {
    List<CommandTarget> commandTargets = new ArrayList<CommandTarget>();
    Map<String, CommandTarget> commands = commandManager.getCommands();
    // Now we need to locate the CommandTargets from the entries in the map
    for (String commandName : commands.keySet()) {
      if (userInput.startsWith(commandName)) {
        // This means that the user has entered the command
        CommandTarget commandTarget = commands.get(commandName);
        if (isAvailable(commandTarget, commandName)) {
          String remainingBuffer = StringUtils.removeStart(userInput, commandName);
          if (remainingBuffer.length() == 0
              || remainingBuffer.startsWith(" ")
              || remainingBuffer.startsWith(GfshParser.LINE_SEPARATOR)) {
            // We need to duplicate with a new MethodTarget as this
            // parser will be used in a concurrent execution environment
            if (!commandTargets.contains(commandTarget)) {
              // This test is necessary as the command may have similar
              // synonyms or which are prefix for the command
              commandTargets.add(commandTarget.duplicate(commandName,
                  remainingBuffer));
            }
          }
        }
      } else if (matchIncomplete && commandName.startsWith(userInput)) {
        // This means that the user is yet to enter the command properly
        CommandTarget commandTarget = commands.get(commandName);
        if (isAvailable(commandTarget, commandName)) {
          // We need to duplicate with a new MethodTarget as this
          // parser will be used in a concurrent execution environment
          if (!commandTargets.contains(commandTarget)) {
            // This test is necessary as the command may have similar
            // synonyms or which are prefix for the command
            commandTargets.add(commandTarget.duplicate(commandName));
          }
        }
      }
    }
    return commandTargets;
  }

  //TODO - Abhishek - create an inner CommandTargetLocater instead of multiple
  //methods like these.
  private CommandTarget locateExactMatchingTarget(final String userInput)//exact matching
      throws IllegalArgumentException, IllegalAccessException,
      InvocationTargetException {
    CommandTarget commandTarget = null;

    Map<String, CommandTarget> commandTargetsMap = commandManager.getCommands();
    // Reverse sort the command names because we should start from longer names
    // E.g. Consider commands "A", "A B" & user input as "A B --opt1=val1"
    // In this case, "A B" is the most probable match & should be matched first
    // which can be achieved by reversing natural order of sorting.
    Set<String> commandNamesReverseSorted = new TreeSet<String>(Collections.reverseOrder());
    commandNamesReverseSorted.addAll(commandTargetsMap.keySet());

    // Now we need to locate the CommandTargets from the entries in the map
    for (final String commandName : commandNamesReverseSorted) {
      if (userInput.startsWith(commandName) && commandWordsMatch(userInput, commandName)) {
        // This means that the user has entered the command & name matches exactly
        commandTarget = commandTargetsMap.get(commandName);
        if (commandTarget != null) {
          String remainingBuffer = StringUtils.removeStart(userInput, commandName);
          commandTarget = commandTarget.duplicate(commandName, remainingBuffer);
          break;
        }
      }
    }
    return commandTarget;
  }

  private static boolean commandWordsMatch(final String userInput, final String commandName) {
    boolean  commandWordsMatch = true;

    String[] commandNameWords = commandName.split(" ");
    String[] userInputWords   = userInput.split(" ");

    // commandName is fixed & hence should have less or same number of words as
    // the user input. E.g. "create disk-store" should match with
    // "create disk-store --name=xyz" but not with "create disk store"
    if (commandNameWords.length <= userInputWords.length) {
      // if both have length zero, words can be considered to be matching.
      for (int i = 0; i < commandNameWords.length; i++) {
        if (!commandNameWords[i].equals(userInputWords[i])) {
          commandWordsMatch = false;
          break;
        }
      }
    } else {
      commandWordsMatch = false;
    }

    return commandWordsMatch;
  }


  private Map<String, CommandTarget> getRequiredCommandTargets(Set<String> requiredCommands) {
    Map<String, CommandTarget> existingCommands = commandManager.getCommands();
    Map<String, CommandTarget> requiredCommandsMap = existingCommands;

    if (requiredCommands != null && !requiredCommands.isEmpty()) {
      requiredCommandsMap = new TreeMap<String, CommandTarget>();
      for (String commandName : requiredCommands) {
        CommandTarget commandTarget = existingCommands.get(commandName);
        if (commandTarget != null) {
          requiredCommandsMap.put(commandName, commandTarget);
        }
      }
    }

    return requiredCommandsMap;
  }

  private Map<Short, List<CommandTarget>> findMatchingCommands(String userSpecifiedCommand, Set<String> requiredCommands)
      throws IllegalArgumentException, IllegalAccessException,
      InvocationTargetException {

    Map<String, CommandTarget> existingCommands = getRequiredCommandTargets(requiredCommands);
    CommandTarget exactCommandTarget = existingCommands.get(userSpecifiedCommand);

    // 1. First find exactly matching commands.
    List<CommandTarget> exactCommandTargets = Collections.emptyList();
    if (exactCommandTarget != null) {
      // This means that the user has entered the command
      // NOTE: we are not skipping synonym here.
      exactCommandTargets = Collections.singletonList(exactCommandTarget);
    }

    // 2. Now find command names that start with 'userSpecifiedCommand'
    List<CommandTarget> possibleCommandTargets = new ArrayList<CommandTarget>();
    // Now we need to locate the CommandTargets from the entries in the map
    for (Map.Entry<String, CommandTarget> entry : existingCommands.entrySet()) {
      CommandTarget commandTarget = entry.getValue();
      String commandName = commandTarget.getCommandName();
       // This check is done to remove commands that are synonyms as
      // CommandTarget.getCommandName() will return name & not a synonym
      if (entry.getKey().equals(commandName)) {
        if (commandName.startsWith(userSpecifiedCommand) && !commandTarget.equals(exactCommandTarget)) {
          // This means that the user is yet to enter the command properly
          possibleCommandTargets.add(commandTarget);
        }
      }
    }

    Map<Short, List<CommandTarget>> commandTargetsArr = new HashMap<Short, List<CommandTarget>>();
    commandTargetsArr.put(EXACT_TARGET,     exactCommandTargets);
    commandTargetsArr.put(MATCHING_TARGETS, possibleCommandTargets);
    return commandTargetsArr;
  }


  private boolean isAvailable(CommandTarget commandTarget, String commandName)
      throws IllegalArgumentException, IllegalAccessException,
      InvocationTargetException {
    AvailabilityTarget availabilityIndicator = commandTarget
        .getAvailabilityIndicator();
    if (availabilityIndicator == null) {
      availabilityIndicator = commandManager.getAvailabilityIndicator(commandName);
      commandTarget.setAvailabilityIndicator(availabilityIndicator);
    }
    return commandTarget.isAvailable();
  }

  public List<String> obtainHelpCommandNames(String userInput) {
    List<String> commandNames = new ArrayList<String>();

    try {
      if (userInput == null) {
        userInput = "";
      }

      List<CommandTarget> commandTargets = new ArrayList<CommandTarget>();
      Map<Short, List<CommandTarget>> matchingCommandsMap = findMatchingCommands(userInput, null);
      commandTargets.addAll(matchingCommandsMap.get(EXACT_TARGET));
      commandTargets.addAll(matchingCommandsMap.get(MATCHING_TARGETS));

      for (CommandTarget commandTarget : commandTargets) {
        commandNames.add(commandTarget.getCommandName());
      }
    } catch (IllegalArgumentException e) {
      logWrapper.warning(CliUtil.stackTraceAsString(e));
    } catch (IllegalAccessException e) {
      logWrapper.warning(CliUtil.stackTraceAsString(e));
    } catch (InvocationTargetException e) {
      logWrapper.warning(CliUtil.stackTraceAsString(e));
    }

    return commandNames;
  }

  public String obtainHelp(String userInput, Set<String> commandNames) {
    final boolean withinShell = commandNames == null || commandNames.isEmpty();
    final String appName = withinShell ? "" : HelpUtils.EXE_PREFIX_FOR_EXTERNAL_HELP;

    StringBuilder helpText = new StringBuilder();
    try {
      if(userInput == null) {
        userInput="";
      }

      Map<Short, List<CommandTarget>> matchingCommandsMap = findMatchingCommands(userInput, commandNames);
      List<CommandTarget> exactCommandTargets    = matchingCommandsMap.get(EXACT_TARGET);
      List<CommandTarget> matchingCommandTargets = matchingCommandsMap.get(MATCHING_TARGETS);
      matchingCommandsMap.clear();

      if (exactCommandTargets.isEmpty() && matchingCommandTargets.isEmpty()) {
        // No matching commands
        helpText.append(CliStrings.GFSHPARSER__MSG__NO_MATCHING_COMMAND).append(GfshParser.LINE_SEPARATOR);
      } else {
        if (exactCommandTargets.size() == 1) {
          helpText.append(obtainCommandSpecificHelp(exactCommandTargets.get(0), withinShell));
          if (!matchingCommandTargets.isEmpty()) {
            helpText.append(GfshParser.LINE_SEPARATOR);
            helpText.append(CliStrings.format(CliStrings.GFSHPARSER__MSG__OTHER_COMMANDS_STARTING_WITH_0_ARE, userInput));
            for (int i = 0; i < matchingCommandTargets.size(); i++) {
              CommandTarget commandTarget = matchingCommandTargets.get(i);
              helpText.append(commandTarget.getCommandName());
              if (i < matchingCommandTargets.size() - 1) {
                helpText.append(", ");
              }
            }
            helpText.append(GfshParser.LINE_SEPARATOR);
          }
        } else {
          List<CommandTarget> commandTargets = new ArrayList<CommandTarget>();
          commandTargets.addAll(exactCommandTargets);
          commandTargets.addAll(matchingCommandTargets);
          for (CommandTarget commandTarget : commandTargets) {
            String availability = commandTarget.isAvailable() ? HelpUtils.HELP__COMMAND_AVAILABLE : HelpUtils.HELP__COMMAND_NOTAVAILABLE;
            // Many matching commands, provide one line description
            helpText.append(commandTarget.getCommandName());
            if (withinShell) {
              helpText.append(" (").append(availability).append(")");
            }
            helpText.append(GfshParser.LINE_SEPARATOR);
            helpText.append(Gfsh.wrapText(commandTarget.getCommandHelp(), 1)).append(GfshParser.LINE_SEPARATOR);
          }
          helpText.append(GfshParser.LINE_SEPARATOR);

          if (withinShell) {
            helpText.append(
              Gfsh.wrapText(CliStrings.format(CliStrings.GFSHPARSER__MSG__USE_0_HELP_COMMAND_TODISPLAY_DETAILS,
                  appName), 0)).append(GfshParser.LINE_SEPARATOR);
            helpText.append(Gfsh.wrapText(CliStrings.format(
              CliStrings.GFSHPARSER__MSG__HELP_CAN_ALSO_BE_OBTAINED_BY_0_KEY, AbstractShell.completionKeys), 0));
          }
        }
      }
    } catch (IllegalArgumentException e) {
      logWrapper.warning(CliUtil.stackTraceAsString(e));
    } catch (IllegalAccessException e) {
      logWrapper.warning(CliUtil.stackTraceAsString(e));
    } catch (InvocationTargetException e) {
      logWrapper.warning(CliUtil.stackTraceAsString(e));
    }
    return helpText.toString();
  }

  private String obtainCommandSpecificHelp(CommandTarget commandTarget, boolean withinShell) {
    NewHelp newHelp = HelpUtils.getNewHelp(commandTarget, withinShell);
    return newHelp.toString();
  }

  public List<String> getCommandNames(String string) {
    List<String> commandNames = new ArrayList<String>();
    try {
      if (string == null) {
        string = "";
      }
      List<CommandTarget> locateTargets = locateTargets(string);
      for (CommandTarget commandTarget : locateTargets) {
        String key = commandTarget.getGfshMethodTarget().getKey();
        if(key.startsWith(string)){
          commandNames.add(key);
        }
      }
    } catch (IllegalArgumentException e) {
      logWrapper.warning(CliUtil.stackTraceAsString(e));
    } catch (IllegalAccessException e) {
      logWrapper.warning(CliUtil.stackTraceAsString(e));
    } catch (InvocationTargetException e) {
      logWrapper.warning(CliUtil.stackTraceAsString(e));
    }
    return commandNames;
  }

  // ///////////////// Parser interface Methods End //////////////////////////

  private void handleCondition(String message, int errorType, Object errorData) {
    this.handleCondition(message, null, errorType, errorData);
  }

  private void handleCondition(String message, Throwable th, int errorType, Object errorData) {
    if (CliUtil.isGfshVM()) {
      logWarning(message); //TODO - Abhishek add throwable if debug is ON
    } else {
      if (th != null) {
        throw new CommandProcessingException(message + ": " + th.getMessage(), errorType, errorData);
      }
      throw new CommandProcessingException(message, errorType, errorData);
    }
  }

  private void logWarning(String message) {
    if (canLogToConsole()) {
      consoleLogger.warning(CLIConsoleBufferUtil.processMessegeForExtraCharactersFromConsoleBuffer(message));
    } else {
      Gfsh.println(message);
    }
  }

  private boolean canLogToConsole() {
    Gfsh gfsh = Gfsh.getCurrentInstance();
    return gfsh != null && !gfsh.isHeadlessMode() && consoleLogger != null;
  }

//  private void logInfo(String message) {
//    if (consoleLogger != null) {
//      consoleLogger.info(message);
//    } else {
//      Gfsh.println(message);
//    }
//  }
}
