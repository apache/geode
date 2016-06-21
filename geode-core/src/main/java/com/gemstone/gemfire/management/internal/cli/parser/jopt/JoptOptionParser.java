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
package com.gemstone.gemfire.management.internal.cli.parser.jopt;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSpecBuilder;

import org.apache.commons.lang.StringUtils;

import com.gemstone.gemfire.management.internal.cli.MultipleValueConverter;
import com.gemstone.gemfire.management.internal.cli.exceptions.CliCommandOptionException;
import com.gemstone.gemfire.management.internal.cli.exceptions.ExceptionGenerator;
import com.gemstone.gemfire.management.internal.cli.parser.Argument;
import com.gemstone.gemfire.management.internal.cli.parser.GfshOptionParser;
import com.gemstone.gemfire.management.internal.cli.parser.Option;
import com.gemstone.gemfire.management.internal.cli.parser.OptionSet;
import com.gemstone.gemfire.management.internal.cli.parser.SyntaxConstants;
import com.gemstone.gemfire.management.internal.cli.parser.preprocessor.Preprocessor;
import com.gemstone.gemfire.management.internal.cli.parser.preprocessor.PreprocessorUtils;
import com.gemstone.gemfire.management.internal.cli.parser.preprocessor.TrimmedInput;
import com.gemstone.gemfire.management.internal.cli.util.HyphenFormatter;

/**
 * Implementation of {@link GfshOptionParser} which internally makes use of
 * {@link joptsimple.OptionParser}
 *
 * Newly constructed JoptOptionParser must be loaded with arguments and
 * options before parsing command strings.
 * 
 * @since GemFire 7.0
 */
public class JoptOptionParser implements GfshOptionParser {

  private OptionParser parser;
  private LinkedList<Argument> arguments = new LinkedList<Argument>();
  private LinkedList<Option> options;

  /**
   * Constructor
   */
  public JoptOptionParser() {
    parser = new OptionParser(true);
  }

  public void setArguments(LinkedList<Argument> arguments) {
    List<Argument> optional = new LinkedList<Argument>();
    // Let us arrange arguments as mandatory arguments
    // followed by optional arguments
    for (Argument argument : arguments) {
      if (argument.isRequired()) {
        this.arguments.add(argument);
      } else {
        optional.add(argument);
      }
    }
    for (Argument argument : optional) {
      this.arguments.add(argument);
    }
  }

  public void setOptions(LinkedList<Option> options) {
    this.options = options;
    for (Option option : options) {
      addJoptOptionObject(option);
    }
  }

  private void addJoptOptionObject(Option option) {
    OptionSpecBuilder optionBuilder = null;

    optionBuilder = parser.acceptsAll(option.getAggregate(),
        option.getHelp());

    /* Now set the the attributes related to the option */

    ArgumentAcceptingOptionSpec<String> argumentSpecs = null;

    if (option.isWithRequiredArgs()) {
      argumentSpecs = optionBuilder.withRequiredArg();
    } else {
      argumentSpecs = optionBuilder.withOptionalArg();
    }

    if (option.isRequired()) {
      argumentSpecs.required();
    }
    if (option.getValueSeparator() != null) {
      argumentSpecs.withValuesSeparatedBy(option.getValueSeparator());
    }
  }

  public OptionSet parse(String userInput) throws CliCommandOptionException {
    OptionSet optionSet = new OptionSet();
    optionSet.setUserInput(userInput!=null?userInput.trim():"");
    if (userInput != null) {
      TrimmedInput input = PreprocessorUtils.trim(userInput);
      String[] preProcessedInput = preProcess(new HyphenFormatter().formatCommand(input.getString()));
      joptsimple.OptionSet joptOptionSet = null;
      CliCommandOptionException ce = null;
      // int factor = 0;
      try {
        joptOptionSet = parser.parse(preProcessedInput);
      } catch (OptionException e) {
        ce = processException(e);
        // TODO: joptOptionSet = e.getDetected(); // removed when geode-joptsimple was removed
      }
      if (joptOptionSet != null) {

        // Make sure there are no miscellaneous, unknown strings that cannot be identified as
        // either options or arguments.
        if (joptOptionSet.nonOptionArguments().size() > arguments.size()) {
          String unknownString = (String)joptOptionSet.nonOptionArguments().get(arguments.size()); // added cast when geode-joptsimple was removed
          // If the first option is un-parseable then it will be returned as "<option>=<value>" since it's
          // been interpreted as an argument. However, all subsequent options will be returned as "<option>".
          // This hack splits off the string before the "=" sign if it's the first case.
          if (unknownString.matches("^-*\\w+=.*$")) {
            unknownString = unknownString.substring(0, unknownString.indexOf('='));
          }
          // TODO: ce = processException(OptionException.createUnrecognizedOptionException(unknownString, joptOptionSet)); // removed when geode-joptsimple was removed
        }
        
        // First process the arguments
        StringBuffer argument = new StringBuffer();
        int j = 0;
        for (int i = 0; i < joptOptionSet.nonOptionArguments().size()
            && j < arguments.size(); i++) {
          argument = argument.append(joptOptionSet.nonOptionArguments().get(i));
          // Check for syntax of arguments before adding them to the
          // option set as we want to support quoted arguments and those
          // in brackets
          if (PreprocessorUtils.isSyntaxValid(argument.toString())) {
            optionSet.put(arguments.get(j), argument.toString());
            j++;
            argument.delete(0, argument.length());
          }
        }
        if(argument.length()>0){
          // Here we do not need to check for the syntax of the argument
          // because the argument list is now over and this is the last
          // argument which was not added due to improper syntax
          optionSet.put(arguments.get(j),argument.toString());
        }

        // Now process the options
        for (Option option : options) {
          List<String> synonyms = option.getAggregate();
          for (String string : synonyms) {
            if (joptOptionSet.has(string)) {
              // Check whether the user has actually entered the
              // full option or just the start
              boolean present = false;
              outer: for (String inputSplit : preProcessedInput) {
                if (inputSplit.startsWith(SyntaxConstants.LONG_OPTION_SPECIFIER)) {
                  // Remove option prefix
                  inputSplit = StringUtils.removeStart(inputSplit,
                      SyntaxConstants.LONG_OPTION_SPECIFIER);
                  // Remove value specifier
                  inputSplit = StringUtils.removeEnd(inputSplit,
                      SyntaxConstants.OPTION_VALUE_SPECIFIER);
                  if (!inputSplit.equals("")) {
                    if (option.getLongOption().equals(inputSplit)) {
                      present = true;
                      break outer;
                    } else {
                      for (String optionSynonym : option.getSynonyms()) {
                        if (optionSynonym.equals(inputSplit)) {
                          present = true;
                          break outer;
                        }
                      }
                    }
                  }
                }
              }
              if (present) {
                if (joptOptionSet.hasArgument(string)) {
                  List<?> arguments = joptOptionSet.valuesOf(string);
                  if (arguments.size() > 1 && !(option.getConverter() instanceof MultipleValueConverter) && option.getValueSeparator() == null) {
                    List<String> optionList = new ArrayList<String>(1);
                    optionList.add(string);
                    // TODO: ce = processException(new MultipleArgumentsForOptionException(optionList, joptOptionSet)); // removed when geode-joptsimple was removed
                  } else if ((arguments.size() == 1 && !(option.getConverter() instanceof MultipleValueConverter)) || option.getValueSeparator() == null) {
                    optionSet.put(option, arguments.get(0).toString().trim());
                  } else {
                    StringBuffer value = new StringBuffer();
                    String valueSeparator = option.getValueSeparator();
                    for (Object object : joptOptionSet.valuesOf(string)) {
                      if (value.length() == 0) {
                        value.append((String) object);
                      } else {
                        if (valueSeparator != null) {
                          value.append(valueSeparator + ((String) object).trim());
                        } else {
                          value.append(((String) object).trim());
                        }
                      }
                    }
                    optionSet.put(option, value.toString());
                  }
                } else {
                  optionSet.put(option, option.getSpecifiedDefaultValue());
                }
                break;
              }
            }
          }
        }
      }

      // Convert the preProcessedInput into List<String>
      List<String> split = new ArrayList<String>();
      for (int i = 0; i < preProcessedInput.length; i++) {
        split.add(preProcessedInput[i]);
      }
      optionSet
          .setNoOfSpacesRemoved(input.getNoOfSpacesRemoved() /* + factor */);
      optionSet.setSplit(split);
      if (ce != null) {
        ce.setOptionSet(optionSet);
        throw ce;
      }
    }
    return optionSet;
  }

  private CliCommandOptionException processException(final OptionException exception) {
    return ExceptionGenerator.generate(getOption(exception), exception);
  }

  private Option getOption(OptionException oe) {
    Option exceptionOption = null;
    Iterator<String> iterator = oe.options().iterator();
    outermost: for (Option option : options) {
      /* outer: */for (String string : option.getAggregate()) {
        /* inner: */while(iterator.hasNext()) {
          String joptOption = iterator.next();
          if (string.equals(joptOption)) {
            exceptionOption = option;
            break outermost;
          }
        }
      }
    }
    
    if (exceptionOption == null) {
      if (oe.options() != null) {
        if (oe.options().size() > 0) {
          exceptionOption = new Option(oe.options().iterator().next());
        }
      }
    }
    return exceptionOption;
  }

  private String[] preProcess(String userInput) {
    return Preprocessor.split(userInput);
  }

  public LinkedList<Argument> getArguments() {
    return arguments;
  }

  public LinkedList<Option> getOptions() {
    return options;
  }

}
