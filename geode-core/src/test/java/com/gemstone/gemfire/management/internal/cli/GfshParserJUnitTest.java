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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;
import org.springframework.shell.core.Parser;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.shell.event.ParseResult;

import com.gemstone.gemfire.management.cli.CliMetaData;
import com.gemstone.gemfire.management.cli.CommandProcessingException;
import com.gemstone.gemfire.management.cli.ConverterHint;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.annotation.CliArgument;
import com.gemstone.gemfire.management.internal.cli.converters.StringArrayConverter;
import com.gemstone.gemfire.management.internal.cli.converters.StringListConverter;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.parser.SyntaxConstants;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * GfshParserJUnitTest - Includes tests to check the parsing and auto-completion
 * capabilities of {@link GfshParser}
 *
 * @author njadhav
 */
@Category(UnitTest.class)
public class GfshParserJUnitTest {
  
  /**
   * @since 8.1
   */
  private static final Method METHOD_command1;
  static {
    try {
      METHOD_command1 = Commands.class.getMethod("command1", String.class, String.class, String.class, String.class, String.class);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * @since 8.1
   */
  private static final Method METHOD_testParamConcat;
  static {
    try {
      METHOD_testParamConcat = Commands.class.getMethod("testParamConcat", String.class, String[].class, List.class, Integer.class, String[].class);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
  
  /**
   * @since 8.1
   */
  private static final Method METHOD_testMultiWordArg;
  static {
    try {
      METHOD_testMultiWordArg = Commands.class.getMethod("testMultiWordArg", String.class, String.class);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static final String COMMAND1_NAME = "command1";
  private static final String COMMAND1_NAME_ALIAS = "command1_alias";
  private static final String COMMAND2_NAME = "c2";

  private static final String COMMAND1_HELP = "help for " + COMMAND1_NAME;
  // ARGUMENTS
  private static final String ARGUMENT1_NAME = "argument1";
  private static final String ARGUMENT1_HELP = "help for argument1";
  private static final String ARGUMENT1_CONTEXT = "context for argument 1";
  private static final Completion[] ARGUMENT1_COMPLETIONS = {
      new Completion("arg1"), new Completion("arg1alt") };
  private static final String ARGUEMNT2_NAME = "argument2";
  private static final String ARGUMENT2_CONTEXT = "context for argument 2";
  private static final String ARGUMENT2_HELP = "help for argument2";
  private static final String ARGUMENT2_UNSPECIFIED_DEFAULT_VALUE = "{unspecified default value for argument2}";
  private static final Completion[] ARGUMENT2_COMPLETIONS = {
      new Completion("arg2"), new Completion("arg2alt") };

  // OPTIONS
  private static final String OPTION1_NAME = "option1";
  private static final String OPTION1_SYNONYM = "opt1";
  private static final String OPTION1_HELP = "help for option1";
  private static final String OPTION1_CONTEXT = "context for option1";
  private static final Completion[] OPTION1_COMPLETIONS = {
      new Completion("option1"), new Completion("option1Alternate") };
  private static final String OPTION2_NAME = "option2";
  private static final String OPTION2_HELP = "help for option2";
  private static final String OPTION2_CONTEXT = "context for option2";
  private static final String OPTION2_SPECIFIED_DEFAULT_VALUE = "{specified default value for option2}";
  private static final Completion[] OPTION2_COMPLETIONS = {
      new Completion("option2"), new Completion("option2Alternate") };
  private static final String OPTION3_NAME = "option3";
  private static final String OPTION3_SYNONYM = "opt3";
  private static final String OPTION3_HELP = "help for option3";
  private static final String OPTION3_CONTEXT = "context for option3";
  private static final String OPTION3_UNSPECIFIED_DEFAULT_VALUE = "{unspecified default value for option3}";
  private static final Completion[] OPTION3_COMPLETIONS = {
      new Completion("option3"), new Completion("option3Alternate") };

  private CommandManager commandManager;

  private GfshParser parser;

  @Before
  public void setUp() throws Exception {
    // Make sure no prior tests leave the CommandManager in a funky state
    CommandManager.clearInstance();

    commandManager = CommandManager.getInstance(false);
    commandManager.add(Commands.class.newInstance());
    commandManager.add(SimpleConverter.class.newInstance());
    commandManager.add(StringArrayConverter.class.newInstance());
    commandManager.add(StringListConverter.class.newInstance());
    // Set up the parser
    parser = new GfshParser(commandManager);

    CliUtil.isGfshVM = false;
  }

  @After
  public void tearDown() {
    CommandManager.clearInstance();
  }
  
  /**
   * Tests the auto-completion capability of {@link GfshParser} with the method
   * {@link GfshParser#complete(String, int, List)}
   *
   * @throws IllegalAccessException
   * @throws InstantiationException
   * @throws NoSuchMethodException
   * @throws SecurityException
   */
  @Test
  public void testComplete() throws Exception {
    // Get the names of the command
    String[] command1Names = ((CliCommand) METHOD_command1
        .getAnnotation(CliCommand.class)).value();

    // Input contains an entirely different string
    String input = "moc";
    List<String> completionCandidates = new ArrayList<String>();
    List<String> completionValues = new ArrayList<String>();
    parser.complete(input, input.length(), completionCandidates);
    assertSimpleCompletionValues(completionValues, completionCandidates);

    // Input contains a string which is prefix
    // of more than 1 command
    input = "c";
    clearAndSimpleComplete(completionCandidates,completionValues,input,parser);
    // completions will come alphabetically sorted
    completionValues.add(COMMAND2_NAME);
    completionValues.add(COMMAND1_NAME);
    assertSimpleCompletionValues(completionValues, completionCandidates);

    // Input contains only prefix of the command
    // name which is not a prefix of other command.
    // It may be the prefix for the synonym of command
    input = command1Names[0].substring(0, 3);
    clearAndSimpleComplete(completionCandidates,completionValues,input,parser);
    completionValues.add(COMMAND1_NAME);
    assertSimpleCompletionValues(completionValues, completionCandidates);

    // Input contains only the command name
    input = command1Names[0];
    clearAndSimpleComplete(completionCandidates,completionValues,input,parser);
    // Here we expect the completions for argument1
    // For arguments, the formatted value will equal the actual arguments
    // But the actual value will contain the ARGUMENT_SEPARATOR
    for (Completion completion : ARGUMENT1_COMPLETIONS) {
      completionValues.add(" "
          + completion.getValue());
    }
    assertSimpleCompletionValues(completionValues, completionCandidates);

    // Input contains command name and prefix of first
    // argument
    input = command1Names[0] + " "
        + ARGUMENT1_COMPLETIONS[0].getValue().substring(0, 3);
    clearAndSimpleComplete(completionCandidates,completionValues,input,parser);
    // Here we expect the completions for argument2
    // which have the provided first argument as the prefix
    for (Completion completion : ARGUMENT1_COMPLETIONS) {
      if (completion.getValue().startsWith(
          ARGUMENT1_COMPLETIONS[0].getValue().substring(0, 3))) {
        completionValues.add(" "
            + completion.getValue());
      }
    }
    assertSimpleCompletionValues(completionValues, completionCandidates);

    // Input contains command name and first argument
    input = command1Names[0] + " "
        + ARGUMENT1_COMPLETIONS[0].getValue();
    clearAndSimpleComplete(completionCandidates,completionValues,input,parser);
    // Here we expect the completions for argument2
    for (Completion completion : ARGUMENT2_COMPLETIONS) {
      completionValues.add(SyntaxConstants.ARGUMENT_SEPARATOR
          + completion.getValue());
    }
    assertSimpleCompletionValues(completionValues, completionCandidates);

    // Input contains command name, first argument and the prefix of second
    // argument
    input = command1Names[0] + " "
        + ARGUMENT1_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + ARGUMENT2_COMPLETIONS[0].getValue().substring(0, 2);
    clearAndSimpleComplete(completionCandidates,completionValues,input,parser);
    // Here we expect the completions for argument2
    for (Completion completion : ARGUMENT2_COMPLETIONS) {
      if (completion.getValue().startsWith(
          ARGUMENT2_COMPLETIONS[0].getValue().substring(0, 2))) {
        completionValues.add(SyntaxConstants.ARGUMENT_SEPARATOR
            + completion.getValue());
      }
    }
    assertSimpleCompletionValues(completionValues, completionCandidates);

    // Input contains command name, first argument and second argument
    input = COMMAND1_NAME + " "
        + ARGUMENT1_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + ARGUMENT2_COMPLETIONS[0].getValue();
    clearAndSimpleComplete(completionCandidates,completionValues,input,parser);
    // Here we expect all the mandatory options
    completionValues.add(" " + SyntaxConstants.LONG_OPTION_SPECIFIER
        + OPTION1_NAME);
    assertSimpleCompletionValues(completionValues, completionCandidates);

    // Input contains command name (synonym), first argument, second argument,
    // prefix of first option
    input = command1Names[1] + " "
        + ARGUMENT1_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + ARGUMENT2_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION1_NAME.substring(0, 3);
    clearAndSimpleComplete(completionCandidates,completionValues,input,parser);
    // Here we expect the names for all the options
    // whose prefix matches with the provided prefix
    completionValues.add(" " + SyntaxConstants.LONG_OPTION_SPECIFIER
        + OPTION1_NAME);
    if (OPTION2_NAME.startsWith(OPTION1_NAME.substring(0, 3))) {
      completionValues.add(" " + SyntaxConstants.LONG_OPTION_SPECIFIER
          + OPTION2_NAME);
    }
    if (OPTION3_NAME.startsWith(OPTION1_NAME.substring(0, 3))) {
      completionValues.add(" " + SyntaxConstants.LONG_OPTION_SPECIFIER
          + OPTION3_NAME);
    }
    assertSimpleCompletionValues(completionValues, completionCandidates);

    // Input contains command name, first argument, second argument
    // and first option
    input = command1Names[0] + " "
        + ARGUMENT1_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + ARGUMENT2_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION1_NAME;
    clearAndSimpleComplete(completionCandidates,completionValues,input,parser);
    // Here we expect the values for the first option
    for (Completion completion : OPTION1_COMPLETIONS) {
      completionValues.add(SyntaxConstants.OPTION_VALUE_SPECIFIER
          + completion.getValue());
    }
    assertSimpleCompletionValues(completionValues, completionCandidates);

    // Input contains command name, first argument, second argument,
    // first option and prefix of one of the values provided
    // by the auto-completor.
    input = command1Names[1] + " "
        + ARGUMENT1_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + ARGUMENT2_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + " "
        + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION1_NAME
        + SyntaxConstants.OPTION_VALUE_SPECIFIER
        + OPTION1_COMPLETIONS[0].getValue().substring(0, 2);
    clearAndSimpleComplete(completionCandidates,completionValues,input,parser);
    // Here we expect the values for the first option
    for (Completion completion : OPTION1_COMPLETIONS) {
      if (completion.getValue().startsWith(
          OPTION1_COMPLETIONS[0].getValue().substring(0, 2))) {
        completionValues.add(SyntaxConstants.OPTION_VALUE_SPECIFIER
            + completion.getValue());
      }
    }
    assertSimpleCompletionValues(completionValues, completionCandidates);

    // Input contains command name, first argument, second argument,
    // first option and one of the values provided
    // by the auto-completor.
    input = command1Names[0] + " "
        + ARGUMENT1_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + ARGUMENT2_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + " "
        + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION1_NAME
        + SyntaxConstants.OPTION_VALUE_SPECIFIER
        + OPTION1_COMPLETIONS[0].getValue();
    clearAndSimpleComplete(completionCandidates,completionValues,input,parser);
    // Here we expect the remaining options
    // As only first option is mandatory, we expect the
    // the other non-mandatory options.
    completionValues.add(" " + SyntaxConstants.LONG_OPTION_SPECIFIER
        + OPTION2_NAME);
    completionValues.add(" " + SyntaxConstants.LONG_OPTION_SPECIFIER
        + OPTION3_NAME);
    assertSimpleCompletionValues(completionValues, completionCandidates);

    // Input contains command name, first argument, second argument,
    // first option, one value for the option and value separator at
    // the end
    input = command1Names[0] + " "
        + ARGUMENT1_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + ARGUMENT2_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + " "
        + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION1_NAME
        + SyntaxConstants.OPTION_VALUE_SPECIFIER
        + OPTION1_COMPLETIONS[0].getValue()
        + SyntaxConstants.VALUE_SEPARATOR;
    clearAndSimpleComplete(completionCandidates,completionValues,input,parser);
    // Here we expect the other values for completion
    completionValues.add(SyntaxConstants.VALUE_SEPARATOR
        + OPTION1_COMPLETIONS[1].getValue());
    assertSimpleCompletionValues(completionValues, completionCandidates);

    // Input contains command name, first argument, second argument,
    // first option and both the values for the option
    input = command1Names[0] + " "
        + ARGUMENT1_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + ARGUMENT2_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + " "
        + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION1_NAME
        + SyntaxConstants.OPTION_VALUE_SPECIFIER
        + OPTION1_COMPLETIONS[0].getValue()
        + SyntaxConstants.VALUE_SEPARATOR
        + OPTION1_COMPLETIONS[1].getValue();
    clearAndSimpleComplete(completionCandidates,completionValues,input,parser);
    // Here we expect the remaining options
    // As only first option is mandatory, we expect the
    // the other non-mandatory options.
    completionValues.add(" " + SyntaxConstants.LONG_OPTION_SPECIFIER
        + OPTION2_NAME);
    completionValues.add(" " + SyntaxConstants.LONG_OPTION_SPECIFIER
        + OPTION3_NAME);
    assertSimpleCompletionValues(completionValues, completionCandidates);

    // Input contains command name, first argument, second argument,
    // first option, both the values for the option and valueSeparator
    // at the end
    input = command1Names[0] + " "
        + ARGUMENT1_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + ARGUMENT2_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + " "
        + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION1_NAME
        + SyntaxConstants.OPTION_VALUE_SPECIFIER
        + OPTION1_COMPLETIONS[0].getValue()
        + SyntaxConstants.VALUE_SEPARATOR
        + OPTION1_COMPLETIONS[1].getValue()
        + SyntaxConstants.VALUE_SEPARATOR;
    clearAndSimpleComplete(completionCandidates,completionValues,input,parser);
    // Here we expect nothing for completion
    assertSimpleCompletionValues(completionValues, completionCandidates);

    // Input contains command name, first argument, second argument,
    // first option and multiple values assigned to it. It also contains
    // start of the second option
    input = command1Names[0] + " "
        + ARGUMENT1_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + ARGUMENT2_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + " "
        + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION1_NAME
        + SyntaxConstants.OPTION_VALUE_SPECIFIER
        + OPTION1_COMPLETIONS[0].getValue()
        + SyntaxConstants.VALUE_SEPARATOR
        + OPTION1_COMPLETIONS[1].getValue()
        + " "
        + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION2_NAME.substring(0, 3);
    clearAndSimpleComplete(completionCandidates,completionValues,input,parser);
    // Here we expect those options which have not been specified
    // before and which have the prefix specified herein as their
    // prefix
    completionValues.add(" " + SyntaxConstants.LONG_OPTION_SPECIFIER
        + OPTION2_NAME);
    if (OPTION3_NAME.startsWith(OPTION2_NAME.substring(0, 3))) {
      completionValues.add(" " + SyntaxConstants.LONG_OPTION_SPECIFIER
          + OPTION3_NAME);
    }
    assertSimpleCompletionValues(completionValues, completionCandidates);
  }

  private void clearAndSimpleComplete(List<String> completionCandidates,List<String> completionValues,String input,Parser parser){
    completionCandidates.clear();
    completionValues.clear();
    parser.complete(input, input.length(), completionCandidates);
  }

  private void assertSimpleCompletionValues(List<String> expected,
      List<String> actual) {
    // TODO Auto-generated method stub
    assertEquals("Check size", expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals("Check completion value no." + i +". Expected("+expected.get(i)+") & Actual("+actual.get(i)+").", expected.get(i),
          actual.get(i));
    }
  }

  /**
   * Tests the auto-completion capability of {@link GfshParser} with the method
   * {@link GfshParser#completeAdvanced(String, int, List)}
   *
   * @throws IllegalAccessException
   * @throws InstantiationException
   * @throws NoSuchMethodException
   * @throws SecurityException
   */
  @Test
  public void testCompleteAdvanced() throws Exception {
    // Get the names of the command
    String[] command1Names = ((CliCommand) METHOD_command1
        .getAnnotation(CliCommand.class)).value();

    // Input contains an entirely different string
    String input = "moc";
    List<Completion> completionCandidates = new ArrayList<Completion>();
    List<Completion> completionValues = new ArrayList<Completion>();
    parser.completeAdvanced(input, input.length(), completionCandidates);
    assertAdvancedCompletionValues(completionValues, completionCandidates);

    // Input contains a string which is prefix
    // of more than 1 command
    input = "c";
    clearAndAdvancedComplete(completionCandidates, completionValues, input,parser);
    // completions will come alphabetically sorted
    completionValues.add(new Completion(COMMAND2_NAME));
    completionValues.add(new Completion(COMMAND1_NAME));
    assertAdvancedCompletionValues(completionValues, completionCandidates);

    // Input contains only prefix of the command
    // name which is not a prefix of other command.
    // It may be the prefix for the synonym of command
    input = command1Names[0].substring(0, 3);
    clearAndAdvancedComplete(completionCandidates, completionValues, input,parser);
    completionValues.add(new Completion(COMMAND1_NAME));
    assertAdvancedCompletionValues(completionValues, completionCandidates);

    // Input contains only the command name
    input = command1Names[0];
    clearAndAdvancedComplete(completionCandidates, completionValues, input,parser);
    // Here we expect the completions for argument1
    // For arguments, the formatted value will equal the actual arguments
    // But the actual value will contain the ARGUMENT_SEPARATOR
    for (Completion completion : ARGUMENT1_COMPLETIONS) {
      completionValues.add(new Completion(" "
          + completion.getValue(), completion.getFormattedValue(), null, 0));
    }
    assertAdvancedCompletionValues(completionValues, completionCandidates);

    // Input contains command name and prefix of first
    // argument
    input = command1Names[0] + " "
        + ARGUMENT1_COMPLETIONS[0].getValue().substring(0, 3);
    clearAndAdvancedComplete(completionCandidates, completionValues, input,parser);
    // Here we expect the completions for argument2
    // which have the provided first argument as the prefix
    for (Completion completion : ARGUMENT1_COMPLETIONS) {
      if (completion.getValue().startsWith(
          ARGUMENT1_COMPLETIONS[0].getValue().substring(0, 3))) {
        completionValues.add(new Completion(" "
            + completion.getValue(), completion.getFormattedValue(), null, 0));
      }
    }
    assertAdvancedCompletionValues(completionValues, completionCandidates);

    // Input contains command name and first argument
    input = command1Names[0] + " "
        + ARGUMENT1_COMPLETIONS[0].getValue();
    clearAndAdvancedComplete(completionCandidates, completionValues, input,parser);
    // Here we expect the completions for argument2
    for (Completion completion : ARGUMENT2_COMPLETIONS) {
      completionValues.add(new Completion(SyntaxConstants.ARGUMENT_SEPARATOR
          + completion.getValue(), completion.getFormattedValue(), null, 0));
    }
    assertAdvancedCompletionValues(completionValues, completionCandidates);

    // Input contains command name, first argument and the prefix of second
    // argument
    input = command1Names[0] + " "
        + ARGUMENT1_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + ARGUMENT2_COMPLETIONS[0].getValue().substring(0, 2);
    clearAndAdvancedComplete(completionCandidates, completionValues, input,parser);
    // Here we expect the completions for argument2
    for (Completion completion : ARGUMENT2_COMPLETIONS) {
      if (completion.getValue().startsWith(
          ARGUMENT2_COMPLETIONS[0].getValue().substring(0, 2))) {
        completionValues.add(new Completion(SyntaxConstants.ARGUMENT_SEPARATOR
            + completion.getValue(), completion.getFormattedValue(), null, 0));
      }
    }
    assertAdvancedCompletionValues(completionValues, completionCandidates);

    // Input contains command name, first argument and second argument
    input = COMMAND1_NAME + " "
        + ARGUMENT1_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + ARGUMENT2_COMPLETIONS[0].getValue();
    clearAndAdvancedComplete(completionCandidates, completionValues, input,parser);
    // Here we expect all the mandatory options
    completionValues.add(new Completion(" "
        + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION1_NAME, OPTION1_NAME,
        null, 0));
    assertAdvancedCompletionValues(completionValues, completionCandidates);

    // Input contains command name (synonym), first argument, second argument,
    // prefix of first option
    input = command1Names[1] + " "
        + ARGUMENT1_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + ARGUMENT2_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION1_NAME.substring(0, 3);
    clearAndAdvancedComplete(completionCandidates, completionValues, input,parser);
    // Here we expect the names for all the options
    // whose prefix matches with the provided prefix
    completionValues.add(new Completion(" "
        + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION1_NAME, OPTION1_NAME,
        null, 0));
    if (OPTION2_NAME.startsWith(OPTION1_NAME.substring(0, 3))) {
      completionValues.add(new Completion(" "
          + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION2_NAME, OPTION2_NAME,
          null, 0));
    }
    if (OPTION3_NAME.startsWith(OPTION1_NAME.substring(0, 3))) {
      completionValues.add(new Completion(" "
          + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION3_NAME, OPTION3_NAME,
          null, 0));
    }
    assertAdvancedCompletionValues(completionValues, completionCandidates);

    // Input contains command name, first argument, second argument
    // and first option
    input = command1Names[0] + " "
        + ARGUMENT1_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + ARGUMENT2_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION1_NAME;
    clearAndAdvancedComplete(completionCandidates, completionValues, input,parser);
    // Here we expect the values for the first option
    for (Completion completion : OPTION1_COMPLETIONS) {
      completionValues.add(new Completion(
          SyntaxConstants.OPTION_VALUE_SPECIFIER + completion.getValue(),
          completion.getValue(), null, 0));
    }
    assertAdvancedCompletionValues(completionValues, completionCandidates);

    // Input contains command name, first argument, second argument,
    // first option and prefix of one of the values provided
    // by the auto-completor.
    input = command1Names[1] + " "
        + ARGUMENT1_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + ARGUMENT2_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + " "
        + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION1_NAME
        + SyntaxConstants.OPTION_VALUE_SPECIFIER
        + OPTION1_COMPLETIONS[0].getValue().substring(0, 2);
    clearAndAdvancedComplete(completionCandidates, completionValues, input,parser);
    // Here we expect the values for the first option
    for (Completion completion : OPTION1_COMPLETIONS) {
      if (completion.getValue().startsWith(
          OPTION1_COMPLETIONS[0].getValue().substring(0, 2))) {
        completionValues.add(new Completion(
            SyntaxConstants.OPTION_VALUE_SPECIFIER + completion.getValue(),
            completion.getValue(), null, 0));
      }
    }
    assertAdvancedCompletionValues(completionValues, completionCandidates);

    // Input contains command name, first argument, second argument,
    // first option and one of the values provided
    // by the auto-completor.
    input = command1Names[0] + " "
        + ARGUMENT1_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + ARGUMENT2_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + " "
        + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION1_NAME
        + SyntaxConstants.OPTION_VALUE_SPECIFIER
        + OPTION1_COMPLETIONS[0].getValue();
    clearAndAdvancedComplete(completionCandidates, completionValues, input,parser);
    // Here we expect the remaining options
    // As only first option is mandatory, we expect the
    // the other non-mandatory options.
    completionValues.add(new Completion(" "
        + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION2_NAME, OPTION2_NAME,
        null, 0));
    completionValues.add(new Completion(" "
        + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION3_NAME, OPTION3_NAME,
        null, 0));
    assertAdvancedCompletionValues(completionValues, completionCandidates);

    // Input contains command name, first argument, second argument,
    // first option, one value for the option and value separator at
    // the end
    input = command1Names[0] + " "
        + ARGUMENT1_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + ARGUMENT2_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + " "
        + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION1_NAME
        + SyntaxConstants.OPTION_VALUE_SPECIFIER
        + OPTION1_COMPLETIONS[0].getValue()
        + SyntaxConstants.VALUE_SEPARATOR;
    clearAndAdvancedComplete(completionCandidates, completionValues, input,parser);
    // Here we expect the other values for completion
    completionValues.add(new Completion(SyntaxConstants.VALUE_SEPARATOR
        + OPTION1_COMPLETIONS[1].getValue(), OPTION1_COMPLETIONS[1].getValue(),
        null, 0));
    assertAdvancedCompletionValues(completionValues, completionCandidates);

    // Input contains command name, first argument, second argument,
    // first option and both the values for the option
    input = command1Names[0] + " "
        + ARGUMENT1_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + ARGUMENT2_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + " "
        + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION1_NAME
        + SyntaxConstants.OPTION_VALUE_SPECIFIER
        + OPTION1_COMPLETIONS[0].getValue()
        + SyntaxConstants.VALUE_SEPARATOR
        + OPTION1_COMPLETIONS[1].getValue();
    clearAndAdvancedComplete(completionCandidates, completionValues, input,parser);
    // Here we expect the remaining options
    // As only first option is mandatory, we expect the
    // the other non-mandatory options.
    completionValues.add(new Completion(" "
        + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION2_NAME, OPTION2_NAME,
        null, 0));
    completionValues.add(new Completion(" "
        + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION3_NAME, OPTION3_NAME,
        null, 0));
    assertAdvancedCompletionValues(completionValues, completionCandidates);

    // Input contains command name, first argument, second argument,
    // first option, both the values for the option and valueSeparator
    // at the end
    input = command1Names[0] + " "
        + ARGUMENT1_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + ARGUMENT2_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + " "
        + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION1_NAME
        + SyntaxConstants.OPTION_VALUE_SPECIFIER
        + OPTION1_COMPLETIONS[0].getValue()
        + SyntaxConstants.VALUE_SEPARATOR
        + OPTION1_COMPLETIONS[1].getValue()
        + SyntaxConstants.VALUE_SEPARATOR;
    clearAndAdvancedComplete(completionCandidates, completionValues, input,parser);
    // Here we expect nothing for completion
    assertAdvancedCompletionValues(completionValues, completionCandidates);

    // Input contains command name, first argument, second argument,
    // first option and multiple values assigned to it. It also contains
    // start of the second option
    input = command1Names[0] + " "
        + ARGUMENT1_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + ARGUMENT2_COMPLETIONS[0].getValue()
        + SyntaxConstants.ARGUMENT_SEPARATOR
        + " "
        + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION1_NAME
        + SyntaxConstants.OPTION_VALUE_SPECIFIER
        + OPTION1_COMPLETIONS[0].getValue()
        + SyntaxConstants.VALUE_SEPARATOR
        + OPTION1_COMPLETIONS[1].getValue()
        + " "
        + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION2_NAME.substring(0, 3);
    clearAndAdvancedComplete(completionCandidates, completionValues, input,parser);
    // Here we expect those options which have not been specified
    // before and which have the prefix specified herein as their
    // prefix
    completionValues.add(new Completion(" "
        + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION2_NAME, OPTION2_NAME,
        null, 0));
    if (OPTION3_NAME.startsWith(OPTION2_NAME.substring(0, 3))) {
      completionValues.add(new Completion(" "
          + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION3_NAME, OPTION3_NAME,
          null, 0));
    }
    assertAdvancedCompletionValues(completionValues, completionCandidates);

  }

  private void clearAndAdvancedComplete(List<Completion> completionCandidates,List<Completion> completionValues,String input,Parser parser){
    completionCandidates.clear();
    completionValues.clear();
    parser.completeAdvanced(input, input.length(), completionCandidates);
  }

  private void assertAdvancedCompletionValues(List<Completion> expected,
      List<Completion> actual) {
    // TODO Auto-generated method stub
    assertEquals("Check size", expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals("Check completion value no." + i+". Expected("+expected.get(i)+") & Actual("+actual.get(i)+").",
          expected.get(i).getValue(), actual.get(i).getValue());
      if (expected.get(i).getFormattedValue() != null) {
        assertEquals("Check completion formatted value no." + i +". Expected("+expected.get(i).getFormattedValue()+") & Actual("+actual.get(i).getFormattedValue()+").", expected
            .get(i).getFormattedValue(), actual.get(i).getFormattedValue());
      }
    }
  }

  /**
   * Test for checking parsing of {@link GfshParser} with method
   * {@link GfshParser#parse(String)}
   *
   * Does not include testing for multiple values as this change is still
   * pending in spring-shell
   *
   * @throws InstantiationException
   * @throws IllegalAccessException
   * @throws NoSuchMethodException
   * @throws SecurityException
   */
  @Test
  public void testParse() throws Exception {
    // Get the names of the command
    String[] command1Names = ((CliCommand) METHOD_command1
        .getAnnotation(CliCommand.class)).value();

    // Input contains an entirely different string
    String input = "moc";
    ParseResult parse = null;
    CommandProcessingException expectedException = null;
    try {
      parse = parser.parse(input);
    } catch (CommandProcessingException e) {
      expectedException = e;
    } finally {
      assertNotNull("Expecting a "+CommandProcessingException.class+" for an invalid command name: "+input, expectedException);
      assertEquals("CommandProcessingException type doesn't match. "
          + "Actual(" + expectedException.getErrorType() + ") & Expected("
          + CommandProcessingException.COMMAND_INVALID_OR_UNAVAILABLE + ") ",
          expectedException.getErrorType(),
          CommandProcessingException.COMMAND_INVALID_OR_UNAVAILABLE);
    }

    // Input contains a string which is prefix
    // of more than 1 command
    input = "c";
    expectedException = null;
    try {
      parse = parser.parse(input);
    } catch (CommandProcessingException e) {
      expectedException = e;
    } finally {
      assertNotNull("Expecting a "+CommandProcessingException.class+" for an invalid/incomplete command name: "+input, expectedException);
      assertEquals("CommandProcessingException type doesn't match. Actual("
          + expectedException.getErrorType() + ") & Expected("
          + CommandProcessingException.COMMAND_INVALID_OR_UNAVAILABLE + ") ",
          expectedException.getErrorType(),
          CommandProcessingException.COMMAND_INVALID_OR_UNAVAILABLE);
    }

    // Input contains only prefix of the command
    // name which is not a prefix of other command.
    // It may be the prefix for the synonym of command
    input = "com";
    expectedException = null;
    try {
      parse = parser.parse(input);
    } catch (CommandProcessingException e) {
      expectedException = e;
    } finally {
      //FIXME - Nikhil/Abhishek prefix shouldn't work
      assertNotNull("Expecting a "+CommandProcessingException.class+" for an invalid/incomplete command name: "+input, expectedException);
      assertEquals("CommandProcessingException type doesn't match. Actual("
          + expectedException.getErrorType() + ") & Expected("
          + CommandProcessingException.COMMAND_INVALID_OR_UNAVAILABLE + ") ",
          expectedException.getErrorType(),
          CommandProcessingException.COMMAND_INVALID_OR_UNAVAILABLE);
    }

    // Input contains only command name
    input = command1Names[0];
    expectedException = null;
    try {
      parse = parser.parse(input);
    } catch (CommandProcessingException e) {
      expectedException = e;
    } finally {
      assertNotNull("Expecting a "+CommandProcessingException.class+" for an invalid/incomplete command name: "+input, expectedException);
      assertEquals("CommandProcessingException type doesn't match. Actual("
          + expectedException.getErrorType() + ") & Expected("
          + CommandProcessingException.REQUIRED_ARGUMENT_MISSING + ") ",
          CommandProcessingException.REQUIRED_ARGUMENT_MISSING,
          expectedException.getErrorType());
    }

    // Input contains first argument and first option with value
    input = command1Names[0] + " ARGUMENT1_VALUE "
        + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION1_NAME
        + SyntaxConstants.OPTION_VALUE_SPECIFIER + "somevalue";
    parse = parser.parse(input);
    assertNotNull(parse);
    assertEquals("Check ParseResult method", parse.getMethod(), METHOD_command1);
    assertEquals("Check no. of method arguments", 5,
        parse.getArguments().length);
    assertEquals("Check argument1", "ARGUMENT1_VALUE", parse.getArguments()[0]);
    assertEquals("Check argument2", ARGUMENT2_UNSPECIFIED_DEFAULT_VALUE,
        parse.getArguments()[1]);
    assertEquals("Check option1 value", "somevalue", parse.getArguments()[2]);
    assertEquals("Check option2 value", null, parse.getArguments()[3]);
    assertEquals("Check option3 value", OPTION3_UNSPECIFIED_DEFAULT_VALUE,
        parse.getArguments()[4]);

    // Input contains only both arguments but is terminated by long option
    // specifiers. These hyphens at the end are ignored by the parser
    input = command1Names[1]
        + " ARGUMENT1_VALUE?      ARGUMENT2_VALUE -- ----------";
    try {
      parse = parser.parse(input);
    } catch (CommandProcessingException e) {
      expectedException = e;
    } finally {
      assertNotNull("Expecting a "+CommandProcessingException.class+" for an invalid/incomplete command name: "+input, expectedException);
      assertEquals("CommandProcessingException type doesn't match. Actual("
          + expectedException.getErrorType() + ") & Expected("
          + CommandProcessingException.REQUIRED_OPTION_MISSING + ") ",
          expectedException.getErrorType(),
          CommandProcessingException.REQUIRED_OPTION_MISSING);
    }

    // Input contains both arguments. The first option is specified with value
    // The second is specified without value and the third option is not
    // specified
    input = command1Names[1]
        + "         ARGUMENT1_VALUE?       ARGUMENT2_VALUE "
        + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION1_NAME
        + SyntaxConstants.OPTION_VALUE_SPECIFIER + "option1value" + " "
        + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION2_NAME;
    parse = parser.parse(input);
    assertNotNull(parse);
    assertEquals("Check ParseResult method", parse.getMethod(), METHOD_command1);
    assertEquals("Check no. of method arguments", 5,
        parse.getArguments().length);
    assertEquals("Check argument1", "ARGUMENT1_VALUE", parse.getArguments()[0]);
    assertEquals("Check argument2", "ARGUMENT2_VALUE", parse.getArguments()[1]);
    assertEquals("Check option1 value", "option1value", parse.getArguments()[2]);
    assertEquals("Check option2 value", OPTION2_SPECIFIED_DEFAULT_VALUE,
        parse.getArguments()[3]);
    assertEquals("Check option3 value", OPTION3_UNSPECIFIED_DEFAULT_VALUE,
        parse.getArguments()[4]);

    // Input contains both arguments. All the three options
    // are specified with values
    input = command1Names[1]
        + "         ARGUMENT1_VALUE?       ARGUMENT2_VALUE "
        + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION1_SYNONYM
        + SyntaxConstants.OPTION_VALUE_SPECIFIER + "option1value" + " "
        + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION2_NAME
        + SyntaxConstants.OPTION_VALUE_SPECIFIER + "option2value" + " "
        + SyntaxConstants.LONG_OPTION_SPECIFIER + OPTION3_NAME
        + SyntaxConstants.OPTION_VALUE_SPECIFIER + "option3value";
    parse = parser.parse(input);
    assertNotNull(parse);
    assertEquals("Check ParseResult method", parse.getMethod(), METHOD_command1);
    assertEquals("Check no. of method arguments", 5,
        parse.getArguments().length);
    assertEquals("Check argument1", "ARGUMENT1_VALUE", parse.getArguments()[0]);
    assertEquals("Check argument2", "ARGUMENT2_VALUE", parse.getArguments()[1]);
    assertEquals("Check option1 value", "option1value", parse.getArguments()[2]);
    assertEquals("Check option2 value", "option2value", parse.getArguments()[3]);
    assertEquals("Check option3 value", "option3value", parse.getArguments()[4]);

    // Test concatenation of options when they appear more than once in the command
    String command = "testParamConcat --string=string1 --stringArray=1,2 --stringArray=3,4 --stringList=11,12,13 --integer=10 --stringArray=5 --stringList=14,15";
    ParseResult parseResult = parser.parse(command);
    assertNotNull(parseResult);
    assertEquals("Check ParseResult method", parseResult.getMethod(), METHOD_testParamConcat);
    assertEquals("Check no. of method arguments", 5, parseResult.getArguments().length);
    Object[] arguments = parseResult.getArguments();
    assertEquals(arguments[0], "string1");
    assertEquals(((String[]) arguments[1])[0], "1");
    assertEquals(((String[]) arguments[1])[1], "2");
    assertEquals(((String[]) arguments[1])[2], "3");
    assertEquals(((String[]) arguments[1])[3], "4");
    assertEquals(((String[]) arguments[1])[4], "5");
    assertEquals(((List) arguments[2]).get(0), "11");
    assertEquals(((List) arguments[2]).get(1), "12");
    assertEquals(((List) arguments[2]).get(2), "13");
    assertEquals(((List) arguments[2]).get(3), "14");
    assertEquals(((List) arguments[2]).get(4), "15");
    assertEquals(arguments[3], 10);

    // Test concatenation of options when they appear more than once in the command
    command = "testParamConcat --stringArray=1,2 --stringArray=\'3,4\'";
    parseResult = parser.parse(command);
    assertNotNull(parseResult);
    assertEquals("Check ParseResult method", parseResult.getMethod(), METHOD_testParamConcat);
    assertEquals("Check no. of method arguments", 5, parseResult.getArguments().length);
    arguments = parseResult.getArguments();
    assertEquals(((String[]) arguments[1])[0], "1");
    assertEquals(((String[]) arguments[1])[1], "2");
    assertEquals(((String[]) arguments[1])[2], "3,4");

    command = "testParamConcat --string=\"1\" --colonArray=2:3:4 --stringArray=5,\"6,7\",8 --stringList=\"9,10,11,12\"";
    parseResult = parser.parse(command);
    assertNotNull(parseResult);
    assertEquals("Check ParseResult method", parseResult.getMethod(), METHOD_testParamConcat);
    assertEquals("Check no. of method arguments", 5, parseResult.getArguments().length);
    arguments = parseResult.getArguments();
    assertEquals(arguments[0], "1");
    assertEquals(((String[]) arguments[1])[0], "5");
    assertEquals(((String[]) arguments[1])[1], "6,7");
    assertEquals(((String[]) arguments[1])[2], "8");
    assertEquals(((List) arguments[2]).get(0), "9,10,11,12");
    assertEquals(((String[]) arguments[4])[0], "2");
    assertEquals(((String[]) arguments[4])[1], "3");
    assertEquals(((String[]) arguments[4])[2], "4");

    try {
    command = "testParamConcat --string=string1 --stringArray=1,2 --string=string2";
    parseResult = parser.parse(command);
    fail("Should have received a CommandProcessingException due to 'string' being specified twice");
    } catch (CommandProcessingException expected) {
      // Expected
    }

    command = "testMultiWordArg this is just one argument?this is a second argument";
    parseResult = parser.parse(command);
    assertNotNull(parseResult);
    assertEquals("Check ParseResult method", parseResult.getMethod(), METHOD_testMultiWordArg);
    assertEquals("Check no. of method arguments", 2, parseResult.getArguments().length);
    arguments = parseResult.getArguments();
    assertEquals(arguments[0], "this is just one argument");
    assertEquals(arguments[1], "this is a second argument");
  }

  public void testDefaultAvailabilityMessage() throws Exception {
    checkAvailabilityMessage(new AvailabilityCommands(), AvailabilityCommands.C2_NAME, AvailabilityCommands.C2_MSG_UNAVAILABLE, AvailabilityCommands.C2_PROP);
  }

  public void testCustomAvailabilityMessage() throws Exception {
    checkAvailabilityMessage(new AvailabilityCommands(), AvailabilityCommands.C1_NAME, AvailabilityCommands.C1_MSG_UNAVAILABLE, AvailabilityCommands.C1_PROP);
  }

  public void checkAvailabilityMessage(CommandMarker availabilityCommands, String commandString, String unavailableMessage, String availabiltyBooleanProp) throws Exception {
    CommandManager cmdManager =  CommandManager.getInstance(false);
    cmdManager.add(availabilityCommands);

    GfshParser parser = new GfshParser(cmdManager);
    ParseResult parseResult = null;

    // Case 1: Command is not available
    try {
      parseResult = parser.parse(commandString);
    } catch (CommandProcessingException e) {
      String actualMessage = e.getMessage();
      String expectedMessage = CliStrings.format(CliStrings.GFSHPARSER__MSG__0_IS_NOT_AVAILABLE_REASON_1, new Object[] {commandString, unavailableMessage});
      assertEquals("1. Unavailabilty message ["+actualMessage+"] is not as expected["+expectedMessage+"].", actualMessage, expectedMessage);
    }

    // Case 2: Command is 'made' available
    try {
      System.setProperty(availabiltyBooleanProp, "true");
      parseResult = parser.parse(commandString);
      assertNotNull("ParseResult should not be null for available command.", parseResult);
    } catch (CommandProcessingException e) {
      fail("Command \""+commandString+"\" is expected to be available");
      e.printStackTrace();
    } finally {
      System.clearProperty(availabiltyBooleanProp);
    }

    // Case 3: Command is not available again
    try {
      parseResult = parser.parse(commandString);
    } catch (CommandProcessingException e) {
      String actualMessage = e.getMessage();
      String expectedMessage = CliStrings.format(CliStrings.GFSHPARSER__MSG__0_IS_NOT_AVAILABLE_REASON_1, new Object[] {commandString, unavailableMessage});
      assertEquals("2. Unavailabilty message ["+actualMessage+"] is not as expected["+expectedMessage+"].", actualMessage, expectedMessage);
    }
  }

  static class Commands implements CommandMarker {

    @CliCommand(value = { COMMAND1_NAME, COMMAND1_NAME_ALIAS }, help = COMMAND1_HELP)
    public static String command1(
        @CliArgument(name = ARGUMENT1_NAME, argumentContext = ARGUMENT1_CONTEXT, help = ARGUMENT1_HELP, mandatory = true)
        String argument1,
        @CliArgument(name = ARGUEMNT2_NAME, argumentContext = ARGUMENT2_CONTEXT, help = ARGUMENT2_HELP, mandatory = false, unspecifiedDefaultValue = ARGUMENT2_UNSPECIFIED_DEFAULT_VALUE, systemProvided = false)
        String argument2,
        @CliOption(key = { OPTION1_NAME, OPTION1_SYNONYM }, help = OPTION1_HELP, mandatory = true, optionContext = OPTION1_CONTEXT)
        String option1,
        @CliOption(key = { OPTION2_NAME }, help = OPTION2_HELP, mandatory = false, optionContext = OPTION2_CONTEXT, specifiedDefaultValue = OPTION2_SPECIFIED_DEFAULT_VALUE)
        String option2,
        @CliOption(key = { OPTION3_NAME, OPTION3_SYNONYM }, help = OPTION3_HELP, mandatory = false, optionContext = OPTION3_CONTEXT, unspecifiedDefaultValue = OPTION3_UNSPECIFIED_DEFAULT_VALUE)
        String option3) {
      return null;
    }

    @CliCommand(value = { COMMAND2_NAME })
    public static String command2() {
      return null;
    }

    @CliCommand(value = { "testParamConcat" })
    public static Result testParamConcat(
        @CliOption(key = { "string" }) String string,
        @CliOption(key = { "stringArray" }) @CliMetaData(valueSeparator = ",") String[] stringArray,
        @CliOption(key = { "stringList" }, optionContext = ConverterHint.STRING_LIST) @CliMetaData(valueSeparator = ",") List<String> stringList,
        @CliOption(key = { "integer" }) Integer integer,
        @CliOption(key = { "colonArray" }) @CliMetaData(valueSeparator = ":") String[] colonArray) {
      return null;
    }

    @CliCommand(value = { "testMultiWordArg" })
    public static Result testMultiWordArg(
        @CliArgument(name = "arg1" ) String arg1,
        @CliArgument(name = "arg2" ) String arg2) {
      return null;
    }
  }

  static class SimpleConverter implements Converter<String> {

    public boolean supports(Class<?> type, String optionContext) {
      // TODO Auto-generated method stub
      if (type.isAssignableFrom(String.class)) {
        return true;
      }
      return false;
    }

    public String convertFromText(String value, Class<?> targetType,
        String optionContext) {
      // TODO Auto-generated method stub
      return value;
    }

    public boolean getAllPossibleValues(List<Completion> completions,
        Class<?> targetType, String existingData, String context,
        MethodTarget target) {
      // TODO Auto-generated method stub
      if (context.equals(ARGUMENT1_CONTEXT)) {
        for (Completion completion : ARGUMENT1_COMPLETIONS) {
          completions.add(completion);
        }
      } else if (context.equals(ARGUMENT2_CONTEXT)) {
        for (Completion completion : ARGUMENT2_COMPLETIONS) {
          completions.add(completion);
        }
      } else if (context.equals(OPTION1_CONTEXT)) {
        for (Completion completion : OPTION1_COMPLETIONS) {
          completions.add(completion);
        }
      }
      return true;
    }
  }

  public static class AvailabilityCommands implements CommandMarker {
    static final String C1_NAME = "C1";
    static final String C1_PROP = C1_NAME+"-available";
    static final String C1_MSG_UNAVAILABLE = "Requires " + C1_PROP + "=true";
    static final String C1_MSG_AVAILABLE   = C1_NAME + " is available.";

    static final String C2_NAME = "C2";
    static final String C2_PROP = C2_NAME+"-available";
    static final String C2_MSG_UNAVAILABLE = CliStrings.AVAILABILITYTARGET_MSG_DEFAULT_UNAVAILABILITY_DESCRIPTION;
    static final String C2_MSG_AVAILABLE   = C2_NAME + " is available.";

    @CliCommand(value = { C1_NAME })
    public Result command1() {
      return ResultBuilder.createInfoResult(C1_MSG_AVAILABLE);
    }

    @CliCommand(value = { C2_NAME })
    public Result command2() {
      return ResultBuilder.createInfoResult(C2_MSG_AVAILABLE);
    }

    @CliAvailabilityIndicator(C1_NAME)
    @CliMetaData.AvailabilityMetadata(availabilityDescription=C1_MSG_UNAVAILABLE)
    public boolean isCommand1Available() {
      return Boolean.getBoolean(C1_PROP);
    }

    @CliAvailabilityIndicator(C2_NAME)
    public boolean isCommand2Available() {
      return Boolean.getBoolean(C2_PROP);
    }
  }
}
