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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.gemstone.gemfire.management.internal.cli.exceptions.CliCommandOptionMissingException;
import com.gemstone.gemfire.management.internal.cli.exceptions.CliCommandOptionNotApplicableException;
import com.gemstone.gemfire.management.internal.cli.parser.Argument;
import com.gemstone.gemfire.management.internal.cli.parser.Option;
import com.gemstone.gemfire.management.internal.cli.parser.OptionSet;
import com.gemstone.gemfire.management.internal.cli.parser.jopt.JoptOptionParser;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
@RunWith(JUnitParamsRunner.class)
public class JoptOptionParserTest {

  private JoptOptionParser emptyOptionParser;
  private OptionSet emptyOptionSet;

  private Argument requiredArgument;
  private Argument optionalArgument;

  private Option requiredOption;
  private Option optionalOption;

  private JoptOptionParser simpleOptionParser;
  private JoptOptionParser exampleOptionParser;

  @Before
  public void setUp() throws Exception {
    this.emptyOptionParser = new JoptOptionParser();
    this.emptyOptionSet = new OptionSet();
    defineSimpleOptionParser();
    defineExampleOptionParser();
  }

  @Test
  public void getArgumentsIsEmptyByDefault() throws Exception {
    assertThat(this.emptyOptionParser.getArguments()).isEmpty();
  }

  @Test
  public void getOptionsIsNullByDefault() throws Exception {
    assertThat(this.emptyOptionParser.getOptions()).isNull();
  }

  @Test
  public void parseNullReturnsDefaultOptionSet() throws Exception {
    OptionSet optionSet = this.emptyOptionParser.parse(null);
    assertThat(optionSet.areArgumentsPresent()).isEqualTo(emptyOptionSet.areArgumentsPresent());
    assertThat(optionSet.areOptionsPresent()).isEqualTo(emptyOptionSet.areOptionsPresent());
    assertThat(optionSet.getNoOfSpacesRemoved()).isEqualTo(emptyOptionSet.getNoOfSpacesRemoved());
    assertThat(optionSet.getSplit()).isEqualTo(emptyOptionSet.getSplit());
    assertThat(optionSet.getNoOfSpacesRemoved()).isEqualTo(emptyOptionSet.getNoOfSpacesRemoved());
    assertThat(optionSet.getUserInput()).isEqualTo(""); //emptyOptionSet.getUserInput());
    assertThat(optionSet.getValue((Argument)null)).isEqualTo(emptyOptionSet.getValue((Argument)null));
    assertThat(optionSet.getValue((Option)null)).isEqualTo(emptyOptionSet.getValue((Option)null));
  }

  @Test
  public void parseEmptyThrowsNullPointerException() throws Exception {
    assertThatThrownBy(() -> this.emptyOptionParser.parse("")).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void setArgumentsShouldCreateCopy() throws Exception {
    Argument argument = mock(Argument.class);
    when(argument.isRequired()).thenReturn(true);

    LinkedList<Argument> arguments = new LinkedList<>();
    arguments.add(argument);

    this.emptyOptionParser.setArguments(arguments);

    assertThat(this.emptyOptionParser.getArguments()).isNotSameAs(arguments);
    assertThat(this.emptyOptionParser.getArguments()).hasSize(1);

    arguments.clear();

    assertThat(arguments).hasSize(0);
    assertThat(this.emptyOptionParser.getArguments()).hasSize(1);
  }

  @Test
  public void setArgumentsShouldKeepRequiredBeforeOptional() throws Exception {
    Argument requiredArgument1 = mock(Argument.class);
    when(requiredArgument1.isRequired()).thenReturn(true);
    Argument optionalArgument1 = mock(Argument.class);
    when(optionalArgument1.isRequired()).thenReturn(false);

    LinkedList<Argument> arguments = new LinkedList<>();
    arguments.add(requiredArgument1);
    arguments.add(optionalArgument1);

    this.emptyOptionParser.setArguments(arguments);

    LinkedList<Argument> argumentsReturned = this.emptyOptionParser.getArguments();

    assertThat(argumentsReturned).hasSize(2);
    assertThat(argumentsReturned.getFirst()).isSameAs(requiredArgument1);
    assertThat(argumentsReturned.getLast()).isSameAs(optionalArgument1);
  }

  @Test
  public void setArgumentsShouldMoveRequiredBeforeOptional() throws Exception {
    Argument requiredArgument1 = mock(Argument.class);
    when(requiredArgument1.isRequired()).thenReturn(true);
    Argument optionalArgument1 = mock(Argument.class);
    when(optionalArgument1.isRequired()).thenReturn(false);

    LinkedList<Argument> arguments = new LinkedList<>();
    arguments.add(optionalArgument1);
    arguments.add(requiredArgument1);

    this.emptyOptionParser.setArguments(arguments);

    LinkedList<Argument> argumentsReturned = this.emptyOptionParser.getArguments();

    assertThat(argumentsReturned).hasSize(2);
    assertThat(argumentsReturned.getFirst()).isSameAs(requiredArgument1);
    assertThat(argumentsReturned.getLast()).isSameAs(optionalArgument1);
  }

  @Test
  public void setOptionsShouldKeepSameInstance() throws Exception {
    Option option = mock(Option.class);
    ArrayList aggregate = new ArrayList<String>();
    aggregate.add("option");
    when(option.getAggregate()).thenReturn(aggregate);
    when(option.getHelp()).thenReturn("help text");

    LinkedList<Option> options = new LinkedList<>();
    options.add(option);

    this.emptyOptionParser.setOptions(options);

    assertThat(this.emptyOptionParser.getOptions()).isSameAs(options);
    assertThat(this.emptyOptionParser.getOptions()).hasSize(1);

    options.clear();

    assertThat(options).hasSize(0);
    assertThat(this.emptyOptionParser.getOptions()).hasSize(0);
  }

  @Test
  public void parseInputWithDefinedArgumentShouldWork() throws Exception {
    LinkedList<Argument> arguments = new LinkedList<>();
    LinkedList<Option> options = new LinkedList<>();

    arguments.add(this.requiredArgument);

    JoptOptionParser optionParser = new JoptOptionParser();
    optionParser.setArguments(arguments);
    optionParser.setOptions(options);

    OptionSet optionSet = optionParser.parse("command1 argument1_value");
    assertThat(optionSet.areArgumentsPresent()).isTrue();
    assertThat(optionSet.hasArgument(this.requiredArgument)).isTrue();
  }

  @Test
  public void parseInputWithOneArgumentShouldFindJustOneArgument() throws Exception {
    LinkedList<Argument> arguments = new LinkedList<>();
    LinkedList<Option> options = new LinkedList<>();

    arguments.add(this.requiredArgument);

    JoptOptionParser optionParser = new JoptOptionParser();
    optionParser.setArguments(arguments);
    optionParser.setOptions(options);

    OptionSet optionSet = optionParser.parse("command1 argument1_value");
    assertThat(optionSet.areArgumentsPresent()).isTrue();
    assertThat(optionSet.hasArgument(this.requiredArgument)).isTrue();
    assertThat(optionSet.hasArgument(this.optionalArgument)).isFalse();
  }

  @Test
  public void parseInputWithTwoArgumentsShouldFindTwoArguments() throws Exception {
    LinkedList<Argument> arguments = new LinkedList<>();
    LinkedList<Option> options = new LinkedList<>();

    arguments.add(this.requiredArgument);
    arguments.add(this.optionalArgument);

    JoptOptionParser optionParser = new JoptOptionParser();
    optionParser.setArguments(arguments);
    optionParser.setOptions(options);

    OptionSet optionSet = optionParser.parse("command1 argument1_value? argument2_value");
    assertThat(optionSet.areArgumentsPresent()).isTrue();
    assertThat(optionSet.hasArgument(this.requiredArgument)).isTrue();
    assertThat(optionSet.hasArgument(this.optionalArgument)).isTrue();
  }

  @Test
  public void parseInputWithUndefinedArgumentShouldThrow() throws Exception {
    LinkedList<Argument> arguments = new LinkedList<>();
    LinkedList<Option> options = new LinkedList<>();

    arguments.add(this.requiredArgument);

    JoptOptionParser optionParser = new JoptOptionParser();
    optionParser.setArguments(arguments);
    optionParser.setOptions(options);

    assertThatThrownBy(() -> optionParser.parse("command1 argument1_value? argument2_value")).isOfAnyClassIn(CliCommandOptionNotApplicableException.class);
  }

  @Test
  public void parseInputWithUndefinedOptionShouldThrow() throws Exception {
    assertThatThrownBy(() -> this.simpleOptionParser.parse("command1 argument1_value argument2_value --undefinedOption")).isExactlyInstanceOf(CliCommandOptionNotApplicableException.class);
  }

  @Test
  public void parseInputWithOneOptionShouldFindOneOption() throws Exception {
    OptionSet optionSet = this.simpleOptionParser.parse("command1 argument1_value --option1");
    assertThat(optionSet.areOptionsPresent()).isTrue();
    assertThat(optionSet.hasOption(this.requiredOption)).isTrue();
    assertThat(optionSet.hasOption(this.optionalOption)).isFalse();
  }

  @Test
  public void parseInputWithTwoOptionsShouldFindTwoOptions() throws Exception {
    OptionSet optionSet = this.simpleOptionParser.parse("command1 argument1_value --option1 --option2");
    assertThat(optionSet.areOptionsPresent()).isTrue();
    assertThat(optionSet.hasOption(this.requiredOption)).isTrue();
    assertThat(optionSet.hasOption(this.optionalOption)).isTrue();
  }

  @Test
  public void parseInputWithOptionWithValueShouldFindOption() throws Exception {
    OptionSet optionSet = this.simpleOptionParser.parse("command1 argument1_value --option1=value");
    assertThat(optionSet.areOptionsPresent()).isTrue();
    assertThat(optionSet.hasOption(this.requiredOption)).isTrue();
  }

  @Test
  public void parseInputWithOptionWithoutValueShouldFindOption() throws Exception {
    OptionSet optionSet = this.simpleOptionParser.parse("command1 argument1_value --option1");
    assertThat(optionSet.areOptionsPresent()).isTrue();
    assertThat(optionSet.hasOption(this.requiredOption)).isTrue();
  }

  @Test
  public void parseInputWithoutOptionShouldNotFindOptions() throws Exception {
    LinkedList<Argument> arguments = new LinkedList<>();
    LinkedList<Option> options = new LinkedList<>();

    arguments.add(this.requiredArgument);

    JoptOptionParser optionParser = new JoptOptionParser();
    optionParser.setArguments(arguments);
    optionParser.setOptions(options);

    OptionSet optionSet = optionParser.parse("command1 argument1_value");
    assertThat(optionSet.areOptionsPresent()).isFalse();
    assertThat(optionSet.hasOption(this.requiredOption)).isFalse();
  }

  @Test
  @Parameters(method = "exampleInputParameters")
  public void parseInputWithExampleInputParametesr(String command, boolean expectException, boolean hasArguments, boolean hasOptions) throws Exception {
    if (expectException) {
      assertThatThrownBy(() -> this.exampleOptionParser.parse(command)).isExactlyInstanceOf(CliCommandOptionMissingException.class);
      return;
    }

    OptionSet options = this.exampleOptionParser.parse(command);
    assertThat(options).isNotNull();
    assertThat(options.areArgumentsPresent()).isEqualTo(hasArguments);
    assertThat(options.areOptionsPresent()).isEqualTo(hasOptions);
  }

  private static Object[] exampleInputParameters() {
    return new Object[]{
      // 0
      new Object[] { " ARGUMENT1_VALUE —option1=somevalue", false, true, false },
      // 1
      new Object[] { " ARGUMENT1_VALUE?      ARGUMENT2_VALUE -- ----------", false, true, false },
      // 2
      new Object[] { " --option1=value", false, false, true },
      // 3
      new Object[] { "         ARGUMENT1_VALUE?       ARGUMENT2_VALUE --option1=option1value --option2", false, true, true },
      // 4
      new Object[] { "         ARGUMENT1_VALUE?       ARGUMENT2_VALUE --option1=option1value --option2=option2value --option3=option3value", false, true, true },
      // 5
      new Object[] { " --string=string1 --stringArray=1,2 --stringArray=3,4 --stringList=11,12,13 --integer=10 --stringArray=5 --stringList=14,15", false, false, true },
      // 6
      new Object[] { " --stringArray=1,2 --stringArray='3,4'", false, false, true },
      // 7
      new Object[] { " --string=\"1\" --colonArray=2:3:4 --stringArray=5,\"6,7\",8 --stringList=\"9,10,11,12\"", false, false, true },
      // 8
      new Object[] { " --string=string1 --stringArray=1,2 --string=string2", false, false, true },
      // 9
      new Object[] { " this is just one argument?this is a second argument", false, true, false }
    };
  }

  private void defineSimpleOptionParser() {
    LinkedList<Argument> arguments = new LinkedList<Argument>();
    LinkedList<Option> options = new LinkedList<Option>();

    this.requiredArgument = mock(Argument.class);
    when(this.requiredArgument.getArgumentName()).thenReturn("argument1");
    when(this.requiredArgument.getContext()).thenReturn("context for argument1");
    when(this.requiredArgument.getHelp()).thenReturn("help for argument1");
    when(this.requiredArgument.isRequired()).thenReturn(true);
    arguments.add(this.requiredArgument);

    this.optionalArgument = mock(Argument.class);
    when(this.optionalArgument.getArgumentName()).thenReturn("argument2");
    when(this.optionalArgument.getContext()).thenReturn("context for argument2");
    when(this.optionalArgument.getHelp()).thenReturn("help for argument2");
    when(this.optionalArgument.isRequired()).thenReturn(false);
    when(this.optionalArgument.getUnspecifiedDefaultValue()).thenReturn("{unspecified default value for argument2}");
    when(this.optionalArgument.isSystemProvided()).thenReturn(false);
    arguments.add(this.optionalArgument);

    this.requiredOption = mock(Option.class);
    when(this.requiredOption.getLongOption()).thenReturn("--option1");
    List<String> aggregate = new ArrayList<>();
    aggregate.add("option1");
    when(this.requiredOption.getAggregate()).thenReturn(aggregate);
    when(this.requiredOption.getLongOption()).thenReturn("option1");
    when(this.requiredOption.getHelp()).thenReturn("help for option1");
    when(this.requiredOption.getValueSeparator()).thenReturn("=");
    when(this.requiredOption.isRequired()).thenReturn(true);
    assertThat(this.requiredOption.getAggregate()).isNotEmpty();
    options.add(this.requiredOption);

    this.optionalOption = mock(Option.class);
    when(this.optionalOption.getLongOption()).thenReturn("--option2");
    aggregate = new ArrayList<>();
    aggregate.add("option2");
    when(this.optionalOption.getAggregate()).thenReturn(aggregate);
    when(this.optionalOption.getLongOption()).thenReturn("option2");
    when(this.optionalOption.getHelp()).thenReturn("help for option2");
    when(this.optionalOption.getValueSeparator()).thenReturn("=");
    when(this.optionalOption.isRequired()).thenReturn(false);
    assertThat(this.optionalOption.getAggregate()).isNotEmpty();
    options.add(this.optionalOption);

    this.simpleOptionParser = new JoptOptionParser();
    this.simpleOptionParser.setArguments(arguments);
    this.simpleOptionParser.setOptions(options);
  }

  private void defineExampleOptionParser() {
    LinkedList<Argument> arguments = new LinkedList<Argument>();
    LinkedList<Option> options = new LinkedList<Option>();

    Argument argument1 = mock(Argument.class);
    when(argument1.getArgumentName()).thenReturn("argument1");
    when(argument1.getContext()).thenReturn("context for argument1");
    when(argument1.getHelp()).thenReturn("help for argument1");
    when(argument1.isRequired()).thenReturn(true);
    arguments.add(argument1);

    Argument argument2 = mock(Argument.class);
    when(argument2.getArgumentName()).thenReturn("argument2");
    when(argument2.getContext()).thenReturn("context for argument2");
    when(argument2.getHelp()).thenReturn("help for argument2");
    when(argument2.isRequired()).thenReturn(false);
    when(argument2.getUnspecifiedDefaultValue()).thenReturn("{unspecified default value for argument2}");
    when(argument2.isSystemProvided()).thenReturn(false);
    arguments.add(argument2);

    Argument argument3 = mock(Argument.class);
    when(argument3.getArgumentName()).thenReturn("argument3");
    when(argument3.getContext()).thenReturn("context for argument3");
    when(argument3.getHelp()).thenReturn("help for argument3");
    when(argument3.isRequired()).thenReturn(false);
    when(argument3.getUnspecifiedDefaultValue()).thenReturn("{unspecified default value for argument3}");
    when(argument2.isSystemProvided()).thenReturn(false);
    arguments.add(argument3);

    Option option1 = mock(Option.class);
    when(option1.getLongOption()).thenReturn("--option1");
    List<String> aggregate1 = new ArrayList<>();
    aggregate1.add("option1");
    when(option1.getAggregate()).thenReturn(aggregate1);
    when(option1.getLongOption()).thenReturn("option1");
    when(option1.getHelp()).thenReturn("help for option1");
    when(option1.getValueSeparator()).thenReturn("=");
    when(option1.isRequired()).thenReturn(false);
    assertThat(option1.getAggregate()).isNotEmpty();
    options.add(option1);

    Option option2 = mock(Option.class);
    when(option2.getLongOption()).thenReturn("--option2");
    List<String> aggregate2 = new ArrayList<>();
    aggregate2.add("option2");
    when(option2.getAggregate()).thenReturn(aggregate2);
    when(option2.getLongOption()).thenReturn("option2");
    when(option2.getHelp()).thenReturn("help for option2");
    when(option2.getValueSeparator()).thenReturn("=");
    when(option2.isRequired()).thenReturn(false);
    assertThat(option2.getAggregate()).isNotEmpty();
    options.add(option2);

    Option option3 = mock(Option.class);
    when(option3.getLongOption()).thenReturn("--option3");
    List<String> aggregate3 = new ArrayList<>();
    aggregate3.add("option3");
    when(option3.getAggregate()).thenReturn(aggregate3);
    when(option3.getLongOption()).thenReturn("option3");
    when(option3.getHelp()).thenReturn("help for option3");
    when(option3.getValueSeparator()).thenReturn("=");
    when(option3.isRequired()).thenReturn(false);
    assertThat(option3.getAggregate()).isNotEmpty();
    options.add(option3);

    Option stringOption = mock(Option.class);
    when(stringOption.getLongOption()).thenReturn("--string");
    List<String> aggregateStringOption = new ArrayList<>();
    aggregateStringOption.add("string");
    when(stringOption.getAggregate()).thenReturn(aggregateStringOption);
    when(stringOption.getLongOption()).thenReturn("string");
    when(stringOption.getHelp()).thenReturn("help for string");
    when(stringOption.getValueSeparator()).thenReturn("=");
    when(stringOption.isRequired()).thenReturn(false);
    assertThat(stringOption.getAggregate()).isNotEmpty();
    options.add(stringOption);

    Option stringArrayOption = mock(Option.class);
    when(stringArrayOption.getLongOption()).thenReturn("--stringArray");
    List<String> aggregateStringArrayOption = new ArrayList<>();
    aggregateStringArrayOption.add("stringArray");
    when(stringArrayOption.getAggregate()).thenReturn(aggregateStringArrayOption);
    when(stringArrayOption.getLongOption()).thenReturn("stringArray");
    when(stringArrayOption.getHelp()).thenReturn("help for stringArray");
    when(stringArrayOption.getValueSeparator()).thenReturn("=");
    when(stringArrayOption.isRequired()).thenReturn(false);
    assertThat(stringArrayOption.getAggregate()).isNotEmpty();
    options.add(stringArrayOption);

    Option stringListOption = mock(Option.class);
    when(stringListOption.getLongOption()).thenReturn("--stringList");
    List<String> aggregateStringListOption = new ArrayList<>();
    aggregateStringListOption.add("stringList");
    when(stringListOption.getAggregate()).thenReturn(aggregateStringListOption);
    when(stringListOption.getLongOption()).thenReturn("stringList");
    when(stringListOption.getHelp()).thenReturn("help for stringList");
    when(stringListOption.getValueSeparator()).thenReturn("=");
    when(stringListOption.isRequired()).thenReturn(false);
    assertThat(stringListOption.getAggregate()).isNotEmpty();
    options.add(stringListOption);

    Option integerOption = mock(Option.class);
    when(integerOption.getLongOption()).thenReturn("--integer");
    List<String> aggregateIntegerOption = new ArrayList<>();
    aggregateIntegerOption.add("integer");
    when(integerOption.getAggregate()).thenReturn(aggregateIntegerOption);
    when(integerOption.getLongOption()).thenReturn("integer");
    when(integerOption.getHelp()).thenReturn("help for integer");
    when(integerOption.getValueSeparator()).thenReturn("=");
    when(integerOption.isRequired()).thenReturn(false);
    assertThat(integerOption.getAggregate()).isNotEmpty();
    options.add(integerOption);

    Option colonArrayOption = mock(Option.class);
    when(colonArrayOption.getLongOption()).thenReturn("--colonArray");
    List<String> aggregateColonArrayOption = new ArrayList<>();
    aggregateColonArrayOption.add("colonArray");
    when(colonArrayOption.getAggregate()).thenReturn(aggregateColonArrayOption);
    when(colonArrayOption.getLongOption()).thenReturn("colonArray");
    when(colonArrayOption.getHelp()).thenReturn("help for colonArray");
    when(colonArrayOption.getValueSeparator()).thenReturn("=");
    when(colonArrayOption.isRequired()).thenReturn(false);
    assertThat(colonArrayOption.getAggregate()).isNotEmpty();
    options.add(colonArrayOption);

    this.exampleOptionParser = new JoptOptionParser();
    this.exampleOptionParser.setArguments(arguments);
    this.exampleOptionParser.setOptions(options);
  }
}
