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

import static org.junit.Assert.*;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.management.cli.CliMetaData;
import com.gemstone.gemfire.management.cli.ConverterHint;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.annotation.CliArgument;
import com.gemstone.gemfire.management.internal.cli.parser.Argument;
import com.gemstone.gemfire.management.internal.cli.parser.AvailabilityTarget;
import com.gemstone.gemfire.management.internal.cli.parser.CommandTarget;
import com.gemstone.gemfire.management.internal.cli.parser.Option;
import com.gemstone.gemfire.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

/**
 * CommandManagerTest - Includes tests to check the CommandManager functions
 */
@Category(UnitTest.class)
public class CommandManagerJUnitTest {

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
  private static final String ARGUMENT2_NAME = "argument2";
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
  private static final String OPTION1_SPECIFIED_DEFAULT_VALUE = "{specified default value for option1}";
  private static final Completion[] OPTION1_COMPLETIONS = {
      new Completion("option1"), new Completion("option1Alternate") };
  private static final String OPTION2_NAME = "option2";
  private static final String OPTION2_HELP = "help for option2";
  private static final String OPTION2_CONTEXT = "context for option2";
  private static final String OPTION2_SPECIFIED_DEFAULT_VALUE = "{specified default value for option2}";
  private static final String OPTION3_NAME = "option3";
  private static final String OPTION3_SYNONYM = "opt3";
  private static final String OPTION3_HELP = "help for option3";
  private static final String OPTION3_CONTEXT = "context for option3";
  private static final String OPTION3_SPECIFIED_DEFAULT_VALUE = "{specified default value for option3}";
  private static final String OPTION3_UNSPECIFIED_DEFAULT_VALUE = "{unspecified default value for option3}";

  @After
  public void tearDown() {
    CommandManager.clearInstance();
  }
  
  /**
   * tests loadCommands()
   */
  @Test
  public void testCommandManagerLoadCommands() throws Exception {
    CommandManager commandManager = CommandManager.getInstance(true);
    assertNotNull(commandManager);
    assertNotSame(0, commandManager.getCommands().size());
  }

  /**
   * tests commandManagerInstance method
   */
  @Test
  public void testCommandManagerInstance() throws Exception {
    CommandManager commandManager = CommandManager.getInstance(true);
    assertNotNull(commandManager);
  }

  /**
   * tests createOption method for creating option
   */
  @Test
  public void testCommandManagerCreateOption() throws Exception {
    CommandManager commandManager = CommandManager.getInstance(true);
    assertNotNull(commandManager);

    Method method = Commands.class.getMethod(COMMAND1_NAME, String.class,
        String.class, String.class, String.class, String.class);

    Annotation[][] annotations = method.getParameterAnnotations();
    Class<?>[] parameterTypes = method.getParameterTypes();
    List<String> optionNames = new ArrayList<String>();
    optionNames.add(OPTION1_NAME);
    optionNames.add(OPTION2_NAME);
    optionNames.add(OPTION3_NAME);

    int parameterNo = 0;
    for (int i = 0; i < annotations.length; i++) {
      Annotation[] annotation = annotations[i];
      for (Annotation ann : annotation) {
        if (ann instanceof CliOption) {
          Option createdOption = commandManager.createOption((CliOption) ann,
              parameterTypes[i], parameterNo);
          assertTrue(optionNames.contains(createdOption.getLongOption()));          
          assertEquals(((CliOption) ann).help(), createdOption.getHelp());          
          if (((CliOption) ann).specifiedDefaultValue() != null
              && ((CliOption) ann).specifiedDefaultValue().length() > 0) {            
            assertEquals(((CliOption) ann).specifiedDefaultValue().trim(),
                createdOption.getSpecifiedDefaultValue().trim());
          }
          if (((CliOption) ann).unspecifiedDefaultValue() != null
              && ((CliOption) ann).unspecifiedDefaultValue().length() > 0) {           
            assertEquals(((CliOption) ann).specifiedDefaultValue().trim(),
                createdOption.getSpecifiedDefaultValue().trim());
          }

        }
      }
    }
  }

  /**
   * tests createArgument method for creating argument
   */
  @Test
  public void testCommandManagerCreateArgument() throws Exception {
    CommandManager commandManager = CommandManager.getInstance(true);
    assertNotNull(commandManager);

    Method method = Commands.class.getMethod(COMMAND1_NAME, String.class,
        String.class, String.class, String.class, String.class);

    Annotation[][] annotations = method.getParameterAnnotations();
    Class<?>[] parameterTypes = method.getParameterTypes();
    List<String> argumentList = new ArrayList<String>();
    argumentList.add(ARGUMENT1_NAME);
    argumentList.add(ARGUMENT2_NAME);

    int parameterNo = 0;
    for (int i = 0; i < annotations.length; i++) {
      Annotation[] annotation = annotations[i];
      for (Annotation ann : annotation) {
        if (ann instanceof CliArgument) {
          Argument arg = commandManager.createArgument((CliArgument) ann,
              parameterTypes[i], parameterNo);
          assertEquals(true, argumentList.contains(arg.getArgumentName()));
          assertEquals(((CliArgument) ann).mandatory(), arg.isRequired());
          assertEquals(((CliArgument) ann).name().trim(), arg.getArgumentName()
              .trim());
          assertEquals(((CliArgument) ann).argumentContext().trim(), arg
              .getContext().trim());
          assertEquals(((CliArgument) ann).help().trim(), arg.getHelp().trim());
        }
      }
    }
  }

  /**
   * tests availabilityIndicator for a method
   */
  @Test
  public void testCommandManagerAvailabilityIndicator() throws Exception {
    CommandManager commandManager = CommandManager.getInstance(true);
    assertNotNull(commandManager);
    commandManager.add(Commands.class.newInstance());
    Map<String, CommandTarget> commands = commandManager.getCommands();
    for (String commandName : commands.keySet()) {
      if (commandName.equals(COMMAND1_NAME)) {
        CommandTarget commandTarget = commands.get(commandName);
        AvailabilityTarget availabilityIndicator = commandTarget
            .getAvailabilityIndicator();
        if (availabilityIndicator == null) {
          availabilityIndicator = commandManager
              .getAvailabilityIndicator(COMMAND1_NAME);
          commandTarget.setAvailabilityIndicator(availabilityIndicator);
        }
        assertEquals(true, commandTarget.isAvailable());
        break;
      }
    }
  }
  
  /**
   * Tests {@link CommandManager#loadPluginCommands()}.
   * @since GemFire 8.1
   */
  @Test
  public void testCommandManagerLoadPluginCommands() throws Exception {
    CommandManager commandManager  = CommandManager.getInstance(true);
    assertNotNull(commandManager);
    
    // see META-INF/services/org.springframework.shell.core.CommandMarker service loader file.
    assertTrue("Should find listed plugin.", commandManager.getCommands().containsKey("mock plugin command"));
    assertTrue("Should not find unlisted plugin.", !commandManager.getCommands().containsKey("mock plugin command unlisted"));
  }

  /**
   * class that represents dummy commands
   */
  public static class Commands implements CommandMarker {

    @CliCommand(value = { COMMAND1_NAME, COMMAND1_NAME_ALIAS }, help = COMMAND1_HELP)
    @CliMetaData(shellOnly = true, relatedTopic = { "relatedTopicOfCommand1" })
    @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
    public static String command1(
        @CliArgument(name = ARGUMENT1_NAME, argumentContext = ARGUMENT1_CONTEXT, help = ARGUMENT1_HELP, mandatory = true)
        String argument1,
        @CliArgument(name = ARGUMENT2_NAME, argumentContext = ARGUMENT2_CONTEXT, help = ARGUMENT2_HELP, mandatory = false, unspecifiedDefaultValue = ARGUMENT2_UNSPECIFIED_DEFAULT_VALUE, systemProvided = false)
        String argument2,
        @CliOption(key = { OPTION1_NAME, OPTION1_SYNONYM }, help = OPTION1_HELP, mandatory = true, optionContext = OPTION1_CONTEXT, specifiedDefaultValue = OPTION1_SPECIFIED_DEFAULT_VALUE)
        String option1,
        @CliOption(key = { OPTION2_NAME }, help = OPTION2_HELP, mandatory = false, optionContext = OPTION2_CONTEXT, specifiedDefaultValue = OPTION2_SPECIFIED_DEFAULT_VALUE)
        String option2,
        @CliOption(key = { OPTION3_NAME, OPTION3_SYNONYM }, help = OPTION3_HELP, mandatory = false, optionContext = OPTION3_CONTEXT, unspecifiedDefaultValue = OPTION3_UNSPECIFIED_DEFAULT_VALUE, specifiedDefaultValue = OPTION3_SPECIFIED_DEFAULT_VALUE)
        String option3) {
      return null;
    }

    @CliCommand(value = { COMMAND2_NAME })
    @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
    public static String command2() {
      return null;
    }

    @CliCommand(value = { "testParamConcat" })
    @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
    public static Result testParamConcat(
        @CliOption(key = { "string" })
        String string,
        @CliOption(key = { "stringArray" })
        @CliMetaData(valueSeparator = ",")
        String[] stringArray,
        @CliOption(key = { "stringList" }, optionContext = ConverterHint.STRING_LIST)
        @CliMetaData(valueSeparator = ",")
        List<String> stringList, @CliOption(key = { "integer" })
        Integer integer, @CliOption(key = { "colonArray" })
        @CliMetaData(valueSeparator = ":")
        String[] colonArray) {
      return null;
    }

    @CliCommand(value = { "testMultiWordArg" })
    @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
    public static Result testMultiWordArg(@CliArgument(name = "arg1")
    String arg1, @CliArgument(name = "arg2")
    String arg2) {
      return null;
    }

    @CliAvailabilityIndicator({ COMMAND1_NAME })
    public boolean isAvailable() {
      return true; // always available on server
    }
  }

  /**
   * Used by testCommandManagerLoadPluginCommands
   */
  private static class SimpleConverter implements Converter<String> {

    @Override
    public boolean supports(Class<?> type, String optionContext) {
      if (type.isAssignableFrom(String.class)) {
        return true;
      }
      return false;
    }

    @Override
    public String convertFromText(String value, Class<?> targetType,
        String optionContext) {
      return value;
    }

    @Override
    public boolean getAllPossibleValues(List<Completion> completions,
        Class<?> targetType, String existingData, String context,
        MethodTarget target) {
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

  public static class MockPluginCommand implements CommandMarker {
    @CliCommand(value = "mock plugin command")
    @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
    public Result mockPluginCommand() {
      return null;
    }
  }

  public static class MockPluginCommandUnlisted implements CommandMarker {
    @CliCommand(value = "mock plugin command unlisted")
    @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
    public Result mockPluginCommandUnlisted() {
      return null;
    }
  }

}
