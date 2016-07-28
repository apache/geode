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
package com.gemstone.gemfire.management.internal.cli.shell;

import static org.junit.Assert.*;

import java.util.List;

import com.gemstone.gemfire.management.cli.CliMetaData;
import com.gemstone.gemfire.management.cli.ConverterHint;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.CommandManager;
import com.gemstone.gemfire.management.internal.cli.GfshParser;
import com.gemstone.gemfire.management.internal.cli.annotation.CliArgument;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.shell.event.ParseResult;

/**
 * GfshExecutionStrategyTest - Includes tests to for GfshExecutionStrategyTest
 */
@Category(UnitTest.class)
public class GfshExecutionStrategyJUnitTest {

  private static final String COMMAND1_NAME = "command1";
  private static final String COMMAND1_NAME_ALIAS = "command1_alias";
  private static final String COMMAND2_NAME = "command2";
  private static final String COMMAND1_SUCESS = "Command1 Executed successfully";
  private static final String COMMAND2_SUCESS = "Command2 Executed successfully";
  private static final String COMMAND1_HELP = "help for " + COMMAND1_NAME;

  @After
  public void tearDown() {
    CommandManager.clearInstance();
  }
  
  /**
   * tests execute method by executing dummy method command1
   */
  @Test
  public void testGfshExecutionStartegyExecute() throws Exception {
    CommandManager commandManager = CommandManager.getInstance();
    assertNotNull("CommandManager should not be null.", commandManager);      
    commandManager.add(Commands.class.newInstance());
    GfshParser parser = new GfshParser(commandManager);
    String[] command1Names = ((CliCommand) Commands.class.getMethod(COMMAND1_NAME).getAnnotation(CliCommand.class)).value();
    String input =command1Names[0];
    ParseResult parseResult = null;   
    parseResult = parser.parse(input);  
    String[] args = new String[] {command1Names[0]  };
    Gfsh gfsh = Gfsh.getInstance(false, args, new GfshConfig());      
    GfshExecutionStrategy gfshExecutionStrategy = new GfshExecutionStrategy(gfsh);
    Result resultObject = (Result)gfshExecutionStrategy.execute(parseResult);     
    String str  = resultObject.nextLine();      
    assertTrue(str.trim().equals(COMMAND1_SUCESS));      
  }
  
  /**
   * tests isReadyForCommnads method by executing dummy method command1.
   * TODO: this method is hard coded in source which may change in future. So this
   * test should also be accordingly changed
   */
  @Test
  public void testGfshExecutionStartegyIsReadyForCommands() throws Exception {
    CommandManager commandManager = CommandManager.getInstance();
    assertNotNull("CommandManager should not be null.", commandManager);
    commandManager.add(Commands.class.newInstance());      
    String[] command1Names = ((CliCommand) Commands.class.getMethod(COMMAND1_NAME).getAnnotation(CliCommand.class)).value();
    String[] args = new String[] {command1Names[0]  };
    Gfsh gfsh = Gfsh.getInstance(false, args, new GfshConfig());      
    GfshExecutionStrategy gfshExecutionStrategy = new GfshExecutionStrategy(gfsh);
    boolean ready = gfshExecutionStrategy.isReadyForCommands();      
    assertTrue(ready);      
  }

  /**
   * represents class for dummy methods
   */
  public static class Commands implements CommandMarker {

    @CliCommand(value = { COMMAND1_NAME, COMMAND1_NAME_ALIAS }, help = COMMAND1_HELP)
    @CliMetaData(shellOnly = true )
    @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
    public static Result command1() {
      return ResultBuilder.createInfoResult(COMMAND1_SUCESS);      
    }

    @CliCommand(value = { COMMAND2_NAME })
    @CliMetaData(shellOnly = false )
    @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
    public static Result command2() {
      return ResultBuilder.createInfoResult(COMMAND2_SUCESS);      
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
  }
}
