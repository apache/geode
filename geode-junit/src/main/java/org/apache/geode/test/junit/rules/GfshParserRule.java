/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.test.junit.rules;


import java.util.ArrayList;
import java.util.List;

import org.junit.rules.ExternalResource;

import org.apache.geode.internal.classloader.ClassPathLoader;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliAroundInterceptor;
import org.apache.geode.management.internal.cli.CommandManager;
import org.apache.geode.management.internal.cli.Completion;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.remote.CommandExecutor;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.test.junit.assertions.CommandResultAssert;

public class GfshParserRule extends ExternalResource {

  private GfshParser parser;
  private CommandManager commandManager;
  private CommandExecutor commandExecutor;

  @Override
  public void before() {
    commandManager = new CommandManager();
    parser = new GfshParser(commandManager);
    // GfshParserRule doesn't need dlock service
    commandExecutor = new CommandExecutor(null);
  }

  public GfshParseResult parse(String command) {
    try {
      return parser.parse(command);
    } catch (IllegalArgumentException e) {
      // In Spring Shell 3.x, parsing errors throw IllegalArgumentException
      // Return null to maintain compatibility with existing error handling
      return null;
    }
  }

  /**
   * @param <T> the type of command class
   * @param instance an instance of the command class
   * @param command the command to execute
   * @return a {@link CommandResult}
   * @deprecated use {@link #executeAndAssertThat(Object, String)} instead
   */
  @Deprecated
  public <T> CommandResult executeCommandWithInstance(T instance, String command) {
    // Register the command instance BEFORE parsing
    // This allows the parser to find commands from external modules (e.g., geode-connectors)
    commandManager.add(instance);

    GfshParseResult parseResult = parse(command);

    if (parseResult == null) {
      // Return error message format matching CommandExecutor behavior
      return new CommandResult(ResultModel.createError(
          "Error while processing command <" + command + "> Reason : Invalid command syntax"));
    }

    CliAroundInterceptor interceptor = null;
    CliMetaData cliMetaData = parseResult.getMethod().getAnnotation(CliMetaData.class);

    if (cliMetaData != null) {
      String interceptorClass = cliMetaData.interceptor();
      if (!CliMetaData.ANNOTATION_NULL_VALUE.equals(interceptorClass)) {
        try {
          interceptor = (CliAroundInterceptor) ClassPathLoader.getLatest().forName(interceptorClass)
              .newInstance();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }

        ResultModel preExecResult = interceptor.preExecution(parseResult);
        if (preExecResult instanceof ResultModel) {
          if (preExecResult.getStatus() != Result.Status.OK) {
            return new CommandResult(preExecResult);
          }
        } else {
          if (Result.Status.ERROR.equals(((Result) preExecResult).getStatus())) {
            return new CommandResult(preExecResult);
          }
        }
      }
    }

    Object exeResult = commandExecutor.execute(instance, parseResult);
    return new CommandResult((ResultModel) exeResult);
  }

  public <T> CommandResultAssert executeAndAssertThat(T instance, String command) {
    CommandResult result = executeCommandWithInstance(instance, command);
    System.out.println("Command Result:");
    System.out.println(result.asString());
    return new CommandResultAssert(result);
  }

  /**
   * Spring Shell 3.x: Command completion support.
   * Provides tab completion support using GfshParser.completeAdvanced().
   */
  public CommandCandidate complete(String command) {
    List<Completion> candidates = new ArrayList<>();
    int cursor = parser.completeAdvanced(command, command.length(), candidates);
    return new CommandCandidate(command, cursor, candidates);
  }

  /**
   * Spring Shell 3.x Migration Note: The spyConverter() method was removed.
   * Spring Shell 3.x removed the Converter interface that this method depended on.
   * Tests that used spyConverter() need to be updated to use Spring's built-in
   * conversion mechanism or custom parameter resolvers.
   */

  public GfshParser getParser() {
    return parser;
  }

  public CommandManager getCommandManager() {
    return commandManager;
  }

  public static class CommandCandidate {
    private final String command;
    private final int cursor;
    private final List<Completion> candidates;

    public CommandCandidate(String command, int cursor, List<Completion> candidates) {
      this.command = command;
      this.cursor = cursor;
      this.candidates = candidates;
    }

    public String getCandidate(int index) {
      return command.substring(0, cursor) + candidates.get(index).getValue();
    }

    public String getFirstCandidate() {
      return getCandidate(0);
    }

    public int size() {
      return candidates.size();
    }

    public int getCursor() {
      return cursor;
    }

    public List<Completion> getCandidates() {
      return candidates;
    }
  }
}
