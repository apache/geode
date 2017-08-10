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

package org.apache.geode.test.dunit.rules;

import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.rules.ExternalResource;
import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.event.ParseResult;
import org.springframework.util.ReflectionUtils;

import org.apache.geode.management.internal.cli.CommandManager;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.result.CommandResult;

public class GfshParserRule extends ExternalResource {

  private GfshParser parser;
  private CommandManager commandManager;

  @Override
  public void before() {
    commandManager = new CommandManager();
    parser = new GfshParser(commandManager);
  }

  public GfshParseResult parse(String command) {
    return parser.parse(command);
  }

  public <T> CommandResult executeCommandWithInstance(T instance, String command) {
    ParseResult parseResult = parse(command);
    return (CommandResult) ReflectionUtils.invokeMethod(parseResult.getMethod(), instance,
        parseResult.getArguments());
  }

  public CommandCandidate complete(String command) {
    List<Completion> candidates = new ArrayList<>();
    int cursor = parser.completeAdvanced(command, command.length(), candidates);
    return new CommandCandidate(command, cursor, candidates);
  }

  public <T extends Converter> T spyConverter(Class<T> klass) {
    Set<Converter<?>> converters = parser.getConverters();
    T foundConverter = null, spy = null;
    for (Converter converter : converters) {
      if (klass.isAssignableFrom(converter.getClass())) {
        foundConverter = (T) converter;
        break;
      }
    }
    if (foundConverter != null) {
      parser.remove(foundConverter);
      spy = spy(foundConverter);
      parser.add(spy);
    }
    return spy;
  }

  public GfshParser getParser() {
    return parser;
  }

  public CommandManager getCommandManager() {
    return commandManager;
  }

  public static class CommandCandidate {
    private String command;
    private int cursor;
    private List<Completion> candidates;

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
