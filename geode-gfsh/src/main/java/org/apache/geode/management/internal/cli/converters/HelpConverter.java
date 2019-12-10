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
package org.apache.geode.management.internal.cli.converters;

import java.util.List;
import java.util.Set;

import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;

import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.internal.cli.CommandManager;
import org.apache.geode.management.internal.cli.CommandManagerAware;
import org.apache.geode.management.internal.cli.commands.GfshHelpCommand;

/**
 * {@link Converter} for {@link GfshHelpCommand#obtainHelp(String)}
 *
 *
 * @since GemFire 7.0
 */
public class HelpConverter implements Converter<String>, CommandManagerAware {

  private CommandManager commandManager;

  @Override
  public String convertFromText(String existingData, Class<?> dataType, String optionContext) {
    return existingData;
  }

  @Override
  public boolean getAllPossibleValues(List<Completion> completionCandidates, Class<?> dataType,
      String existingData, String optionContext, MethodTarget arg4) {
    Set<String> commandNames = commandManager.getHelper().getCommands();
    for (String string : commandNames) {
      completionCandidates.add(new Completion(string));
    }

    return completionCandidates.size() > 0;
  }

  @Override
  public boolean supports(Class<?> arg0, String optionContext) {
    return String.class.isAssignableFrom(arg0) && optionContext.contains(ConverterHint.HELP);
  }

  @Override
  public void setCommandManager(CommandManager commandManager) {
    this.commandManager = commandManager;
  }
}
