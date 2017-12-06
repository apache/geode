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
import org.apache.geode.management.internal.cli.help.Helper;

/**
 *
 * @since GemFire 7.0
 */
public class HintTopicConverter implements Converter<String>, CommandManagerAware {

  private CommandManager commandManager;

  @Override
  public boolean supports(Class<?> type, String optionContext) {
    return String.class.equals(type) && optionContext.contains(ConverterHint.HINT);
  }

  @Override
  public String convertFromText(String value, Class<?> targetType, String optionContext) {
    return value;
  }

  @Override
  public boolean getAllPossibleValues(List<Completion> completions, Class<?> targetType,
      String existingData, String optionContext, MethodTarget target) {
    Helper helper = commandManager.getHelper();
    Set<String> topicNames = helper.getTopicNames();

    for (String topicName : topicNames) {
      if (existingData != null && !existingData.isEmpty()) {
        if (topicName.startsWith(existingData)) { // match exact case
          completions.add(new Completion(topicName));
        } else if (topicName.toLowerCase().startsWith(existingData.toLowerCase())) { // match
          // case
          // insensitive
          String completionStr = existingData + topicName.substring(existingData.length());

          completions.add(new Completion(completionStr));
        }
      } else {
        completions.add(new Completion(topicName));
      }
    }

    return !completions.isEmpty();
  }

  @Override
  public void setCommandManager(CommandManager commandManager) {
    this.commandManager = commandManager;
  }
}
