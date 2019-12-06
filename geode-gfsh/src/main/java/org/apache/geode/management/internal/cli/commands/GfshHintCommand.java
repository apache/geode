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

package org.apache.geode.management.internal.cli.commands;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.internal.cli.CommandManager;
import org.apache.geode.management.internal.cli.CommandManagerAware;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;

public class GfshHintCommand extends OfflineGfshCommand implements CommandManagerAware {
  private CommandManager commandManager = null;

  @Override
  public void setCommandManager(CommandManager commandManager) {
    this.commandManager = commandManager;
  }

  @CliCommand(value = CliStrings.HINT, help = CliStrings.HINT__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GEODE_HELP})
  public ResultModel hint(@CliOption(key = {"", CliStrings.HINT__TOPICNAME},
      optionContext = ConverterHint.HINT,
      help = CliStrings.HINT__TOPICNAME__HELP) String topicName) {
    return ResultModel.createInfo(commandManager.obtainHint(topicName));
  }
}
