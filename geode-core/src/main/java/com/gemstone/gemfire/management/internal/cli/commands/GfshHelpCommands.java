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
package com.gemstone.gemfire.management.internal.cli.commands;

import com.gemstone.gemfire.management.cli.CliMetaData;
import com.gemstone.gemfire.management.cli.ConverterHint;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.CommandManager;
import com.gemstone.gemfire.management.internal.cli.GfshParser;
import com.gemstone.gemfire.management.internal.cli.annotation.CliArgument;
import com.gemstone.gemfire.management.internal.cli.help.CliTopic;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CompositeResultData;
import com.gemstone.gemfire.management.internal.cli.result.CompositeResultData.SectionResultData;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * 
 * 
 * @since GemFire 7.0
 */
public class GfshHelpCommands implements CommandMarker{

  private Gfsh getGfsh() {
    return Gfsh.getCurrentInstance();
  }
  
  @CliCommand(value = CliStrings.HELP, help = CliStrings.HELP__HELP)
  @CliMetaData(shellOnly=true, relatedTopic = {CliStrings.TOPIC_GEODE_HELP })
  public Result obtainHelp(
      @CliArgument(name = CliStrings.HELP__COMMAND, 
                 argumentContext = CliStrings.PARAM_CONTEXT_HELP, 
                 help = CliStrings.HELP__COMMAND__HELP) 
                 String commandString) {
    return ResultBuilder.createInfoResult(getGfsh().obtainHelp(commandString, null));
  }
  
  
  
  @CliCommand(value = CliStrings.HINT, help = CliStrings.HINT__HELP)
  @CliMetaData(shellOnly=true, relatedTopic = {CliStrings.TOPIC_GEODE_HELP })
  public Result hint(
      @CliArgument(name = CliStrings.HINT__TOPICNAME, 
                argumentContext = ConverterHint.HINTTOPIC, 
                help = CliStrings.HINT__TOPICNAME) 
                String topicName) {
    Result result = null;
    CommandManager commandManager = CommandManager.getExisting();
    if (commandManager == null) {
      result= ResultBuilder.createShellClientErrorResult(CliStrings.HINT__MSG__SHELL_NOT_INITIALIZED);
    } else { 
      StringBuilder builder = new StringBuilder();
      if (topicName == null) {
        builder.append(CliStrings.HINT__MSG__TOPICS_AVAILABLE).append(GfshParser.LINE_SEPARATOR);
        Set<String> topicNameSet = commandManager.getTopicNames();
        for (String topic : topicNameSet) {
          builder.append(topic).append(GfshParser.LINE_SEPARATOR);
        }
        result = ResultBuilder.createInfoResult(builder.toString());
      } else {
        CliTopic topic = commandManager.getTopic(topicName);
        if (topic == null) {
          result = ResultBuilder.createInfoResult(CliStrings.format(CliStrings.HINT__MSG__UNKNOWN_TOPIC, topicName));
        } else {
          CompositeResultData compositeResultData = ResultBuilder.createCompositeResultData();
          SectionResultData commandHelpSection = compositeResultData.addSection("Commands And Help");
          compositeResultData.setHeader(topic.getOneLinerDescription());
          Map<String, String> commandsNameHelp = topic.getCommandsNameHelp();
          Set<Entry<String, String>> entries = commandsNameHelp.entrySet();
          
          for (Entry<String, String> entry : entries) {
            commandHelpSection.addData(entry.getKey(), entry.getValue());
          }

          result = ResultBuilder.buildResult(compositeResultData);
        }
      }
    }
    
    return result;
  }
}
