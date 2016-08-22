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
package com.gemstone.gemfire.management.internal.cli.help;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.parser.CommandTarget;

/**
 * 
 * 
 * @since GemFire 7.0
 */
public class CliTopic implements Comparable<CliTopic> {
  private static final Map<String, String> nameDescriptionMap = new HashMap<String, String>();

  static {
    nameDescriptionMap.put(CliStrings.DEFAULT_TOPIC_GEODE,    CliStrings.DEFAULT_TOPIC_GEODE__DESC);
    nameDescriptionMap.put(CliStrings.TOPIC_GEODE_REGION,     CliStrings.TOPIC_GEODE_REGION__DESC);
    nameDescriptionMap.put(CliStrings.TOPIC_GEODE_WAN,        CliStrings.TOPIC_GEODE_WAN__DESC);
    nameDescriptionMap.put(CliStrings.TOPIC_GEODE_JMX,        CliStrings.TOPIC_GEODE_JMX__DESC);
    nameDescriptionMap.put(CliStrings.TOPIC_GEODE_DISKSTORE,  CliStrings.TOPIC_GEODE_DISKSTORE__DESC);
    nameDescriptionMap.put(CliStrings.TOPIC_GEODE_LOCATOR,    CliStrings.TOPIC_GEODE_LOCATOR__DESC);
    nameDescriptionMap.put(CliStrings.TOPIC_GEODE_SERVER,     CliStrings.TOPIC_GEODE_SERVER__DESC);
    nameDescriptionMap.put(CliStrings.TOPIC_GEODE_MANAGER,    CliStrings.TOPIC_GEODE_MANAGER__DESC);
    nameDescriptionMap.put(CliStrings.TOPIC_GEODE_STATISTICS, CliStrings.TOPIC_GEODE_STATISTICS__DESC);
    nameDescriptionMap.put(CliStrings.TOPIC_GEODE_LIFECYCLE,  CliStrings.TOPIC_GEODE_LIFECYCLE__DESC);
    nameDescriptionMap.put(CliStrings.TOPIC_GEODE_M_AND_M,    CliStrings.TOPIC_GEODE_M_AND_M__DESC);
    nameDescriptionMap.put(CliStrings.TOPIC_GEODE_DATA,       CliStrings.TOPIC_GEODE_DATA__DESC);
    nameDescriptionMap.put(CliStrings.TOPIC_GEODE_CONFIG,     CliStrings.TOPIC_GEODE_CONFIG__DESC);
    nameDescriptionMap.put(CliStrings.TOPIC_GEODE_FUNCTION,   CliStrings.TOPIC_GEODE_FUNCTION__DESC);
    nameDescriptionMap.put(CliStrings.TOPIC_GEODE_HELP,       CliStrings.TOPIC_GEODE_HELP__DESC);
    nameDescriptionMap.put(CliStrings.TOPIC_GEODE_DEBUG_UTIL, CliStrings.TOPIC_GEODE_DEBUG_UTIL__DESC);
    nameDescriptionMap.put(CliStrings.TOPIC_GFSH,               CliStrings.TOPIC_GFSH__DESC);
  }


  private final String       name;
  private final String       oneLinerDescription;
  private Set<CommandTarget> commandTargets;
  
  public CliTopic(String name) {
    this.name                = name;
    this.oneLinerDescription = nameDescriptionMap.get(this.name);
    this.commandTargets      = new HashSet<CommandTarget>();
  }

  public String getName() {
    return name;
  }

  public String getOneLinerDescription() {
    return oneLinerDescription;
  }

  public void addCommandTarget(CommandTarget commandTarget) {
    commandTargets.add(commandTarget);
  }
  
  public Map<String, String> getCommandsNameHelp() {
    Map<String, String> commandsNameHelp = new TreeMap<String, String>();
    
    for (CommandTarget commandTarget : commandTargets) {
      commandsNameHelp.put(commandTarget.getCommandName(), commandTarget.getCommandHelp());
    }
    
    return commandsNameHelp;
  }

  @Override
  public int compareTo(CliTopic o) {
    if (o != null) {
      return this.name.compareTo(o.name);
    } else {
      return -1;
    }
  }

  // hashCode & equals created using Eclipse
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!getClass().isInstance(obj)) {
      return false;
    }
    CliTopic other = (CliTopic) obj;
    if (name == null) {
      if (other.name != null) {
        return false;
      }
    } else if (!name.equals(other.name)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return CliTopic.class.getSimpleName() + "["+name+"]";
  }
}
