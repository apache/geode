/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
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
 * @author Abhishek Chaudhari
 * 
 * @since 7.0
 */
public class CliTopic implements Comparable<CliTopic> {
  private static final Map<String, String> nameDescriptionMap = new HashMap<String, String>();

  static {
    nameDescriptionMap.put(CliStrings.DEFAULT_TOPIC_GEMFIRE,    CliStrings.DEFAULT_TOPIC_GEMFIRE__DESC);
    nameDescriptionMap.put(CliStrings.TOPIC_GEMFIRE_REGION,     CliStrings.TOPIC_GEMFIRE_REGION__DESC);
    nameDescriptionMap.put(CliStrings.TOPIC_GEMFIRE_WAN,        CliStrings.TOPIC_GEMFIRE_WAN__DESC);
    nameDescriptionMap.put(CliStrings.TOPIC_GEMFIRE_JMX,        CliStrings.TOPIC_GEMFIRE_JMX__DESC);
    nameDescriptionMap.put(CliStrings.TOPIC_GEMFIRE_DISKSTORE,  CliStrings.TOPIC_GEMFIRE_DISKSTORE__DESC);
    nameDescriptionMap.put(CliStrings.TOPIC_GEMFIRE_LOCATOR,    CliStrings.TOPIC_GEMFIRE_LOCATOR__DESC);
    nameDescriptionMap.put(CliStrings.TOPIC_GEMFIRE_SERVER,     CliStrings.TOPIC_GEMFIRE_SERVER__DESC);
    nameDescriptionMap.put(CliStrings.TOPIC_GEMFIRE_MANAGER,    CliStrings.TOPIC_GEMFIRE_MANAGER__DESC);
    nameDescriptionMap.put(CliStrings.TOPIC_GEMFIRE_STATISTICS, CliStrings.TOPIC_GEMFIRE_STATISTICS__DESC);
    nameDescriptionMap.put(CliStrings.TOPIC_GEMFIRE_LIFECYCLE,  CliStrings.TOPIC_GEMFIRE_LIFECYCLE__DESC);
    nameDescriptionMap.put(CliStrings.TOPIC_GEMFIRE_M_AND_M,    CliStrings.TOPIC_GEMFIRE_M_AND_M__DESC);
    nameDescriptionMap.put(CliStrings.TOPIC_GEMFIRE_DATA,       CliStrings.TOPIC_GEMFIRE_DATA__DESC);
    nameDescriptionMap.put(CliStrings.TOPIC_GEMFIRE_CONFIG,     CliStrings.TOPIC_GEMFIRE_CONFIG__DESC);
    nameDescriptionMap.put(CliStrings.TOPIC_GEMFIRE_FUNCTION,   CliStrings.TOPIC_GEMFIRE_FUNCTION__DESC);
    nameDescriptionMap.put(CliStrings.TOPIC_GEMFIRE_HELP,       CliStrings.TOPIC_GEMFIRE_HELP__DESC);
    nameDescriptionMap.put(CliStrings.TOPIC_GEMFIRE_DEBUG_UTIL, CliStrings.TOPIC_GEMFIRE_DEBUG_UTIL__DESC);
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
