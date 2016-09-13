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
package org.apache.geode.management.internal.cli.parser;

import java.lang.reflect.InvocationTargetException;

import org.apache.geode.management.internal.cli.GfshParser;

/**
 * Used by {@link GfshParser} to store details of a command
 * 
 * @since GemFire 7.0
 * 
 */
public class CommandTarget {
  private final String commandName;
  private final String[] synonyms;
  private final String commandHelp;
  private final GfshMethodTarget gfshMethodTarget;
  private final GfshOptionParser optionParser;
  private AvailabilityTarget availabilityIndicator;

  public CommandTarget(String commandName, String[] synonyms,
      GfshMethodTarget methodTarget, GfshOptionParser optionParser,
      AvailabilityTarget availabilityIndicator, String commandHelp) {
    this.commandName = commandName;
    this.synonyms = synonyms;
    this.gfshMethodTarget = methodTarget;
    this.optionParser = optionParser;
    this.availabilityIndicator = availabilityIndicator;
    this.commandHelp = commandHelp;
  }

  public GfshMethodTarget getGfshMethodTarget() {
    return gfshMethodTarget;
  }

  public GfshOptionParser getOptionParser() {
    return optionParser;
  }

  public boolean isAvailable() throws IllegalArgumentException,
      IllegalAccessException, InvocationTargetException {
    if (availabilityIndicator != null) {
      return (Boolean) availabilityIndicator.getMethod().invoke(
          availabilityIndicator.getTarget());
    } else {
      return true;
    }
  }

  public AvailabilityTarget getAvailabilityIndicator() {
    return availabilityIndicator;
  }

  // TODO Change for concurrent access
  public void setAvailabilityIndicator(AvailabilityTarget availabilityIndicator) {
    this.availabilityIndicator = availabilityIndicator;
  }

  public String getCommandHelp() {
    return commandHelp;
  }

  public CommandTarget duplicate(String key) {
    return duplicate(key, null);
  }

  public CommandTarget duplicate(String key, String remainingBuffer) {
    return new CommandTarget(commandName, synonyms, new GfshMethodTarget(
        gfshMethodTarget.getMethod(), gfshMethodTarget.getTarget(),
        remainingBuffer, key), optionParser, availabilityIndicator, commandHelp);
  }
  
  @Override
  public int hashCode() {
    final int prime = 47;
    int result = 3;
    result = prime * result
        + ((commandName == null) ? 0 : commandName.hashCode());
    result = prime * result
        + ((commandHelp == null) ? 0 : commandHelp.hashCode());
    result = prime * result
        + ((gfshMethodTarget == null) ? 0 : gfshMethodTarget.hashCode());
    result = prime * result
        + ((optionParser == null) ? 0 : optionParser.hashCode());
    result = prime
        * result
        + ((availabilityIndicator == null) ? 0 : availabilityIndicator
            .hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    // If two command targets have the same OptionParser
    // then they are equal
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    CommandTarget commandTarget = (CommandTarget) obj;
    if (commandName == null) {
      if (commandTarget.getCommandName() != null) {
        return false;
      }
    } else if (!commandName.equals(commandTarget.getCommandName())) {
      return false;
    }
    if (commandHelp == null) {
      if (commandTarget.getCommandHelp() != null) {
        return false;
      }
    } else if (!commandHelp.equals(commandTarget.getCommandHelp())) {
      return false;
    }
    if (gfshMethodTarget == null) {
      if (commandTarget.getGfshMethodTarget() != null) {
        return false;
      }
    } else if (!gfshMethodTarget.equals(commandTarget.getGfshMethodTarget())) {
      return false;
    }
    if (optionParser == null) {
      if (commandTarget.getOptionParser() != null) {
        return false;
      }
    } else if (!optionParser.equals(commandTarget.getOptionParser())) {
      return false;
    }
    if (availabilityIndicator == null) {
      if (commandTarget.getAvailabilityIndicator() != null) {
        return false;
      }
    } else if (!availabilityIndicator.equals(commandTarget
        .getAvailabilityIndicator())) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(CommandTarget.class.getSimpleName())
        .append("[" + "commandName=" + commandName)
        .append(",commandHelp=" + commandHelp);
    builder.append(",synonyms=");
    if (synonyms != null) {
      for (String string : synonyms) {
        builder.append(string + " ");
      }
    } 
    builder.append(",gfshMethodTarget=" + gfshMethodTarget);
    builder.append(",optionParser=" + optionParser);
    builder.append(",availabilityIndicator=" + availabilityIndicator);
    builder.append("]");
    return builder.toString();
  }
  
  public String getCommandName() {
    return commandName;
  }

  public String[] getSynonyms() {
    return synonyms;
  }
}
