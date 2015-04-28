/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.parser;

import java.lang.reflect.InvocationTargetException;

import com.gemstone.gemfire.management.internal.cli.GfshParser;

/**
 * Used by {@link GfshParser} to store details of a command
 * 
 * @author Nikhil Jadhav
 * @since 7.0
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