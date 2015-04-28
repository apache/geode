/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.parser;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Stores the result after parsing
 * 
 * @author Nikhil Jadhav
 * @since 7.0
 *
 */
public class OptionSet {
  private Map<Option, String> optionsMap;
  private Map<Argument, String> argumentsMap;
  private int noOfSpacesRemoved;
  private List<String> split;
  private String userInput;

  public OptionSet() {
    optionsMap = new HashMap<Option, String>();
    argumentsMap = new HashMap<Argument, String>();
  }

  public void put(Argument argument, String value) {
    argumentsMap.put(argument, value);
  }

  public void put(Option option, String value) {
    optionsMap.put(option, value);
  }

  public boolean hasOption(Option option) {
    return optionsMap.containsKey(option);
  }

  public boolean hasArgument(Argument argument) {
    String string = argumentsMap.get(argument);
    if (string != null) {
      return true;
    } else {
      return false;
    }
  }

  public boolean hasValue(Option option) {
    String string = optionsMap.get(option);
    if (string != null && !string.equals("__NULL__")) {
      return true;
    } else {
      return false;
    }
  }

  public String getValue(Argument argument) {
    return argumentsMap.get(argument);
  }

  public String getValue(Option option) {
    return optionsMap.get(option);
  }

  public boolean areArgumentsPresent() {
    if (!argumentsMap.isEmpty()) {
      return true;
    } else
      return false;
  }

  public boolean areOptionsPresent() {
    if (!optionsMap.isEmpty()) {
      return true;
    } else {
      return false;
    }
  }

  public int getNoOfSpacesRemoved() {
    return noOfSpacesRemoved;
  }

  public void setNoOfSpacesRemoved(int noOfSpacesRemoved) {
    this.noOfSpacesRemoved = noOfSpacesRemoved;
  }

  /**
   * @return the split
   */
  public List<String> getSplit() {
    return split;
  }

  /**
   * @param split
   *          the split to set
   */
  public void setSplit(List<String> split) {
    this.split = split;
  }

  public String getUserInput() {
    return userInput;
  }

  public void setUserInput(String userInput) {
    this.userInput = userInput;
  }

  @Override
  public String toString() {
    return "OptionSet [optionsMap=" + optionsMap + ", argumentsMap="
        + argumentsMap + ", noOfSpacesRemoved=" + noOfSpacesRemoved
        + ", split=" + split + ", userInput=" + userInput + "]";
  }
}
