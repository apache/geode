/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.parser;

/**
 * Argument of a Command
 * 
 * @author Nikhil Jadhav
 * @since 7.0
 *
 */
public class Argument extends Parameter {
  private String argumentName;

  public String getArgumentName() {
    return argumentName;
  }

  public void setArgumentName(String argumentName) {
    this.argumentName = argumentName;
  }
  
  @Override
  public int hashCode() {
    final int prime = 13;
    int result = 1;
    result = prime * result
        + ((argumentName == null) ? 0 : argumentName.hashCode());
    return result;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Argument argument = (Argument) obj;
    if (argumentName == null) {
      if (argument.getArgumentName() != null) {
        return false;
      }
    } else if (!argumentName.equals(argument.getArgumentName())) {
      return false;
    }
    return true;
  }
  
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(Argument.class.getSimpleName())
        .append("[name=" + argumentName).append(",help=" + help)
        .append(",required" + required + "]");
    return builder.toString();
  }
}
