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
package com.gemstone.gemfire.management.internal.cli.parser;

/**
 * Argument of a Command
 * 
 * @since GemFire 7.0
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
