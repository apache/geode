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
package org.apache.geode.management.internal.cli.functions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CliFunctionExecutionResult
    implements Comparable<CliFunctionExecutionResult>, Serializable {
  private String memberName;
  private String message;
  private Object resultObject;
  private boolean successful;

  public CliFunctionExecutionResult(String memberName, Throwable throwable) {
    this(memberName, throwable.getMessage(), throwable, false);
  }

  public CliFunctionExecutionResult(String memberName, Object resultObject, String message) {
    this(memberName, message, resultObject, true);
  }

  public CliFunctionExecutionResult(String memberName, String message, boolean successful) {
    this(memberName, message, null, successful);
  }

  public CliFunctionExecutionResult(String memberName, String message, Object resultObject,
      boolean successful) {
    this.memberName = memberName;
    this.message = message;
    this.resultObject = resultObject;
    this.successful = successful;
  }

  public String getMemberName() {
    return memberName;
  }

  public String getMessage() {
    if (message != null)
      return message;
    return successful ? "success" : "failed";
  }

  public Object getResultObject() {
    return resultObject;
  }

  public boolean isSuccessful() {
    return successful;
  }

  @Override
  public int compareTo(CliFunctionExecutionResult o) {
    if (this.memberName == null && o.memberName == null) {
      return 0;
    }
    if (this.memberName == null && o.memberName != null) {
      return -1;
    }
    if (this.memberName != null && o.memberName == null) {
      return 1;
    }
    return getMemberName().compareTo(o.memberName);
  }

  public static List<CliFunctionExecutionResult> cleanResults(List<?> results) {
    List<CliFunctionExecutionResult> returnResults = new ArrayList(results.size());
    for (Object result : results) {
      if (result instanceof CliFunctionExecutionResult) {
        returnResults.add((CliFunctionExecutionResult) result);
      }
    }
    Collections.sort(returnResults);
    return returnResults;
  }
}
