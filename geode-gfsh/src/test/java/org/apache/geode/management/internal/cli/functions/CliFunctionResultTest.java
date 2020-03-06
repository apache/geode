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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import org.apache.geode.management.internal.functions.CliFunctionResult;

@SuppressWarnings("deprecation")
public class CliFunctionResultTest {

  private CliFunctionResult result;

  @Test
  public void getStatusWithSuccessMessage() {
    result = new CliFunctionResult("memberName", true, "message");
    assertThat(result.getLegacyStatus()).isEqualTo("message");
  }

  @Test
  public void getStatusWithErrorMessage() {
    result = new CliFunctionResult("memberName", false, "message");
    assertThat(result.getLegacyStatus()).isEqualTo("ERROR: message");
  }

  @Test
  public void getStatusWithExceptionOnly() {
    result = new CliFunctionResult("memberName", new Exception("exception message"), null);
    assertThat(result.getLegacyStatus()).isEqualTo("ERROR: java.lang.Exception: exception message");
  }

  @Test
  public void getStatusWithExceptionAndSameErrorMessage() {
    result = new CliFunctionResult("memberName", new Exception("exception message"),
        "exception message");
    assertThat(result.getLegacyStatus()).isEqualTo("ERROR: java.lang.Exception: exception message");

  }

  @Test
  public void getStatusWithExceptionAndDifferentErrorMessage() {
    result = new CliFunctionResult("memberName", new Exception("exception message"),
        "some other message");
    assertThat(result.getLegacyStatus())
        .isEqualTo("ERROR: some other message java.lang.Exception: exception message");
  }
}
