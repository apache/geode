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
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;


@Category(UnitTest.class)
public class CliFunctionResultTest {

  private CliFunctionResult result;

  @Test
  public void getErrorMessage() throws Exception {
    result = new CliFunctionResult("memberName", false, "message");
    assertThat(result.getErrorMessage()).isEqualTo("message");

    result = new CliFunctionResult("memberName", new Exception("exception message"), "message");
    assertThat(result.getErrorMessage()).isEqualTo("message");

    result = new CliFunctionResult("memberName", new Exception("exception message"), null);
    assertThat(result.getErrorMessage()).isEqualTo("java.lang.Exception: exception message");
  }
}
