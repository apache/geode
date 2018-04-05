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

package org.apache.geode.management.internal.cli.result;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ErrorResultDataTest {

  @Test
  public void emptyError() {
    ErrorResultData result = new ErrorResultData();
    assertThat(result.getGfJsonObject().getString("content")).isEqualTo("{}");
  }

  @Test
  public void errorWithMessage() {
    ErrorResultData result = new ErrorResultData("This is an error");
    assertThat(result.getGfJsonObject().getJSONObject("content").getString("message"))
        .isEqualTo("[\"This is an error\"]");
  }

  @Test
  public void errorWithErrorCode() {
    ErrorResultData result = new ErrorResultData("This is an error");
    result.setErrorCode(77);

    assertThat(result.getGfJsonObject().getJSONObject("content").getString("message"))
        .isEqualTo("[\"This is an error\"]");
    assertThat(result.getGfJsonObject().getJSONObject("content").getString("errorCode"))
        .isEqualTo("77");
  }

  @Test
  public void errorWithMultipleMessages() {
    ErrorResultData result = new ErrorResultData("This is an error");
    result.addLine("This is another error");

    assertThat(result.getGfJsonObject().getJSONObject("content").getString("message"))
        .isEqualTo("[\"This is an error\",\"This is another error\"]");
  }

}
