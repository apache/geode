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


public class InfoResultDataTest {

  @Test
  public void emptyInfo() {
    InfoResultData result = new InfoResultData();
    assertThat(result.getGfJsonObject().getString("content")).isEqualTo("{}");
  }

  @Test
  public void infoWithContent() {
    InfoResultData result = new InfoResultData("some content");
    assertThat(result.getGfJsonObject().getJSONObject("content").getString("message"))
        .isEqualTo("[\"some content\"]");
  }

  @Test
  public void infoWithMultipleContentLines() {
    InfoResultData result = new InfoResultData("some content");
    result.addLine("another line of content");
    assertThat(result.getGfJsonObject().getJSONObject("content").getString("message"))
        .isEqualTo("[\"some content\",\"another line of content\"]");
  }

  @Test
  public void infoWithFile() throws Exception {
    InfoResultData result = new InfoResultData("some content");
    ResultData data = result.addAsFile("content.zip", "file contents", "a message", false);

    assertThat(result.getGfJsonObject().getJSONObject("content").getJSONArray("__bytes__")
        .getInternalJsonObject(0).getString("fileName")).isEqualTo("content.zip");
    assertThat(result.getGfJsonObject().getJSONObject("content").getJSONArray("__bytes__")
        .getInternalJsonObject(0).getString("fileType")).isEqualTo("1");
    assertThat(result.getGfJsonObject().getJSONObject("content").getJSONArray("__bytes__")
        .getInternalJsonObject(0).getString("fileMessage")).isEqualTo("a message");
    assertThat(result.getGfJsonObject().getJSONObject("content").getJSONArray("__bytes__")
        .getInternalJsonObject(0).getString("fileData").length()).isGreaterThan(0);
  }
}
