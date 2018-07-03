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

import static org.apache.commons.lang.SystemUtils.LINE_SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.assertj.core.api.Assertions;
import org.junit.Test;


public class CommandResultTest {

  @Test
  public void emptyResultHasOneEmptyLine() {
    CommandResult commandResult = new LegacyCommandResult(new InfoResultData());
    Assertions.assertThat(commandResult.nextLine()).isEqualTo("");
    Assertions.assertThat(commandResult.hasNextLine()).isFalse();
  }

  @Test
  public void resultWithOneLineHasOneLine() {
    CommandResult commandResult = new LegacyCommandResult(new InfoResultData("oneLine"));

    assertThat(commandResult.nextLine()).isEqualTo("oneLine" + LINE_SEPARATOR);
    assertThat(commandResult.hasNextLine()).isFalse();
  }

  @Test
  public void resultWithTwoLinesHasTwoLines() {
    InfoResultData resultData = new InfoResultData();
    resultData.addLine("lineOne");
    resultData.addLine("lineTwo");
    CommandResult commandResult = new LegacyCommandResult(resultData);

    assertThat(commandResult.nextLine())
        .isEqualTo("lineOne" + LINE_SEPARATOR + "lineTwo" + LINE_SEPARATOR);
    assertThat(commandResult.hasNextLine()).isFalse();
  }

  @Test
  public void emptyResultDoesNotHaveFileToDownload() {
    CommandResult commandResult = new LegacyCommandResult(new InfoResultData());
    Assertions.assertThat(commandResult.hasFileToDownload()).isFalse();
  }

  @Test
  public void resultWithFileDoesHaveFileToDownload() {
    Path fileToDownload = Paths.get(".").toAbsolutePath();
    CommandResult commandResult = new LegacyCommandResult(fileToDownload);

    assertThat(commandResult.hasFileToDownload()).isTrue();
    assertThat(commandResult.nextLine()).isEqualTo(fileToDownload.toString() + LINE_SEPARATOR);
    assertThat(commandResult.getFileToDownload()).isEqualTo(fileToDownload);
  }
}
