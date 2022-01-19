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
package org.apache.geode.internal.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.util.regex.Pattern;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;


public class MainWithChildrenRollingFileHandlerIntegrationTest {

  private String name;
  private MainWithChildrenRollingFileHandler handler;

  @Rule
  public TestName testName = new TestName();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    name = testName.getMethodName();
    handler = new MainWithChildrenRollingFileHandler();
  }

  @Test
  public void getFilePattern_matchesFilesWithBothIds() throws Exception {
    Pattern pattern = handler.getFilePattern(name);

    assertThat(pattern).isNotNull();
    assertThat(pattern.matcher(name).matches()).isFalse();
    assertThat(pattern.matcher(name + "-01-01").matches()).isTrue();
    assertThat(pattern.matcher(name + "-01-02").matches()).isTrue();
    assertThat(pattern.matcher(name + "-02-01").matches()).isTrue();
    assertThat(pattern.matcher(name + "-01").matches()).isFalse();
    assertThat(pattern.matcher(name + "0101").matches()).isFalse();
    assertThat(pattern.matcher(name + "--").matches()).isFalse();

    // TODO: revisit these to determine if behavior should change
    assertThat(pattern.matcher(name + "-01-01-01").matches()).isFalse();
    assertThat(pattern.matcher(name + ".01-01-01").matches()).isFalse();
  }

  @Test
  public void getFilePattern_withNumbers_matchesFiles() throws Exception {
    name = "a1s2d3f4_cache1_statistics";
    Pattern pattern = handler.getFilePattern(name);

    assertThat(pattern).isNotNull();
    assertThat(pattern.matcher(name + "-01-41").matches()).isTrue();
  }

  @Test
  public void getFilePattern_withHyphens_matchesFiles() throws Exception {
    name = "a1s2d3f4_cache1-statistics";
    Pattern pattern = handler.getFilePattern(name);

    assertThat(pattern).isNotNull();
    assertThat(pattern.matcher(name + "-01-41").matches()).isTrue();
  }

  @Test
  public void getFilePattern_empty_throwsIllegalArgumentException() throws Exception {
    name = "";

    assertThatThrownBy(() -> handler.getFilePattern(name))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void getFilePattern_null_throwsIllegalArgumentException() throws Exception {
    name = null;

    assertThatThrownBy(() -> handler.getFilePattern(name))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void calcNextChildId_noExtensionInFilename_doesNotThrow() {
    File file = new File(temporaryFolder.getRoot(), "fileWithoutExtension");
    assertThat(handler.calcNextChildId(file, 0)).isEqualTo(1);
  }

}
