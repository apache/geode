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
package org.apache.geode.internal.process;

import static java.util.Collections.synchronizedList;
import static org.apache.geode.internal.process.ProcessLauncherContext.getOverriddenDefaults;
import static org.apache.geode.internal.process.ProcessLauncherContext.getStartupListener;
import static org.apache.geode.internal.process.ProcessLauncherContext.isRedirectingOutput;
import static org.apache.geode.internal.process.ProcessLauncherContext.remove;
import static org.apache.geode.internal.process.ProcessLauncherContext.set;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

/**
 * Unit tests for {@link ProcessLauncherContext}.
 */
public class ProcessLauncherContextTest {

  private boolean redirectOutput;
  private Properties overriddenDefaults;
  private StartupStatusListener startupListener;
  private List<String> statusMessageList;

  @Before
  public void before() throws Exception {
    redirectOutput = false;
    overriddenDefaults = new Properties();
    startupListener = mock(StartupStatusListener.class);
    statusMessageList = synchronizedList(new ArrayList<>());
  }

  @After
  public void after() throws Exception {
    remove();
  }

  @Test
  public void isRedirectingOutput_defaultsToFalse() throws Exception {
    assertThat(isRedirectingOutput()).isFalse();
  }

  @Test
  public void getOverriddenDefaults_defaultsToEmpty() throws Exception {
    assertThat(getOverriddenDefaults()).isEmpty();
  }

  @Test
  public void getStartupListener_defaultsToNull() throws Exception {
    assertThat(getStartupListener()).isNull();
  }

  @Test
  public void null_overriddenDefaults_throwsIllegalArgumentException() throws Exception {
    // arrange
    overriddenDefaults = null;

    // act/assert
    assertThatThrownBy(() -> set(redirectOutput, overriddenDefaults, startupListener))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid overriddenDefaults 'null' specified");
  }

  @Test
  public void null_startupListener_isAllowed() throws Exception {
    // arrange
    startupListener = null;

    // act
    set(redirectOutput, overriddenDefaults, startupListener);

    // assert
    assertThat(getStartupListener()).isNull();
  }

  @Test
  public void empty_overriddenDefaults_isAllowed() throws Exception {
    // act
    set(redirectOutput, overriddenDefaults, startupListener);

    // assert
    assertThat(getOverriddenDefaults()).isEmpty();
  }

  @Test
  public void isRedirectingOutput_returnsPassedValue() throws Exception {
    // arrange
    redirectOutput = true;

    // act
    set(redirectOutput, overriddenDefaults, startupListener);

    // assert
    assertThat(isRedirectingOutput()).isTrue();
  }

  @Test
  public void getOverriddenDefaults_returnsPassedInProps() throws Exception {
    // arrange
    overriddenDefaults.setProperty("key", "value");

    // act
    set(redirectOutput, overriddenDefaults, startupListener);

    // assert
    assertThat(getOverriddenDefaults()).hasSize(1).containsEntry("key", "value");
  }

  @Test
  public void getStartupListener_returnsPassedInListener() throws Exception {
    // arrange
    overriddenDefaults.setProperty("key", "value");

    // act
    set(redirectOutput, overriddenDefaults, startupListener);

    // assert
    assertThat(getStartupListener()).isSameAs(startupListener);
  }

  @Test
  public void remove_clearsOverriddenDefaults() throws Exception {
    // arrange
    overriddenDefaults.setProperty("key", "value");
    set(false, overriddenDefaults, startupListener);

    // act
    remove();

    // assert
    assertThat(getOverriddenDefaults()).isEmpty();
  }

  @Test
  public void remove_unsetsRedirectOutput() throws Exception {
    // arrange
    redirectOutput = true;
    set(redirectOutput, overriddenDefaults, startupListener);

    // act
    remove();

    // assert
    assertThat(isRedirectingOutput()).isFalse();
  }

  @Test
  public void remove_clearsStartupListener() throws Exception {
    // arrange
    startupListener = statusMessage -> statusMessageList.add(statusMessage);
    set(redirectOutput, overriddenDefaults, startupListener);

    // act
    remove();

    // assert
    assertThat(getStartupListener()).isNull();
  }

  @Test
  public void startupListener_installsInStartupStatus() throws Exception {
    // arrange
    startupListener = statusMessage -> statusMessageList.add(statusMessage);

    // act
    set(redirectOutput, overriddenDefaults, startupListener);

    // assert
    assertThat(StartupStatus.getStartupListener()).isSameAs(startupListener);
  }

  @Test
  public void remove_uninstallsInStartupStatus() throws Exception {
    // arrange
    startupListener = statusMessage -> statusMessageList.add(statusMessage);
    set(redirectOutput, overriddenDefaults, startupListener);

    // act
    remove();

    // assert
    assertThat(StartupStatus.getStartupListener()).isNull();
  }
}
