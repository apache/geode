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

/**
 * Unit tests for {@link ProcessLauncherContext}.
 */
public class ProcessLauncherContextTest {

  private boolean redirectOutput;
  private Properties overriddenDefaults;
  private StartupStatusListener startupListener;
  private List<String> statusMessageList;

  @Before
  public void before() {
    redirectOutput = false;
    overriddenDefaults = new Properties();
    startupListener = mock(StartupStatusListener.class);
    statusMessageList = synchronizedList(new ArrayList<>());
  }

  @After
  public void after() {
    remove();
  }

  @Test
  public void isRedirectingOutput_defaultsToFalse() {
    assertThat(isRedirectingOutput()).isFalse();
  }

  @Test
  public void getOverriddenDefaults_defaultsToEmpty() {
    assertThat(getOverriddenDefaults()).isEmpty();
  }

  @Test
  public void getStartupListener_defaultsToNull() {
    assertThat(getStartupListener()).isNull();
  }

  @Test
  public void null_overriddenDefaults_throwsIllegalArgumentException() {
    // arrange
    overriddenDefaults = null;

    // act/assert
    assertThatThrownBy(() -> set(redirectOutput, overriddenDefaults, startupListener))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid overriddenDefaults 'null' specified");
  }

  @Test
  public void null_startupListener_isAllowed() {
    // arrange
    startupListener = null;

    // act
    set(redirectOutput, overriddenDefaults, startupListener);

    // assert
    assertThat(getStartupListener()).isNull();
  }

  @Test
  public void empty_overriddenDefaults_isAllowed() {
    // act
    set(redirectOutput, overriddenDefaults, startupListener);

    // assert
    assertThat(getOverriddenDefaults()).isEmpty();
  }

  @Test
  public void isRedirectingOutput_returnsPassedValue() {
    // arrange
    redirectOutput = true;

    // act
    set(redirectOutput, overriddenDefaults, startupListener);

    // assert
    assertThat(isRedirectingOutput()).isTrue();
  }

  @Test
  public void getOverriddenDefaults_returnsPassedInProps() {
    // arrange
    overriddenDefaults.setProperty("key", "value");

    // act
    set(redirectOutput, overriddenDefaults, startupListener);

    // assert
    assertThat(getOverriddenDefaults()).hasSize(1).containsEntry("key", "value");
  }

  @Test
  public void getStartupListener_returnsPassedInListener() {
    // arrange
    overriddenDefaults.setProperty("key", "value");

    // act
    set(redirectOutput, overriddenDefaults, startupListener);

    // assert
    assertThat(getStartupListener()).isSameAs(startupListener);
  }

  @Test
  public void remove_clearsOverriddenDefaults() {
    // arrange
    overriddenDefaults.setProperty("key", "value");
    set(false, overriddenDefaults, startupListener);

    // act
    remove();

    // assert
    assertThat(getOverriddenDefaults()).isEmpty();
  }

  @Test
  public void remove_unsetsRedirectOutput() {
    // arrange
    redirectOutput = true;
    set(redirectOutput, overriddenDefaults, startupListener);

    // act
    remove();

    // assert
    assertThat(isRedirectingOutput()).isFalse();
  }

  @Test
  public void remove_clearsStartupListener() {
    // arrange
    startupListener = statusMessage -> statusMessageList.add(statusMessage);
    set(redirectOutput, overriddenDefaults, startupListener);

    // act
    remove();

    // assert
    assertThat(getStartupListener()).isNull();
  }

  @Test
  public void startupListener_installsInStartupStatus() {
    // arrange
    startupListener = statusMessage -> statusMessageList.add(statusMessage);

    // act
    set(redirectOutput, overriddenDefaults, startupListener);

    // assert
    assertThat(StartupStatusListenerRegistry.getStartupListener()).isSameAs(startupListener);
  }

  @Test
  public void remove_uninstallsInStartupStatus() {
    // arrange
    startupListener = statusMessage -> statusMessageList.add(statusMessage);
    set(redirectOutput, overriddenDefaults, startupListener);

    // act
    remove();

    // assert
    assertThat(StartupStatusListenerRegistry.getStartupListener()).isNull();
  }
}
