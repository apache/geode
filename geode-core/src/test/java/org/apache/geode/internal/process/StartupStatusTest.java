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
import static org.apache.geode.internal.process.StartupStatus.clearListener;
import static org.apache.geode.internal.process.StartupStatus.getStartupListener;
import static org.apache.geode.internal.process.StartupStatus.setListener;
import static org.apache.geode.internal.process.StartupStatus.startup;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class StartupStatusTest {

  private StartupStatusListener listener;
  private List<String> statusMessageList;

  @Before
  public void before() throws Exception {
    listener = mock(StartupStatusListener.class);
    statusMessageList = synchronizedList(new ArrayList<>());
  }

  @After
  public void after() throws Exception {
    clearListener();
  }

  @Test
  public void getStartupListener_returnsNullByDefault() throws Exception {
    // act/assert
    assertThat(getStartupListener()).isNull();
  }

  @Test
  public void setListener_null_clearsStartupListener() throws Exception {
    // arrange
    listener = null;

    // act
    setListener(listener);

    // assert
    assertThat(getStartupListener()).isNull();
  }

  @Test
  public void getStartupListener_returnsSetListener() throws Exception {
    // arrange
    setListener(listener);

    // act/assert
    assertThat(getStartupListener()).isSameAs(listener);
  }

  @Test
  public void clearListener_doesNothingIfNull() throws Exception {
    // arrange
    listener = null;
    setListener(listener);
    assertThat(getStartupListener()).isNull();

    // act
    clearListener();

    // assert
    assertThat(getStartupListener()).isNull();
  }

  @Test
  public void clearListener_unsetsListener() throws Exception {
    // arrange
    setListener(listener);
    assertThat(getStartupListener()).isNotNull();

    // act
    clearListener();

    // assert
    assertThat(getStartupListener()).isNull();
  }

  @Test
  public void startup_nullStringId_throwsIllegalArgumentException() throws Exception {
    // arrange
    String stringId = null;
    Object[] params = new Object[0];

    // act/assert
    assertThatThrownBy(() -> startup(stringId, params)).isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid msgId 'null' specified");
  }

  @Test
  public void startup_emptyParams() throws Exception {
    // arrange
    String stringId = "my string";
    Object[] params = new Object[0];

    // act
    startup(stringId, params);

    // assert (does not throw)
    assertThat(getStartupListener()).isNull();
  }

  @Test
  public void startup_doesNothingIfNoListener() throws Exception {
    // arrange
    String stringId = "my string";
    Object[] params = new Object[0];

    // act
    startup(stringId, params);

    // assert (does nothing)
    assertThat(getStartupListener()).isNull();
  }

  @Test
  public void startup_invokesListener() throws Exception {
    // arrange
    listener = statusMessage -> statusMessageList.add(statusMessage);
    String stringId = "my string";
    Object[] params = new Object[0];
    setListener(listener);

    // act
    startup(stringId, params);

    // assert
    assertThat(statusMessageList).hasSize(1).contains("my string");
  }

  @Test
  public void startupTwice_invokesListenerTwice() throws Exception {
    // arrange
    listener = statusMessage -> statusMessageList.add(statusMessage);
    String stringIdOne = "my string";
    String stringIdTwo = "other string";
    Object[] params = new Object[0];
    setListener(listener);

    // act
    startup(stringIdOne, params);
    startup(stringIdTwo, params);

    // assert
    assertThat(statusMessageList).hasSize(2).contains("my string").contains("other string");
  }
}
