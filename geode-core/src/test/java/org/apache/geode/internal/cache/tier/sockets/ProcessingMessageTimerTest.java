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

package org.apache.geode.internal.cache.tier.sockets;

import static org.apache.geode.internal.cache.tier.sockets.ServerConnection.ProcessingMessageTimer.NOT_PROCESSING;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import org.apache.geode.internal.cache.tier.sockets.ServerConnection.ProcessingMessageTimer;

public class ProcessingMessageTimerTest {

  @Test
  public void newInstancesNotProcessingMessage() {
    ProcessingMessageTimer timer = new ProcessingMessageTimer();
    assertThat(timer.getCurrentMessageProcessingTime()).isEqualTo(NOT_PROCESSING);
  }

  @Test
  public void updateWhenNotSetDoesNotSet() {
    ProcessingMessageTimer timer = new ProcessingMessageTimer();
    timer.updateProcessingMessage();
    assertThat(timer.getCurrentMessageProcessingTime()).isEqualTo(NOT_PROCESSING);
  }

  @Test
  public void setWhenNotSetWillSet() {
    ProcessingMessageTimer timer = new ProcessingMessageTimer();
    timer.setProcessingMessage();
    assertThat(timer.getCurrentMessageProcessingTime()).isNotEqualTo(NOT_PROCESSING);
  }

  @Test
  public void getCurrentProgressesWhenSet() throws InterruptedException {
    ProcessingMessageTimer timer = new ProcessingMessageTimer();
    timer.setProcessingMessage();
    Thread.sleep(2);
    long firstSample = timer.getCurrentMessageProcessingTime();
    assertThat(firstSample).isGreaterThan(0);
    Thread.sleep(2);
    assertThat(timer.getCurrentMessageProcessingTime()).isGreaterThan(firstSample);
  }

  @Test
  public void updateWhenSetWillReset() throws InterruptedException {
    ProcessingMessageTimer timer = new ProcessingMessageTimer();
    timer.setProcessingMessage();
    final long previous = timer.processingMessageStartTime.get();
    Thread.sleep(2);
    timer.updateProcessingMessage();
    assertThat(timer.processingMessageStartTime.get()).isNotEqualTo(previous);
  }

  @Test
  public void setWhenSetWillReset() throws InterruptedException {
    ProcessingMessageTimer timer = new ProcessingMessageTimer();
    timer.setProcessingMessage();
    final long previous = timer.processingMessageStartTime.get();
    Thread.sleep(2);
    timer.setProcessingMessage();
    assertThat(timer.processingMessageStartTime.get()).isNotEqualTo(previous);
  }

  @Test
  public void setNotProcessingWhenSetWillNotProcessing() {
    ProcessingMessageTimer timer = new ProcessingMessageTimer();
    timer.setProcessingMessage();
    timer.setNotProcessingMessage();
    assertThat(timer.getCurrentMessageProcessingTime()).isEqualTo(NOT_PROCESSING);
  }

}
