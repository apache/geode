/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.management.internal.api;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

public class ClusterManagementResultFactoryTest {
  private ClusterManagementResult result;
  private ClusterManagementResultFactory cmrFactory;

  @Before
  public void setup() {
    cmrFactory = new ClusterManagementResultFactory();
  }

  @Test
  public void failsWhenNotAppliedOnAllMembers() {
    cmrFactory.addMemberStatus("member-1", true, "msg-1");
    cmrFactory.addMemberStatus("member-2", false, "msg-2");
    assertThat(cmrFactory.isSuccessfullyAppliedOnMembers()).isFalse();
    assertThat(cmrFactory.isSuccessful()).isFalse();
  }

  @Test
  public void successfulOnlyWhencmrFactoryIsSuccessfulOnAllMembers() {
    cmrFactory.addMemberStatus("member-1", true, "msg-1");
    cmrFactory.addMemberStatus("member-2", true, "msg-2");
    assertThat(cmrFactory.isSuccessfullyAppliedOnMembers()).isTrue();
    assertThat(cmrFactory.isSuccessful()).isTrue();
  }

  @Test
  public void emptyMemberStatus() {
    assertThat(cmrFactory.isSuccessfullyAppliedOnMembers()).isFalse();
    assertThat(cmrFactory.isSuccessfullyPersisted()).isFalse();
    assertThat(cmrFactory.isSuccessful()).isFalse();
  }


  @Test
  public void failsWhenNotPersisted() {
    cmrFactory.setPersistenceStatus(false, "msg-1");
    assertThat(cmrFactory.isSuccessfullyPersisted()).isFalse();
    assertThat(cmrFactory.isSuccessful()).isFalse();
  }

  @Test
  public void failsWhenNoMembersExists() {
    cmrFactory.setPersistenceStatus(true, "msg-1");
    assertThat(cmrFactory.isSuccessfullyPersisted()).isTrue();
    assertThat(cmrFactory.isSuccessfullyAppliedOnMembers()).isFalse();
    assertThat(cmrFactory.isSuccessful()).isFalse();
  }
}
