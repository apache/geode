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

import org.apache.geode.management.api.ClusterManagementResult;

public class ClusterManagementResultTest {
  private ClusterManagementResult result;

  @Before
  public void setup() {
    result = new ClusterManagementResult();
  }

  @Test
  public void failsWhenNotAppliedOnAllMembers() {
    result.addMemberStatus("member-1", true, "msg-1");
    result.addMemberStatus("member-2", false, "msg-2");
    result.setPersistenceStatus(true, "message");
    assertThat(result.isRealizedOnAllOrNone()).isFalse();
    assertThat(result.isSuccessful()).isFalse();
  }

  @Test
  public void successfulOnlyWhenResultIsSuccessfulOnAllMembers() {
    result.addMemberStatus("member-1", true, "msg-1");
    result.addMemberStatus("member-2", true, "msg-2");
    result.setPersistenceStatus(true, "message");
    assertThat(result.isRealizedOnAllOrNone()).isTrue();
    assertThat(result.isSuccessful()).isTrue();
  }

  @Test
  public void emptyMemberStatus() {
    assertThat(result.isRealizedOnAllOrNone()).isTrue();
    assertThat(result.isPersisted()).isFalse();
    assertThat(result.isSuccessful()).isFalse();
  }


  @Test
  public void failsWhenNotPersisted() {
    result.setPersistenceStatus(false, "msg-1");
    assertThat(result.isPersisted()).isFalse();
    assertThat(result.isSuccessful()).isFalse();
  }

  @Test
  public void whenNoMembersExists() {
    result.setPersistenceStatus(false, "msg-1");
    assertThat(result.isPersisted()).isFalse();
    assertThat(result.isRealizedOnAllOrNone()).isTrue();
    assertThat(result.isSuccessful()).isFalse();

    result.setPersistenceStatus(true, "msg-1");
    assertThat(result.isPersisted()).isTrue();
    assertThat(result.isRealizedOnAllOrNone()).isTrue();
    assertThat(result.isSuccessful()).isTrue();
  }
}
