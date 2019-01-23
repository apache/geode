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

package org.apache.geode.management.api;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class ClusterManagementResultTest {
  @Test
  public void successfulOnlyWhenResultIsSuccessfulOnAllMembers() {
    ClusterManagementResult result = new ClusterManagementResult();
    result.addMemberStatus("member-1", Status.Result.SUCCESS, "msg-1");
    result.addMemberStatus("member-2", Status.Result.FAILURE, "msg-2");
    assertThat(result.isSuccessfullyAppliedOnMembers()).isFalse();
    assertThat(result.isSuccessful()).isFalse();

    result = new ClusterManagementResult();
    result.addMemberStatus("member-1", Status.Result.SUCCESS, "msg-1");
    result.addMemberStatus("member-2", Status.Result.SUCCESS, "msg-2");
    assertThat(result.isSuccessfullyAppliedOnMembers()).isTrue();
    assertThat(result.isSuccessful()).isTrue();
  }

  @Test
  public void successfulOnlyWhenClusterConfigIsPersisted() {
    ClusterManagementResult result = new ClusterManagementResult();
    result.setClusterConfigPersisted(Status.Result.FAILURE, "msg-1");
    assertThat(result.isSuccessfullyPersisted()).isFalse();
    assertThat(result.isSuccessful()).isFalse();

    result = new ClusterManagementResult();
    result.setClusterConfigPersisted(Status.Result.SUCCESS, "msg-1");
    assertThat(result.isSuccessfullyPersisted()).isTrue();
    assertThat(result.isSuccessful()).isTrue();
  }
}
