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

package org.apache.geode.test.junit.assertions;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.ListAssert;

import org.apache.geode.management.api.ClusterManagementListResult;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.EntityGroupInfo;
import org.apache.geode.management.api.EntityInfo;
import org.apache.geode.management.configuration.AbstractConfiguration;
import org.apache.geode.management.runtime.RuntimeInfo;

public class ClusterManagementListResultAssert<T extends AbstractConfiguration<R>, R extends RuntimeInfo>
    extends
    AbstractAssert<ClusterManagementListResultAssert<T, R>, ClusterManagementListResult<T, R>> {
  public ClusterManagementListResultAssert(
      ClusterManagementListResult<T, R> clusterManagementResult, Class<?> selfType) {
    super(clusterManagementResult, selfType);
  }

  public ClusterManagementListResultAssert<T, R> isSuccessful() {
    assertThat(actual.isSuccessful()).isTrue();
    return this;
  }

  public ClusterManagementListResultAssert<T, R> failed() {
    assertThat(actual.isSuccessful()).isFalse();
    return this;
  }

  public ClusterManagementListResultAssert<T, R> hasStatusCode(
      ClusterManagementResult.StatusCode... codes) {
    assertThat(actual.getStatusCode()).isIn((Object[]) codes);
    return this;
  }

  public ClusterManagementListResultAssert<T, R> containsStatusMessage(String statusMessage) {
    assertThat(actual.getStatusMessage()).contains(statusMessage);
    return this;
  }

  public List<EntityGroupInfo<T, R>> getResult() {
    return actual.getEntityGroupInfo();
  };

  public ListAssert<T> hasConfigurations() {
    return assertThat(getActual().getConfigResult());
  }

  public ListAssert<R> hasRuntimeInfos() {
    return assertThat(getActual().getRuntimeResult());
  }

  public ListAssert<EntityInfo<T, R>> hasEntityInfo() {
    return assertThat(getActual().getResult());
  }

  public static <T extends AbstractConfiguration<R>, R extends RuntimeInfo> ClusterManagementListResultAssert<T, R> assertManagementListResult(
      ClusterManagementListResult<T, R> result) {
    return new ClusterManagementListResultAssert<>(result, ClusterManagementListResultAssert.class);
  }

  public ClusterManagementListResult<T, R> getActual() {
    return actual;
  }
}
