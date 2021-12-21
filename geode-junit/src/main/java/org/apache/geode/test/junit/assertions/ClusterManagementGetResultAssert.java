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

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.ListAssert;
import org.assertj.core.api.ObjectAssert;

import org.apache.geode.management.api.ClusterManagementGetResult;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.EntityGroupInfo;
import org.apache.geode.management.configuration.AbstractConfiguration;
import org.apache.geode.management.runtime.RuntimeInfo;

public class ClusterManagementGetResultAssert<T extends AbstractConfiguration<R>, R extends RuntimeInfo>
    extends
    AbstractAssert<ClusterManagementGetResultAssert<T, R>, ClusterManagementGetResult<T, R>> {
  public ClusterManagementGetResultAssert(
      ClusterManagementGetResult<T, R> clusterManagementResult, Class<?> selfType) {
    super(clusterManagementResult, selfType);
  }

  public ClusterManagementGetResultAssert<T, R> isSuccessful() {
    assertThat(actual.isSuccessful()).isTrue();
    return this;
  }

  public ClusterManagementGetResultAssert<T, R> failed() {
    assertThat(actual.isSuccessful()).isFalse();
    return this;
  }

  public ClusterManagementGetResultAssert<T, R> hasStatusCode(
      ClusterManagementResult.StatusCode... codes) {
    assertThat(actual.getStatusCode()).isIn((Object[]) codes);
    return this;
  }

  public ClusterManagementGetResultAssert<T, R> containsStatusMessage(String statusMessage) {
    assertThat(actual.getStatusMessage()).contains(statusMessage);
    return this;
  }

  public EntityGroupInfo<T, R> getResult() {
    return actual.getResult().getGroups().get(0);
  }

  public ObjectAssert<T> hasConfiguration() {
    return assertThat(getActual().getResult().getGroups().get(0).getConfiguration());
  }

  public ListAssert<R> hasRuntimeInfos() {
    return assertThat(getActual().getResult().getGroups().get(0).getRuntimeInfo());
  }

  public static <T extends AbstractConfiguration<R>, R extends RuntimeInfo> ClusterManagementGetResultAssert<T, R> assertManagementGetResult(
      ClusterManagementGetResult<T, R> result) {
    return new ClusterManagementGetResultAssert<>(result, ClusterManagementGetResultAssert.class);
  }

  public ClusterManagementGetResult<T, R> getActual() {
    return actual;
  }
}
