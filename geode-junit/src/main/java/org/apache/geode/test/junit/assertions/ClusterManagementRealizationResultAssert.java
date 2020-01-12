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

import org.apache.geode.management.api.ClusterManagementRealizationResult;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.RealizationResult;

public class ClusterManagementRealizationResultAssert
    extends
    AbstractAssert<ClusterManagementRealizationResultAssert, ClusterManagementRealizationResult> {
  public ClusterManagementRealizationResultAssert(
      ClusterManagementRealizationResult clusterManagementResult, Class<?> selfType) {
    super(clusterManagementResult, selfType);
  }

  public ClusterManagementRealizationResultAssert isSuccessful() {
    assertThat(actual.isSuccessful()).isTrue();
    return this;
  }

  public ClusterManagementRealizationResultAssert failed() {
    assertThat(actual.isSuccessful()).isFalse();
    return this;
  }

  public ClusterManagementRealizationResultAssert hasStatusCode(
      ClusterManagementResult.StatusCode... codes) {
    assertThat(actual.getStatusCode()).isIn((Object[]) codes);
    return this;
  }

  public ClusterManagementRealizationResultAssert containsStatusMessage(String statusMessage) {
    assertThat(actual.getStatusMessage()).contains(statusMessage);
    return this;
  }

  public ListAssert<RealizationResult> hasMemberStatus() {
    return assertThat(actual.getMemberStatuses());
  }

  public List<RealizationResult> getMemberStatus() {
    return actual.getMemberStatuses();
  }

  public static ClusterManagementRealizationResultAssert assertManagementResult(
      ClusterManagementRealizationResult result) {
    return new ClusterManagementRealizationResultAssert(result,
        ClusterManagementRealizationResultAssert.class);
  }

  public ClusterManagementResult getActual() {
    return actual;
  }
}
