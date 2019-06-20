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
package org.apache.geode.internal.cache.execute;

import static org.assertj.core.api.Assertions.assertThat;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.test.awaitility.GeodeAwaitility;

@RunWith(JUnitParamsRunner.class)
@SuppressWarnings("serial")
public class FunctionOnServerRetryDUnitTest extends FunctionRetryTestBase {

  @Test
  @Parameters({
      /*
       * haStatus | clientMetadataStatus | functionIdentifierType | retryAttempts | expectedCalls
       */
      "NOT_HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | -1 | 1",
      "NOT_HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | 0 | 1",
      "NOT_HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | 2 | 1",
      "NOT_HA | CLIENT_MISSING_METADATA | STRING | -1 | 1",
      "NOT_HA | CLIENT_MISSING_METADATA | STRING | 0 | 1",
      "NOT_HA | CLIENT_MISSING_METADATA | STRING | 2 | 1",
      "NOT_HA | CLIENT_HAS_METADATA | OBJECT_REFERENCE | -1 | 1",
      "NOT_HA | CLIENT_HAS_METADATA | STRING | -1 | 1",

      "HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | -1 | 3",
      "HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | 0 | 1",
      "HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | 2 | 3",
      "HA | CLIENT_MISSING_METADATA | STRING | -1 | 3",
      "HA | CLIENT_MISSING_METADATA | STRING | 0 | 1",
      "HA | CLIENT_MISSING_METADATA | STRING | 2 | 3",
      "HA | CLIENT_HAS_METADATA | OBJECT_REFERENCE | -1 | 3",
      "HA | CLIENT_HAS_METADATA | STRING | -1 | 3",
  })
  @TestCaseName("[{index}] {method}: {params}")
  public void testOnServer(final FunctionRetryTestBase.HAStatus haStatus,
      final FunctionRetryTestBase.ClientMetadataStatus clientMetadataStatus,
      final FunctionRetryTestBase.FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final int expectedCalls) throws Exception {

    Function function = testFunctionRetry(haStatus,
        clientMetadataStatus,
        FunctionRetryTestBase.ExecutionTarget.SERVER,
        functionIdentifierType,
        retryAttempts);

    GeodeAwaitility.await("Awaiting getNumberOfFunctionCalls isEqualTo expectedCalls")
        .untilAsserted(
            () -> assertThat(getNumberOfFunctionCalls(function.getId())).isEqualTo(expectedCalls));

  }

  @Test
  @Parameters({
      /*
       * haStatus | clientMetadataStatus | functionIdentifierType | retryAttempts | expectedCalls
       */
      "NOT_HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | -1 | 1",
      "NOT_HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | 0 | 1",
      "NOT_HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | 2 | 1",
      "NOT_HA | CLIENT_MISSING_METADATA | STRING | -1 | 1",
      "NOT_HA | CLIENT_MISSING_METADATA | STRING | 0 | 1",
      "NOT_HA | CLIENT_MISSING_METADATA | STRING | 2 | 1",
      "NOT_HA | CLIENT_HAS_METADATA | OBJECT_REFERENCE | -1 | 1",
      "NOT_HA | CLIENT_HAS_METADATA | STRING | -1 | 1",

      "HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | -1 | 3",
      "HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | 0 | 1",
      "HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | 2 | 3",
      "HA | CLIENT_MISSING_METADATA | STRING | -1 | 3",
      "HA | CLIENT_MISSING_METADATA | STRING | 0 | 1",
      "HA | CLIENT_MISSING_METADATA | STRING | 2 | 3",
      "HA | CLIENT_HAS_METADATA | OBJECT_REFERENCE | -1 | 3",
      "HA | CLIENT_HAS_METADATA | STRING | -1 | 3",
  })
  @TestCaseName("[{index}] {method}: {params}")
  public void testOnServerWithRegionService(final FunctionRetryTestBase.HAStatus haStatus,
      final FunctionRetryTestBase.ClientMetadataStatus clientMetadataStatus,
      final FunctionRetryTestBase.FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final int expectedCalls) throws Exception {

    Function function = testFunctionRetry(haStatus,
        clientMetadataStatus,
        FunctionRetryTestBase.ExecutionTarget.SERVER_REGION_SERVICE,
        functionIdentifierType,
        retryAttempts);

    GeodeAwaitility.await("Awaiting getNumberOfFunctionCalls isEqualTo expectedCalls")
        .untilAsserted(
            () -> assertThat(getNumberOfFunctionCalls(function.getId())).isEqualTo(expectedCalls));

  }
}
