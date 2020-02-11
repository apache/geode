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
package org.apache.geode.pdx.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.internal.cache.InternalCache;

@RunWith(JUnitParamsRunner.class)
public class LonerTypeRegistrationTest {
  InternalCache mockCache = mock(InternalCache.class, RETURNS_DEEP_STUBS);
  LonerTypeRegistration spyRegistration;

  @Before
  public void setUp() {
    spyRegistration = spy(new LonerTypeRegistration(mockCache));
  }

  @Test
  @TestCaseName("{method} isClient={0}")
  @Parameters(method = "clientOrPeerTypeRegistrationParams")
  public void createTypeRegistrationCreatesCorrectDelegate(final boolean isClient,
      final Class<?> expectedTypeRegistration) {
    assertThat(spyRegistration.createTypeRegistration(isClient))
        .isInstanceOf(expectedTypeRegistration);
  }

  @Test
  @TestCaseName("{method} isClient={0}")
  @Parameters(method = "clientOrPeerTypeRegistrationParams")
  public void initializeRegistryWithNullArgumentSetsCorrectDelegateWhenDelegateIsNullAndDoesNotUpdateDelegateWhenDelegateIsNotNull(
      final boolean isClient, final Class<?> expectedTypeRegistration) {
    assertThat(spyRegistration.getDelegateClass()).isNull();
    when(mockCache.hasPool()).thenReturn(isClient);

    spyRegistration.initializeRegistry(null);

    verify(spyRegistration, times(1)).createTypeRegistration(isClient);
    assertThat(spyRegistration.getDelegateClass()).isEqualTo(expectedTypeRegistration);

    spyRegistration.initializeRegistry(null);

    verify(spyRegistration, times(1)).createTypeRegistration(isClient);
    assertThat(spyRegistration.getDelegateClass()).isEqualTo(expectedTypeRegistration);
  }

  @Test
  @TestCaseName("{method} isClient={0}")
  @Parameters(method = "clientOrPeerTypeRegistrationParams")
  public void initializeRegistryWithNonNullArgumentSetsCorrectDelegateWhenDelegateIsNullAndDoesNotUpdateDelegateWhenDelegateIsNotNull(
      final boolean isClient, final Class<?> expectedTypeRegistration) {
    assertThat(spyRegistration.getDelegateClass()).isNull();

    spyRegistration.initializeRegistry(isClient);

    verify(spyRegistration, times(1)).createTypeRegistration(isClient);
    assertThat(spyRegistration.getDelegateClass()).isEqualTo(expectedTypeRegistration);

    spyRegistration.initializeRegistry(isClient);

    verify(spyRegistration, times(1)).createTypeRegistration(isClient);
    assertThat(spyRegistration.getDelegateClass()).isEqualTo(expectedTypeRegistration);
  }

  @Test
  @TestCaseName("{method} isLoner={0}, isPdxPersistent={1}")
  @Parameters({
      "FASLE, FALSE, FALSE",
      "TRUE, FALSE, TRUE",
      "FASLE, TRUE, FALSE",
      "TRUE, TRUE, FALSE",
  })
  public void isIndeterminateLonerReturnsCorrectValue(final boolean internalDistSysIsLoner,
      final boolean isPdxPersistent, final boolean expectedResult) {
    when(mockCache.getInternalDistributedSystem().isLoner()).thenReturn(internalDistSysIsLoner);
    when(mockCache.getPdxPersistent()).thenReturn(isPdxPersistent);

    assertThat(LonerTypeRegistration.isIndeterminateLoner(mockCache)).isEqualTo(expectedResult);
  }

  @SuppressWarnings("unused")
  private Object[] clientOrPeerTypeRegistrationParams() {
    return new Object[] {
        new Object[] {true, ClientTypeRegistration.class},
        new Object[] {false, PeerTypeRegistration.class}
    };
  }
}
