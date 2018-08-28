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
package org.apache.geode.internal.cache.tier;

import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.internal.cache.tier.DeltaPropagationFailureRegressionTest.DeltaFailure.FROM_DELTA;
import static org.apache.geode.internal.cache.tier.DeltaPropagationFailureRegressionTest.DeltaFailure.TO_DELTA;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.EOFException;
import java.io.Serializable;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DeltaSerializationException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.tier.sockets.DeltaEOFException;
import org.apache.geode.internal.cache.tier.sockets.FaultyDelta;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Test delta propagation for faulty delta implementation
 *
 * <p>
 * TRAC #40396: Need better error messages (and unit tests) when users code delta propagation
 * methods incorrectly
 *
 * @since GemFire 6.1
 */
@Category(ClientServerTest.class)
@SuppressWarnings("serial")
public class DeltaPropagationFailureRegressionTest implements Serializable {

  private static final int PUT_COUNT = 10;

  private String regionName;

  private VM server1;
  private VM server2;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() throws Exception {
    server1 = getVM(0);
    server2 = getVM(2);

    regionName = getClass().getSimpleName() + "_" + testName.getMethodName();

    server1.invoke(() -> createServerCache());
    server2.invoke(() -> createServerCache());
  }

  /**
   * Verifies error handling when trying to read more in fromDelta then what sent by toDelta which
   * throws EOFException
   */
  @Test
  public void fromDeltaThrowsDeltaSerializationExceptionWithCauseEofException() {
    addIgnoredException(EOFException.class);

    Throwable thrown = server1.invoke(() -> catchThrowable(() -> putDelta(FROM_DELTA)));

    assertThat(thrown).isInstanceOf(DeltaSerializationException.class)
        .hasMessageContaining("deserializing delta bytes").hasCauseInstanceOf(EOFException.class);
  }

  /**
   * Verifies error handling when reading incorrect order from toDelta which throws
   * ArrayIndexOutOfBoundsException
   */
  @Test
  public void toDeltaThrowsArrayIndexOutOfBoundsException() {
    addIgnoredException(ArrayIndexOutOfBoundsException.class);

    Throwable thrown = server1.invoke(() -> catchThrowable(() -> putDelta(TO_DELTA)));

    assertThat(thrown).isInstanceOf(ArrayIndexOutOfBoundsException.class);
  }

  private int createServerCache() throws Exception {
    cacheRule.createCache();

    RegionFactory<?, ?> regionFactory = cacheRule.getCache().createRegionFactory(REPLICATE);
    regionFactory.setCloningEnabled(false);

    regionFactory.create(regionName);

    CacheServer cacheServer = cacheRule.getCache().addCacheServer();
    cacheServer.setPort(0);
    cacheServer.start();
    return cacheServer.getPort();
  }

  private void putDelta(DeltaFailure deltaFailure) {
    Region<String, FaultyDelta> region = cacheRule.getCache().getRegion(regionName);
    deltaFailure.putInRegion(region);
  }

  enum DeltaFailure {
    FROM_DELTA(new DeltaEOFException()), TO_DELTA(new FaultyDelta());

    private final FaultyDelta value;

    DeltaFailure(FaultyDelta value) {
      this.value = value;
    }

    void putInRegion(Region<String, FaultyDelta> region) {
      for (int i = 0; i < PUT_COUNT; i++) {
        value.setIntVal(i);
        value.setBigObj(new byte[] {(byte) (i + 3), (byte) (i + 3)});
        region.put("key", value);
      }
    }

  }
}
