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
package org.apache.geode.internal.cache;

import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.internal.lang.SystemPropertyHelper.EARLY_ENTRY_EVENT_SERIALIZATION;
import static org.apache.geode.internal.lang.SystemPropertyHelper.GEODE_PREFIX;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.DataSerializable;
import org.apache.geode.ToDataException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;

@SuppressWarnings("serial")
public class BrokenSerializationConsistencyRegressionTest implements Serializable {

  private static final String REGION_NAME = "replicateRegion";
  private static final String REGION_NAME2 = "replicateRegion2";
  private static final String KEY = "key";

  private VM vm0;

  private transient FailsToDataSerialize valueFailsToSerialize;
  private transient FailsToPdxSerialize pdxValueFailsToSerialize;
  private transient String stringValue;
  private transient PdxValue pdxValue;
  private transient byte[] bytesValue;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Before
  public void setUpAll() {
    vm0 = getVM(0);

    System.setProperty(GEODE_PREFIX + EARLY_ENTRY_EVENT_SERIALIZATION, "true");
    createReplicateRegions();

    vm0.invoke(() -> {
      System.setProperty(GEODE_PREFIX + EARLY_ENTRY_EVENT_SERIALIZATION, "true");
      createReplicateRegions();
    });

    valueFailsToSerialize = new FailsToDataSerialize();
    pdxValueFailsToSerialize = new FailsToPdxSerialize();

    stringValue = "hello world";
    pdxValue = new PdxValue();
    bytesValue = new byte[] {0, 1, 2};
  }

  @Test
  public void pdxValuePdxSerializes() {
    Region<String, PdxValue> region = cacheRule.getCache().getRegion(REGION_NAME);
    region.put(KEY, pdxValue);

    vm0.invoke(() -> {
      Region<String, PdxValue> regionOnVm0 = cacheRule.getCache().getRegion(REGION_NAME);
      assertThat(regionOnVm0.get(KEY)).isInstanceOf(PdxValue.class);
    });
  }

  @Test
  public void bytesValueDataSerializes() {
    Region<String, byte[]> region = cacheRule.getCache().getRegion(REGION_NAME);
    region.put(KEY, bytesValue);

    vm0.invoke(() -> {
      Region<String, byte[]> regionOnVm0 = cacheRule.getCache().getRegion(REGION_NAME);
      assertThat(regionOnVm0.get(KEY)).isNotNull().isInstanceOf(byte[].class);
    });
  }

  @Test
  public void failureToDataSerializeFailsToPropagate() {
    Region<String, DataSerializable> region = cacheRule.getCache().getRegion(REGION_NAME);
    Throwable caughtException = catchThrowable(() -> region.put(KEY, valueFailsToSerialize));

    assertThat(caughtException).isInstanceOf(ToDataException.class);
    assertThat(caughtException.getCause()).isInstanceOf(IOException.class)
        .hasMessage("FailsToSerialize");
    assertThat(region.get(KEY)).isNull();

    vm0.invoke(() -> {
      Region<String, DataSerializable> regionOnVm0 = cacheRule.getCache().getRegion(REGION_NAME);
      assertThat(regionOnVm0.get(KEY)).isNull();
    });
  }

  @Test
  public void failureToDataSerializeFailsToPropagateInTransaction() {
    Region<String, DataSerializable> region = cacheRule.getCache().getRegion(REGION_NAME);
    Region<String, String> region2 = cacheRule.getCache().getRegion(REGION_NAME2);
    TXManagerImpl txManager = cacheRule.getCache().getTxManager();
    txManager.begin();
    region2.put(KEY, stringValue);
    region.put(KEY, valueFailsToSerialize);
    Throwable caughtException = catchThrowable(() -> txManager.commit());

    assertThat(caughtException).isInstanceOf(ToDataException.class);
    assertThat(caughtException.getCause()).isInstanceOf(IOException.class)
        .hasMessage("FailsToSerialize");
    assertThat(region.get(KEY)).isNull();
    assertThat(region2.get(KEY)).isNull();

    vm0.invoke(() -> {
      Region<String, DataSerializable> regionOnVm0 = cacheRule.getCache().getRegion(REGION_NAME);
      Region<String, String> region2OnVm0 = cacheRule.getCache().getRegion(REGION_NAME2);
      assertThat(regionOnVm0.get(KEY)).isNull();
      assertThat(region2OnVm0.get(KEY)).isNull();
    });
  }

  @Test
  public void failureToPdxSerializeFails() {
    Region<String, PdxSerializable> region = cacheRule.getCache().getRegion(REGION_NAME);
    Throwable caughtException = catchThrowable(() -> region.put(KEY, pdxValueFailsToSerialize));

    assertThat(caughtException).isInstanceOf(ToDataException.class);
    assertThat(caughtException.getCause()).isInstanceOf(UncheckedIOException.class);
    assertThat(caughtException.getCause().getCause()).isInstanceOf(IOException.class)
        .hasMessage("FailsToSerialize");
    assertThat(region.get(KEY)).isNull();

    vm0.invoke(() -> {
      Region<String, PdxSerializable> regionOnVm0 = cacheRule.getCache().getRegion(REGION_NAME);
      assertThat(regionOnVm0.get(KEY)).isNull();
    });
  }

  private void createReplicateRegions() {
    RegionFactory<?, ?> regionFactory = cacheRule.getOrCreateCache().createRegionFactory(REPLICATE);
    regionFactory.create(REGION_NAME);
    regionFactory.create(REGION_NAME2);
  }

  private static class FailsToDataSerialize implements DataSerializable {

    @Override
    public void toData(DataOutput out) throws IOException {
      throw new IOException("FailsToSerialize");
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      // nothing
    }
  }

  private static class FailsToPdxSerialize implements PdxSerializable {

    @Override
    public void toData(PdxWriter writer) {
      throw new UncheckedIOException(new IOException("FailsToSerialize"));
    }

    @Override
    public void fromData(PdxReader reader) {
      // nothing
    }
  }

  public static class PdxValue implements PdxSerializable {

    @Override
    public void toData(PdxWriter writer) {
      // nothing
    }

    @Override
    public void fromData(PdxReader reader) {
      // nothing
    }
  }
}
