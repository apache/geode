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

import static org.apache.geode.pdx.internal.TypeRegistrationStatistics.ENUM_CREATED;
//import static org.apache.geode.pdx.internal.TypeRegistrationStatistics.ENUM_DEFINED;
import static org.apache.geode.pdx.internal.TypeRegistrationStatistics.SIZE;
import static org.apache.geode.pdx.internal.TypeRegistrationStatistics.TYPE_CREATED;
//import static org.apache.geode.pdx.internal.TypeRegistrationStatistics.TYPE_DEFINED;
import static org.apache.geode.test.dunit.Disconnect.disconnectAllFromDS;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.statistics.SuppliableStatistics;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;

@SuppressWarnings("serial")
public class TypeRegistrationDistributedTest implements Serializable {

  private VM vm0;
  private VM vm1;

  @ClassRule
  public static DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Before
  public void setUp() throws Exception {
    vm0 = getVM(0);
    vm1 = getVM(1);
  }

  @After
  public void tearDown() throws Exception {
    disconnectAllFromDS();
  }

  @Test
  public void pdxInstanceWithEnumIncrementsStats() {
    pdxWithEnumIncrementsStats(this::createPdxInstanceWithEnum);
  }

  @Test
  public void pdxSerializableWithEnumIncrementsStats() {
    pdxWithEnumIncrementsStats(this::createPdxSerializableWithEnum);
  }

  private void pdxWithEnumIncrementsStats(final SerializableRunnableIF pdxGenerator) {
    vm0.invoke(() -> {
      final InternalCache cache = cacheRule.getOrCreateCache();
      final SuppliableStatistics statistics = getStatistics(cache);
      statistics.updateSuppliedValues();
//      assertThat(statistics.getLong(TYPE_DEFINED)).isEqualTo(0);
//      assertThat(statistics.getLong(ENUM_DEFINED)).isEqualTo(0);
      assertThat(statistics.getLong(TYPE_CREATED)).isEqualTo(0);
      assertThat(statistics.getLong(ENUM_CREATED)).isEqualTo(0);
      assertThat(statistics.getLong(SIZE)).isEqualTo(0);
      pdxGenerator.run();
      statistics.updateSuppliedValues();
//      assertThat(statistics.getLong(TYPE_DEFINED)).isEqualTo(1);
//      assertThat(statistics.getLong(ENUM_DEFINED)).isEqualTo(1);
      assertThat(statistics.getLong(TYPE_CREATED)).isEqualTo(1);
      assertThat(statistics.getLong(ENUM_CREATED)).isEqualTo(1);
      assertThat(statistics.getLong(SIZE)).isEqualTo(2);
      pdxGenerator.run();
      statistics.updateSuppliedValues();
//      assertThat(statistics.getLong(TYPE_DEFINED)).isEqualTo(1);
//      assertThat(statistics.getLong(ENUM_DEFINED)).isEqualTo(1);
      assertThat(statistics.getLong(TYPE_CREATED)).isEqualTo(1);
      assertThat(statistics.getLong(ENUM_CREATED)).isEqualTo(1);
      assertThat(statistics.getLong(SIZE)).isEqualTo(2);
    });
    vm1.invoke(() -> {
      final InternalCache cache = cacheRule.getOrCreateCache();
      final SuppliableStatistics statistics = getStatistics(cache);
      statistics.updateSuppliedValues();
//      assertThat(statistics.getLong(TYPE_DEFINED)).isEqualTo(0);
//      assertThat(statistics.getLong(ENUM_DEFINED)).isEqualTo(0);
      assertThat(statistics.getLong(TYPE_CREATED)).isEqualTo(0);
      assertThat(statistics.getLong(ENUM_CREATED)).isEqualTo(0);
      assertThat(statistics.getLong(SIZE)).isEqualTo(2);
      pdxGenerator.run();
      statistics.updateSuppliedValues();
//      assertThat(statistics.getLong(TYPE_DEFINED)).isEqualTo(1);
//      assertThat(statistics.getLong(ENUM_DEFINED)).isEqualTo(1);
      assertThat(statistics.getLong(TYPE_CREATED)).isEqualTo(0);
      assertThat(statistics.getLong(ENUM_CREATED)).isEqualTo(0);
      assertThat(statistics.getLong(SIZE)).isEqualTo(2);
//      pdxGenerator.run();
//      statistics.updateSuppliedValues();
//      assertThat(statistics.getLong(TYPE_DEFINED)).isEqualTo(1);
//      assertThat(statistics.getLong(ENUM_DEFINED)).isEqualTo(1);
//      assertThat(statistics.getLong(TYPE_CREATED)).isEqualTo(0);
//      assertThat(statistics.getLong(ENUM_CREATED)).isEqualTo(0);
//      assertThat(statistics.getLong(SIZE)).isEqualTo(2);
    });
  }

  private SuppliableStatistics getStatistics(final InternalCache cache) {
    return (SuppliableStatistics) cache.getDistributedSystem()
        .findStatisticsByTextId(PeerTypeRegistration.class.getSimpleName())[0];
  }

  private void createPdxInstanceWithEnum() {
    final InternalCache cache = cacheRule.getOrCreateCache();
    final PdxInstanceFactory pdxInstanceFactory =
        PdxInstanceFactoryImpl.newCreator("testPdxEnum", false, cache);
    pdxInstanceFactory.writeObject("enumField", MyEnum.ONE);
    pdxInstanceFactory.create();
  }

  private void createPdxSerializableWithEnum() {
    final DataOutput out = new DataOutputStream(new ByteArrayOutputStream());
    try {
      DataSerializer.writeObject(new MyPdx(), out);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public enum MyEnum {
    ONE, TWO
  }

  public static class MyPdx implements PdxSerializable {
    private MyEnum myEnum = MyEnum.ONE;

    @Override
    public void toData(final PdxWriter writer) {
      writer.writeObject("myEnum", myEnum);
    }

    @Override
    public void fromData(final PdxReader reader) {
      myEnum = (MyEnum) reader.readObject("myEnum");
    }
  }

}
