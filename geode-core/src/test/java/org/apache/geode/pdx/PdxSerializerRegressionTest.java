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
package org.apache.geode.pdx;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SerializationTest;

@Category({DistributedTest.class, SerializationTest.class})
public class PdxSerializerRegressionTest {
  @Rule
  public transient ClusterStartupRule startupRule = new ClusterStartupRule();


  /**
   * A PdxSerializer may PDX-serialize any object it chooses, even JDK classes used in
   * Geode messaging. This caused initial image transfer for the _PR partition region
   * metadata to fail when pdx-read-serialized was set to true due to a class-cast
   * exception when a PdxInstance was returned by DataSerializer.readObject() instead
   * of the expected collection of PartitionConfig objects.
   */
  @Test
  public void serializerHandlingSetDoesNotBreakInitialImageTransferForPRRegistry() {
    MemberVM server1 = startupRule.startServerVM(1,
        x -> x.withConnectionToLocator(DistributedTestUtils.getLocatorPort())
            .withPDXReadSerialized().withPdxSerializer(new TestSerializer()));
    createRegion(server1, RegionShortcut.PARTITION);
    MemberVM server2 = startupRule.startServerVM(2,
        x -> x.withConnectionToLocator(DistributedTestUtils.getLocatorPort())
            .withPDXReadSerialized().withPdxSerializer(new TestSerializer()));
    createRegion(server2, RegionShortcut.PARTITION);
    assertThat((boolean) server1.invoke(() -> {
      return TestSerializer.serializerInvoked;
    })).isTrue();
    assertThat((boolean) server2.invoke(() -> {
      return TestSerializer.serializerInvoked;
    })).isTrue();
  }

  private void createRegion(MemberVM vm, RegionShortcut regionShortcut) {
    vm.invoke("create region", () -> {
      ClusterStartupRule.getCache().createRegionFactory(regionShortcut)
          .addAsyncEventQueueId("queue1").create("region");
    });
  }

  static class TestSerializer implements PdxSerializer, Serializable {
    static boolean serializerInvoked;

    @Override
    public boolean toData(Object o, PdxWriter out) {
      if (o.getClass().getName().equals("java.util.Collections$UnmodifiableSet")) {
        serializerInvoked = true;
        System.out.println("TestSerializer is serializing a Set");
        Set set = (Set<Object>) o;
        out.writeInt("size", set.size());
        int elementIndex = 0;
        for (Object element : set) {
          out.writeObject("element" + (elementIndex++), element);
        }
        return true;
      }
      return false;
    }

    @Override
    public Object fromData(Class<?> clazz, PdxReader in) {
      if (clazz.getName().equals("java.util.Collections$UnmodifiableSet")) {
        System.out.println("TestSerializer is deserializing a Set");
        int size = in.readInt("size");
        Set result = new HashSet(size);
        for (int elementIndex = 0; elementIndex < size; elementIndex++) {
          result.add(in.readObject("element" + elementIndex));
        }
        return Collections.unmodifiableSet(result);
      }
      return null;
    }
  }
}
