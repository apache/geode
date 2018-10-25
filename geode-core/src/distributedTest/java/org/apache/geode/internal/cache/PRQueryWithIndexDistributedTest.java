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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.test.dunit.DUnitEnv.get;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.stream.IntStream;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.DataSerializable;
import org.apache.geode.InternalGemFireException;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.IndexInvalidException;
import org.apache.geode.cache.query.internal.index.AbstractIndex;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.OQLIndexTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Extracted from {@link PRQueryDistributedTest}.
 */
@Category(OQLIndexTest.class)
@RunWith(JUnitParamsRunner.class)
@SuppressWarnings("serial")
public class PRQueryWithIndexDistributedTest implements Serializable {

  private String regionName;

  private VM vm0;
  private VM vm1;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() {
    vm0 = getVM(0);
    vm1 = getVM(1);

    regionName = getClass().getSimpleName() + "_" + testName.getMethodName();
  }

  @Test
  @Parameters({"OnLocalThrowsIndexInvalidException", "OnRemoteThrowsInternalGemFireException"})
  @TestCaseName("{method}({params})")
  public void failureToCreateIndex(VmThrows whichVmAndException) {
    VM vmToFailCreationOn = getVM(whichVmAndException.vmIndex());
    Class<? extends Exception> exceptionClass = whichVmAndException.exceptionClass();

    vm0.invoke(() -> {
      cacheRule.createCache();
      PartitionAttributesFactory paf = new PartitionAttributesFactory().setTotalNumBuckets(10);
      cacheRule.getCache().createRegionFactory(RegionShortcut.PARTITION)
          .setPartitionAttributes(paf.create()).create(regionName);
    });
    vm1.invoke(() -> {
      cacheRule.createCache();
      PartitionAttributesFactory paf = new PartitionAttributesFactory().setTotalNumBuckets(10);
      cacheRule.getCache().createRegionFactory(RegionShortcut.PARTITION)
          .setPartitionAttributes(paf.create()).create(regionName);
    });

    vm0.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(regionName);
      IntStream.range(1, 10)
          .forEach(i -> region.put(i, new NotDeserializableAsset(vmToFailCreationOn.getId())));
    });

    vm0.invoke(() -> {
      assertThatThrownBy(() -> cacheRule.getCache().getQueryService()
          .createHashIndex("ContractDocumentIndex", "document", SEPARATOR + regionName))
              .isInstanceOf(exceptionClass);
    });

    vm1.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(regionName);
      AbstractIndex index = (AbstractIndex) cacheRule.getCache().getQueryService().getIndex(region,
          "ContractDocumentIndex");
      if (index != null) {
        assertThat(index.isPopulated()).isFalse();
      }
    });
  }

  private enum VmThrows {
    OnLocalThrowsIndexInvalidException(0, IndexInvalidException.class),
    OnRemoteThrowsInternalGemFireException(1, InternalGemFireException.class);

    private final int vmIndex;
    private final Class<? extends Exception> exceptionClass;

    VmThrows(int vmIndex, Class<? extends Exception> exceptionClass) {
      this.vmIndex = vmIndex;
      this.exceptionClass = exceptionClass;
    }

    int vmIndex() {
      return vmIndex;
    }

    Class<? extends Exception> exceptionClass() {
      return exceptionClass;
    }
  }

  @SuppressWarnings("unused")
  private static class NotDeserializableAsset implements DataSerializable {

    private int disallowedPid;

    public NotDeserializableAsset() {
      // nothing
    }

    public NotDeserializableAsset(final int disallowedPid) {
      this.disallowedPid = disallowedPid;
    }

    @Override
    public void toData(final DataOutput out) throws IOException {
      out.writeInt(disallowedPid);

    }

    @Override
    public void fromData(final DataInput in) throws IOException, ClassNotFoundException {
      disallowedPid = in.readInt();
      if (disallowedPid == get().getPid()) {
        throw new IOException("Cannot deserialize");
      }
    }
  }
}
