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

import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.internal.cache.InitialImageOperation.RequestImageMessage;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * This class tests that bucket regions can handle a failure of the GII target during GII.
 *
 * <p>
 * TRAC #41091: Missing primary detected after member forcefully disconnected from DS (underlying
 * InternalGemFireError: Trying to clear a bucket region that was not destroyed)
 */

public class BucketCreationGIIHARegressionTest extends CacheTestCase {

  private String uniqueName;

  private VM server1;
  private VM server2;
  private VM server3;

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() throws Exception {
    server1 = getHost(0).getVM(0);
    server2 = getHost(0).getVM(1);
    server3 = getHost(0).getVM(2);

    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
  }

  @After
  public void tearDown() throws Exception {
    DistributionMessageObserver.setInstance(null);
    invokeInEveryVM(() -> {
      DistributionMessageObserver.setInstance(null);
    });

    disconnectAllFromDS();
  }

  @Test
  public void bucketCreationLosesGiiTarget() {
    server1.invoke(() -> createRegion());
    server2.invoke(() -> createRegion());

    server3.invoke(() -> {
      PartitionAttributesFactory paf = new PartitionAttributesFactory();
      paf.setRedundantCopies(1);
      paf.setLocalMaxMemory(0);

      AttributesFactory af = new AttributesFactory();
      af.setPartitionAttributes(paf.create());

      Region<Integer, String> region = getCache().createRegion(uniqueName, af.create());

      region.put(0, "a");
    });
  }

  private void createRegion() {
    DistributionMessageObserver.setInstance(new MyDistributionMessageObserver());

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(1);

    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(paf.create());

    getCache().createRegion(uniqueName, af.create());
  }

  private class MyDistributionMessageObserver extends DistributionMessageObserver {

    @Override
    public void beforeProcessMessage(ClusterDistributionManager dm, DistributionMessage message) {
      if (message instanceof RequestImageMessage) {
        RequestImageMessage rim = (RequestImageMessage) message;
        Region region = getCache().getRegion(rim.regionPath);
        if (region instanceof BucketRegion) {
          getCache().close();
        }
      }
    }
  }
}
