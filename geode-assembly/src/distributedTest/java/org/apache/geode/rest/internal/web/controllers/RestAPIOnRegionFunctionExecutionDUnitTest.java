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
package org.apache.geode.rest.internal.web.controllers;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.PartitionAttributesImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionTestHelper;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.rest.internal.web.RestFunctionTemplate;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.RestAPITest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

/**
 * Dunit Test to validate OnRegion function execution with REST APIs
 *
 * @since GemFire 8.0
 */

@Category({RestAPITest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class RestAPIOnRegionFunctionExecutionDUnitTest extends RestAPITestBase {

  private final String REPLICATE_REGION_NAME = "sampleRRegion";

  private final String PR_REGION_NAME = "samplePRRegion";

  @Parameterized.Parameter
  public String urlContext;

  @Parameterized.Parameters
  public static Collection<String> data() {
    return Arrays.asList("/geode", "/gemfire-api");
  }

  public RestAPIOnRegionFunctionExecutionDUnitTest() {
    super();
  }


  private void createPeer(DataPolicy policy) {
    Region<Object, Object> region = CacheFactory.getAnyInstance()
        .createRegionFactory()
        .setDataPolicy(policy)
        .setScope(Scope.DISTRIBUTED_ACK)
        .create(REPLICATE_REGION_NAME);

    assertThat(region).isNotNull();
  }

  private boolean createPeerWithPR() {
    RegionAttributes ra = PartitionedRegionTestHelper.createRegionAttrsForPR(0, 10);
    PartitionAttributesImpl pa = new PartitionAttributesImpl();
    pa.setAll(ra.getPartitionAttributes());
    pa.setTotalNumBuckets(17);

    Region<Object, Object> region = CacheFactory.getAnyInstance()
        .createRegionFactory()
        .setPartitionAttributes(pa)
        .create(PR_REGION_NAME);

    assertThat(region).isNotNull();
    return Boolean.TRUE;
  }

  private void populatePRRegion() {
    PartitionedRegion pr =
        (PartitionedRegion) CacheFactory.getAnyInstance().getRegion(PR_REGION_NAME);
    DistributedSystem.setThreadsSocketPolicy(false);

    for (int i = (pr.getTotalNumberOfBuckets() * 3); i > 0; i--) {
      pr.put("execKey-" + i, i + 1);
    }
    // Assert there is data in each bucket
    for (int bid = 0; bid < pr.getTotalNumberOfBuckets(); bid++) {
      assertThat(pr.getBucketKeys(bid).size() > 0).isTrue();
    }
  }

  private void populateRRRegion() {
    Region<String, Integer> region = CacheFactory.getAnyInstance().getRegion(REPLICATE_REGION_NAME);
    assertThat(region).isNotNull();

    final HashSet<String> testKeys = new HashSet<>();
    for (int i = 17 * 3; i > 0; i--) {
      testKeys.add("execKey-" + i);
    }
    int j = 0;
    for (final String testKey : testKeys) {
      region.put(testKey, j++);
    }

  }

  private void createCacheAndRegisterFunction() {
    restURLs.add(vm0.invoke("createCacheWithGroups",
        () -> createCacheWithGroups(vm0.getHost().getHostName(), null, urlContext)));
    restURLs.add(vm1.invoke("createCacheWithGroups",
        () -> createCacheWithGroups(vm1.getHost().getHostName(), null, urlContext)));
    restURLs.add(vm2.invoke("createCacheWithGroups",
        () -> createCacheWithGroups(vm2.getHost().getHostName(), null, urlContext)));
    restURLs.add(vm3.invoke("createCacheWithGroups",
        () -> createCacheWithGroups(vm3.getHost().getHostName(), null, urlContext)));

    vm0.invoke("registerFunction(new SampleFunction())",
        () -> FunctionService.registerFunction(new SampleFunction()));
    vm1.invoke("registerFunction(new SampleFunction())",
        () -> FunctionService.registerFunction(new SampleFunction()));
    vm2.invoke("registerFunction(new SampleFunction())",
        () -> FunctionService.registerFunction(new SampleFunction()));
    vm3.invoke("registerFunction(new SampleFunction())",
        () -> FunctionService.registerFunction(new SampleFunction()));
  }

  @Test
  public void testOnRegionExecutionWithReplicateRegion() {
    createCacheAndRegisterFunction();

    vm3.invoke("createPeer", () -> createPeer(DataPolicy.EMPTY));
    vm0.invoke("createPeer", () -> createPeer(DataPolicy.REPLICATE));
    vm1.invoke("createPeer", () -> createPeer(DataPolicy.REPLICATE));
    vm2.invoke("createPeer", () -> createPeer(DataPolicy.REPLICATE));

    vm3.invoke("populateRRRegion", this::populateRRRegion);

    CloseableHttpResponse response = executeFunctionThroughRestCall("SampleFunction",
        REPLICATE_REGION_NAME, null, null, null, null);
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
    assertThat(response.getEntity()).isNotNull();

    assertCorrectInvocationCount("SampleFunction", 1, vm0, vm1, vm2, vm3);

    // remove the expected exception
    restURLs.clear();
  }

  @Test
  public void testOnRegionExecutionWithPartitionRegion() {
    createCacheAndRegisterFunction();

    createPeersWithPR(vm0, vm1, vm2, vm3);

    vm3.invoke("populatePRRegion", this::populatePRRegion);

    CloseableHttpResponse response =
        executeFunctionThroughRestCall("SampleFunction", PR_REGION_NAME, null, null, null, null);
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
    assertThat(response.getEntity()).isNotNull();

    assertCorrectInvocationCount("SampleFunction", 4, vm0, vm1, vm2, vm3);

    restURLs.clear();
  }

  @Test
  public void testOnRegionWithFilterExecutionWithPartitionRegion() {
    createCacheAndRegisterFunction();

    createPeersWithPR(vm0, vm1, vm2, vm3);

    vm3.invoke("populatePRRegion", this::populatePRRegion);

    CloseableHttpResponse response =
        executeFunctionThroughRestCall("SampleFunction", PR_REGION_NAME, "key2", null, null, null);
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
    assertThat(response.getEntity()).isNotNull();

    assertCorrectInvocationCount("SampleFunction", 1, vm0, vm1, vm2, vm3);

    restURLs.clear();
  }

  private void createPeersWithPR(VM... vms) {
    for (final VM vm : vms) {
      vm.invoke("createPeerWithPR", this::createPeerWithPR);
    }
  }

  @Test
  public void testOnRegionWithFilterExecutionWithPartitionRegionJsonArgs() {
    createCacheAndRegisterFunction();

    createPeersWithPR(vm0, vm1, vm2, vm3);

    vm3.invoke("populatePRRegion", this::populatePRRegion);

    String jsonBody = "[" + "{\"@type\": \"double\",\"@value\": 210}"
        + ",{\"@type\":\"org.apache.geode.rest.internal.web.controllers.Item\","
        + "\"itemNo\":\"599\",\"description\":\"Part X Free on Bumper Offer\","
        + "\"quantity\":\"2\"," + "\"unitprice\":\"5\"," + "\"totalprice\":\"10.00\"}" + "]";

    CloseableHttpResponse response = executeFunctionThroughRestCall("SampleFunction",
        PR_REGION_NAME, null, jsonBody, null, null);
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
    assertThat(response.getEntity()).isNotNull();

    // Assert that only 1 node has executed the function.
    assertCorrectInvocationCount("SampleFunction", 4, vm0, vm1, vm2, vm3);

    jsonBody = "[" + "{\"@type\": \"double\",\"@value\": 220}"
        + ",{\"@type\":\"org.apache.geode.rest.internal.web.controllers.Item\","
        + "\"itemNo\":\"609\",\"description\":\"Part X Free on Bumper Offer\","
        + "\"quantity\":\"3\"," + "\"unitprice\":\"9\"," + "\"totalprice\":\"12.00\"}" + "]";

    vm0.invoke(() -> resetInvocationCount("SampleFunction"));
    vm1.invoke(() -> resetInvocationCount("SampleFunction"));
    vm2.invoke(() -> resetInvocationCount("SampleFunction"));
    vm3.invoke(() -> resetInvocationCount("SampleFunction"));

    response = executeFunctionThroughRestCall("SampleFunction", PR_REGION_NAME, "key2", jsonBody,
        null, null);
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
    assertThat(response.getEntity()).isNotNull();

    // Assert that only 1 node has executed the function.
    assertCorrectInvocationCount("SampleFunction", 1, vm0, vm1, vm2, vm3);

    restURLs.clear();
  }

  private static class SampleFunction extends RestFunctionTemplate {
    static final String Id = "SampleFunction";

    @Override
    @SuppressWarnings("unchecked")
    public void execute(FunctionContext context) {
      invocationCount++;
      if (context instanceof RegionFunctionContext) {
        RegionFunctionContext rfContext = (RegionFunctionContext) context;
        rfContext.getCache().getLogger()
            .info("Executing function :  SampleFunction.execute(hasResult=true) with filter: "
                + rfContext.getFilter() + "  " + rfContext);
        if (rfContext.getArguments() instanceof Boolean) {
          /* return rfContext.getArguments(); */
          if (hasResult()) {
            rfContext.getResultSender().lastResult(rfContext.getArguments());
          } else {
            rfContext.getCache().getLogger()
                .info("Executing function :  SampleFunction.execute(hasResult=false) " + rfContext);
            while (!rfContext.getDataSet().isDestroyed()) {
              rfContext.getCache().getLogger().info("For Bug43513 ");
              try {
                Thread.sleep(100);
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
              }
            }
          }
        } else if (rfContext.getArguments() instanceof String) {
          String key = (String) rfContext.getArguments();
          if (key.equals("TestingTimeOut")) { // for test
            // PRFunctionExecutionDUnitTest#testRemoteMultiKeyExecution_timeout
            try {
              Thread.sleep(2000);
            } catch (InterruptedException e) {
              rfContext.getCache().getLogger()
                  .warning("Got Exception : Thread Interrupted" + e);
            }
          }
          if (PartitionRegionHelper.isPartitionedRegion(rfContext.getDataSet())) {
            /*
             * return (Serializable)PartitionRegionHelper.getLocalDataForContext(
             * rfContext).get(key);
             */
            rfContext.getResultSender().lastResult(
                PartitionRegionHelper.getLocalDataForContext(rfContext).get(key));
          } else {
            rfContext.getResultSender().lastResult(rfContext.getDataSet().get(key));
          }
          /* return (Serializable)rfContext.getDataSet().get(key); */
        } else if (rfContext.getArguments() instanceof Set) {
          Set origKeys = (Set) rfContext.getArguments();
          ArrayList vals = new ArrayList();
          for (Object key : origKeys) {
            Object val = PartitionRegionHelper.getLocalDataForContext(rfContext).get(key);
            if (val != null) {
              vals.add(val);
            }
          }
          rfContext.getResultSender().lastResult(vals);
          /* return vals; */
        } else if (rfContext.getArguments() instanceof HashMap) {
          HashMap putData = (HashMap) rfContext.getArguments();
          for (Object o : putData.entrySet()) {
            Map.Entry me = (Map.Entry) o;
            rfContext.getDataSet().put(me.getKey(), me.getValue());
          }
          rfContext.getResultSender().lastResult(Boolean.TRUE);
        } else {
          rfContext.getResultSender().lastResult(Boolean.FALSE);
        }
      } else {
        if (hasResult()) {
          context.getResultSender().lastResult(Boolean.FALSE);
        } else {
          DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
          assertThat(ds).isNotNull();
          Logger logger = LogService.getLogger();
          logger.info("Executing in SampleFunction on Server : " + ds.getDistributedMember()
              + "with Context : " + context);
          while (ds.isConnected()) {
            logger.debug("Just executing function in infinite loop for Bug43513");
            try {
              Thread.sleep(250);
            } catch (InterruptedException e) {
              return;
            }
          }
        }
      }
    }

    @Override
    public String getId() {
      return Id;
    }

    @Override
    public boolean hasResult() {
      return true;
    }

    @Override
    public boolean optimizeForWrite() {
      return true;
    }

    @Override
    public boolean isHA() {
      return false;
    }
  }

}
