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
package org.apache.geode.management;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.ManagementConstants;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;

@Category(DistributedTest.class)
public class CompositeTypeTestDUnitTest extends ManagementTestBase {

  public CompositeTypeTestDUnitTest() {
    super();
    // TODO Auto-generated constructor stub
  }

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  private static ObjectName objectName;

  @Category(FlakyTest.class) // GEODE-1492
  @Test
  public void testCompositeTypeGetters() throws Exception {

    initManagement(false);
    String member = getMemberId(managedNode1);
    member = MBeanJMXAdapter.makeCompliantName(member);

    registerMBeanWithCompositeTypeGetters(managedNode1, member);


    checkMBeanWithCompositeTypeGetters(managingNode, member);

  }


  /**
   * Creates a Local region
   *
   * @param vm reference to VM
   */
  protected void registerMBeanWithCompositeTypeGetters(VM vm, final String memberID)
      throws Exception {
    SerializableRunnable regMBean =
        new SerializableRunnable("Register CustomMBean with composite Type") {
          public void run() {
            GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
            SystemManagementService service = (SystemManagementService) getManagementService();

            try {
              ObjectName objectName = new ObjectName("GemFire:service=custom,type=composite");
              CompositeTestMXBean mbean = new CompositeTestMBean();
              objectName = service.registerMBean(mbean, objectName);
              service.federate(objectName, CompositeTestMXBean.class, false);
            } catch (MalformedObjectNameException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            } catch (NullPointerException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }



          }
        };
    vm.invoke(regMBean);
  }


  /**
   * Creates a Local region
   *
   * @param vm reference to VM
   */
  protected void checkMBeanWithCompositeTypeGetters(VM vm, final String memberID) throws Exception {
    SerializableRunnable checkMBean =
        new SerializableRunnable("Check CustomMBean with composite Type") {
          public void run() {
            GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
            final SystemManagementService service =
                (SystemManagementService) getManagementService();

            try {
              final ObjectName objectName =
                  new ObjectName("GemFire:service=custom,type=composite,member=" + memberID);

              Wait.waitForCriterion(new WaitCriterion() {
                public String description() {
                  return "Waiting for Composite Type MBean";
                }

                public boolean done() {
                  CompositeTestMXBean bean =
                      service.getMBeanInstance(objectName, CompositeTestMXBean.class);
                  boolean done = (bean != null);
                  return done;
                }

              }, ManagementConstants.REFRESH_TIME * 4, 500, true);


              CompositeTestMXBean bean =
                  service.getMBeanInstance(objectName, CompositeTestMXBean.class);

              CompositeStats listData = bean.listCompositeStats();

              System.out.println("connectionStatsType = " + listData.getConnectionStatsType());
              System.out.println("connectionsOpened = " + listData.getConnectionsOpened());
              System.out.println("connectionsClosed = " + listData.getConnectionsClosed());
              System.out.println("connectionsAttempted = " + listData.getConnectionsAttempted());
              System.out.println("connectionsFailed = " + listData.getConnectionsFailed());

              CompositeStats getsData = bean.getCompositeStats();
              System.out.println("connectionStatsType = " + getsData.getConnectionStatsType());
              System.out.println("connectionsOpened = " + getsData.getConnectionsOpened());
              System.out.println("connectionsClosed = " + getsData.getConnectionsClosed());
              System.out.println("connectionsAttempted = " + getsData.getConnectionsAttempted());
              System.out.println("connectionsFailed = " + getsData.getConnectionsFailed());

              CompositeStats[] arrayData = bean.getCompositeArray();
              Integer[] intArrayData = bean.getIntegerArray();
              Thread.sleep(2 * 60 * 1000);
            } catch (MalformedObjectNameException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            } catch (NullPointerException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            } catch (InterruptedException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }



          }
        };
    vm.invoke(checkMBean);
  }


}
