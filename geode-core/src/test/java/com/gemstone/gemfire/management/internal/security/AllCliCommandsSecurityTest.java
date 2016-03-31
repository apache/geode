/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.security;

import com.gemstone.gemfire.cache.operations.OperationContext.OperationCode;
import com.gemstone.gemfire.cache.operations.OperationContext.Resource;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.management.MemberMXBean;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertNull;

@Category(IntegrationTest.class)
public class AllCliCommandsSecurityTest {
  private static int jmxManagerPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

  private MemberMXBean bean;

  private static class Permission {
    private final Resource resource;
    private final OperationCode operationCode;

    Permission(Resource resource, OperationCode operationCode) {
      this.resource = resource;
      this.operationCode = operationCode;
    }

    @Override
    public String toString() {
      String result = resource.toString() + ":" + operationCode.toString();
      return result;
    }
  }

  private static final Permission ASYNC_EVENT_QUEUE_MANAGE = new Permission(Resource.ASYNC_EVENT_QUEUE, OperationCode.MANAGE);
  private static final Permission ASYNC_EVENT_QUEUE_LIST = new Permission(Resource.ASYNC_EVENT_QUEUE, OperationCode.LIST);
  private static final Permission CLUSTER_CONFIGURATION_STATUS = new Permission(Resource.CLUSTER_CONFIGURATION, OperationCode.STATUS);
  private static final Permission DISKSTORE_MANAGE = new Permission(Resource.DISKSTORE, OperationCode.MANAGE);
  private static final Permission DISKSTORE_LIST = new Permission(Resource.DISKSTORE, OperationCode.LIST);
  private static final Permission DISTRIBUTED_SYSTEM_ALL = new Permission(Resource.DISTRIBUTED_SYSTEM, OperationCode.ALL);
  private static final Permission DISTRIBUTED_SYSTEM_LIST = new Permission(Resource.DISTRIBUTED_SYSTEM, OperationCode.LIST);
  private static final Permission DISTRIBUTED_SYSTEM_MANAGE = new Permission(Resource.DISTRIBUTED_SYSTEM, OperationCode.MANAGE);
  private static final Permission GATEWAY_MANAGE = new Permission(Resource.GATEWAY, OperationCode.MANAGE);
  private static final Permission GATEWAY_LIST = new Permission(Resource.GATEWAY, OperationCode.LIST);
  private static final Permission PDX_MANAGE = new Permission(Resource.PDX, OperationCode.MANAGE);

  private Map<String, Permission> commandPermission = new HashMap<>();


  public AllCliCommandsSecurityTest() {

    // Config Commands
    commandPermission.put("status cluster-config-service", CLUSTER_CONFIGURATION_STATUS);

    // Diskstore Commands
    commandPermission.put("backup disk-store --dir=foo", DISKSTORE_MANAGE);
    commandPermission.put("list disk-stores", DISKSTORE_LIST);
    commandPermission.put("create disk-store --name=foo --dir=bar", DISKSTORE_MANAGE);
    commandPermission.put("compact disk-store --name=foo", DISKSTORE_MANAGE);
    commandPermission.put("compact offline-disk-store --name=foo --disk-dirs=bar", DISKSTORE_MANAGE);
    commandPermission.put("upgrade offline-disk-store --name=foo --disk-dirs=bar", DISKSTORE_MANAGE);
    commandPermission.put("describe disk-store --name=foo --member=baz", DISKSTORE_LIST);
    commandPermission.put("revoke missing-disk-store --id=foo", DISKSTORE_MANAGE);
    commandPermission.put("show missing-disk-stores", DISKSTORE_MANAGE);
    commandPermission.put("describe offline-disk-store --name=foo --disk-dirs=bar", DISKSTORE_LIST);
    commandPermission.put("export offline-disk-store --name=foo --disk-dirs=bar --dir=baz", DISKSTORE_MANAGE);
    commandPermission.put("validate offline-disk-store --name=foo --disk-dirs=bar", DISKSTORE_MANAGE);
//    commandPermission.put("alter offline-disk-store --name=foo --region=xyz --disk-dirs=bar", DISKSTORE_MANAGE);
    commandPermission.put("destroy disk-store --name=foo", DISKSTORE_MANAGE);

    // Misc Commands
    commandPermission.put("change loglevel --loglevel=severe --member=server1", DISTRIBUTED_SYSTEM_MANAGE);
    commandPermission.put("export logs --dir=data/logs", DISTRIBUTED_SYSTEM_LIST);
    commandPermission.put("export stack-traces --file=stack.txt", DISTRIBUTED_SYSTEM_LIST);
    commandPermission.put("gc", DISTRIBUTED_SYSTEM_MANAGE);
    commandPermission.put("netstat --member=server1", DISTRIBUTED_SYSTEM_MANAGE);
    commandPermission.put("show dead-locks --file=deadlocks.txt", DISTRIBUTED_SYSTEM_LIST);
    commandPermission.put("show log --member=locator1 --lines=5", DISTRIBUTED_SYSTEM_LIST);
    commandPermission.put("show metrics", DISTRIBUTED_SYSTEM_LIST);
//    commandPermission.put("shutdown", DISTRIBUTED_SYSTEM_MANAGE);

    // PDX Commands
    commandPermission.put("configure pdx --read-serialized=true", PDX_MANAGE);
    commandPermission.put("pdx rename --old=com.gemstone --new=com.pivotal --disk-store=ds1 --disk-dirs=/diskDir1", PDX_MANAGE);

    // Queue Commands
    commandPermission.put("create async-event-queue --id=myAEQ --listener=myApp.myListener", ASYNC_EVENT_QUEUE_MANAGE);
    commandPermission.put("list async-event-queues", ASYNC_EVENT_QUEUE_LIST);

    // Shell Commands
    commandPermission.put("connect", DISTRIBUTED_SYSTEM_ALL);
    commandPermission.put("debug --state=on", DISTRIBUTED_SYSTEM_ALL);
    commandPermission.put("describe connection", DISTRIBUTED_SYSTEM_ALL);
    commandPermission.put("echo --string=\"Hello World!\"", DISTRIBUTED_SYSTEM_ALL);
    commandPermission.put("encrypt password --password=value", DISTRIBUTED_SYSTEM_ALL);
    commandPermission.put("version", DISTRIBUTED_SYSTEM_ALL);
    commandPermission.put("sleep", DISTRIBUTED_SYSTEM_ALL);
    commandPermission.put("sh ls", DISTRIBUTED_SYSTEM_ALL);
    commandPermission.put("disconnect", DISTRIBUTED_SYSTEM_ALL);

    // WAN Commands
    commandPermission.put("create gateway-sender --id=sender1 --remote-distributed-system-id=2", GATEWAY_MANAGE);
    commandPermission.put("start gateway-sender --id=sender1", GATEWAY_MANAGE);
    commandPermission.put("pause gateway-sender --id=sender1", GATEWAY_MANAGE);
    commandPermission.put("resume gateway-sender --id=sender1", GATEWAY_MANAGE);
    commandPermission.put("stop gateway-sender --id=sender1", GATEWAY_MANAGE);
    commandPermission.put("load-balance gateway-sender --id=sender1", GATEWAY_MANAGE);
    commandPermission.put("list gateways", GATEWAY_LIST);
    commandPermission.put("create gateway-receiver", GATEWAY_MANAGE);
    commandPermission.put("start gateway-receiver", GATEWAY_MANAGE);
    commandPermission.put("stop gateway-receiver", GATEWAY_MANAGE);
    commandPermission.put("status gateway-receiver", GATEWAY_LIST);
  }

  @ClassRule
  public static JsonAuthorizationCacheStartRule serverRule = new JsonAuthorizationCacheStartRule(
      jmxManagerPort, "cacheServer.json");

  @Rule
  public MBeanServerConnectionRule connectionRule = new MBeanServerConnectionRule(jmxManagerPort);

  @Before
  public void setUp() throws Exception {
    bean = connectionRule.getProxyMBean(MemberMXBean.class);
  }

  @Test
  @JMXConnectionConfiguration(user = "adminUser", password = "1234567")
  public void testAdminUser() throws Exception {
    for (String cmd : commandPermission.keySet()) {
      try {
        bean.processCommand(cmd);
      } catch (Throwable t) {
        assertNull(String.format("Error evaluating command: '%s'", cmd), t);
      }
    }
  }

  // dataUser has all the permissions granted, but not to region2 (only to region1)
  @Test
  @JMXConnectionConfiguration(user = "dataUser", password = "1234567")
  public void testNoAccess(){
    for (Map.Entry<String, Permission> e : commandPermission.entrySet()) {
      try {
        assertThatThrownBy(() -> bean.processCommand(e.getKey()))
            .hasMessageStartingWith("Access Denied: Not authorized for " + e.getValue())
            .isInstanceOf(SecurityException.class);
      } catch (Throwable t) {
        assertNull(String.format("Command should have failed: '%s'", e.getKey(), t));
      }
    }

  }

}
