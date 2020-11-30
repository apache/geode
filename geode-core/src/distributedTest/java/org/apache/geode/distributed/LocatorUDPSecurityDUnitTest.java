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
package org.apache.geode.distributed;

import static org.apache.geode.distributed.ConfigurationProperties.MEMBER_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_UDP_DHALGO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category(MembershipTest.class)
public class LocatorUDPSecurityDUnitTest extends LocatorDUnitTest {

  @Override
  protected void addDSProps(Properties p) {
    super.addDSProps(p);
    p.setProperty(SECURITY_UDP_DHALGO, "AES:128");
  }


  @Test
  public void testLocatorWithUDPSecurityButServer() {
    String locators = hostName + "[" + port1 + "]";

    startLocatorWithSomeBasicProperties(vm0, port1);

    try {
      Properties props = getBasicProperties(locators);
      props.setProperty(MEMBER_TIMEOUT, "1000");
      system = getConnectedDistributedSystem(props);
      fail("Should not have reached this line, it should have caught the exception.");
    } catch (GemFireConfigException e) {
      assertThat(e.getCause().getMessage()).contains("Rejecting findCoordinatorRequest");
    }
  }
}
