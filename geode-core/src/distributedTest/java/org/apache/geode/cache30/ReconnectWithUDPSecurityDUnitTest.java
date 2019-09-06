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
package org.apache.geode.cache30;

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_UDP_DHALGO;

import java.util.Properties;

import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category({MembershipTest.class, ClientServerTest.class})
public class ReconnectWithUDPSecurityDUnitTest extends ReconnectDUnitTest {

  @Override
  protected void addDSProps(Properties p) {
    p.setProperty(SECURITY_UDP_DHALGO, "AES:128");
  }
}
