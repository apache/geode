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
package org.apache.geode.management.internal.cli.commands;

import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class JPMCQueryClassCastExceptionFailureDUnitTest {
  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  private MemberVM locator, server;

  @Test
  public void classCastExceptionWhileExecutingQuery() throws Exception {
    locator =
        cluster.startLocatorVM(0/*
                                 * ,
                                 * locator ->
                                 * locator.withHttpService().withProperties(locatorProperties())
                                 */);
    Properties props = new Properties();
    props.setProperty("cache-xml-file", "/Users/nnag/Downloads/RDC_Cache_And_GFD/rdc_cache.xml");
    int locPort = locator.getPort();
    server = cluster.startServerVM(-1,
        s -> s.withConnectionToLocator(locPort).withName("server1").withProperties(props));
    gfsh.connectAndVerify(locator);
    // upload the snapshot
    gfsh.execute(
        "import data --region=/product --file=/Users/nnag/Downloads/RDC_Cache_And_GFD/product_2019_10_15.gfd --member=server1");
    // execute the query
    // "select productId, productCodes['GMI'], contractSize from /product where contractSize = null
    // and productCodes['GMI'] in (select distinct b.productCodes['GMI'] from /product b where
    // b.contractSize != null and b.status='ACTIVE')"

    gfsh.execute(
        "query --query=\"<trace> select  productId, productCodes['GMI'], contractSize from /product where contractSize = null and productCodes['GMI'] in (select  distinct b.productCodes['GMI'] from /product b where b.contractSize != null and b.status='ACTIVE') LIMIT 2000\"");
  }

  // private Properties locatorProperties() {
  // int jmxPort = AvailablePortHelper.getRandomAvailableTCPPort();
  // Properties props = new Properties();
  // props.setProperty(MCAST_PORT, "0");
  // props.setProperty(LOG_LEVEL, "fine");
  // props.setProperty(SERIALIZABLE_OBJECT_FILTER, SERIALIZATION_FILTER);
  // props.setProperty(ConfigurationProperties.JMX_MANAGER_HOSTNAME_FOR_CLIENTS, "localhost");
  // props.setProperty(ConfigurationProperties.JMX_MANAGER_PORT, "" + jmxPort);
  //
  // return props;
  // }
}
