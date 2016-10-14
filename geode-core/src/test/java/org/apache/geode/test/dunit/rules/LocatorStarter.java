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

package org.apache.geode.test.dunit.rules;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.jayway.awaitility.Awaitility;
import org.junit.rules.ExternalResource;

import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalLocator;

/**
 * This is a rule to start up a locator in your current VM. It's useful for your
 * Integration Tests.
 *
 * If you need a rule to start a server/locator in different VM for Distribution tests,
 * You should use LocatorServerStartupRule
 *
 * This rule does not have a before(), because you may choose to start a locator in different time
 * of your tests. You may choose to use this class not as a rule or use it in your own rule,
 * (see LocatorServerStartupRule) you will need to call after() manually in that case.
 */

public class LocatorStarter extends ExternalResource implements Serializable {

  public InternalLocator locator;

  private Properties properties;

  public LocatorStarter(Properties properties){
    this.properties = properties;
  }

  public void startLocator() throws Exception{
    if (!properties.containsKey(MCAST_PORT)) {
      properties.setProperty(MCAST_PORT, "0");
    }
    locator = (InternalLocator) Locator.startLocatorAndDS(0, null, properties);
    int locatorPort = locator.getPort();
    locator.resetInternalLocatorFileNamesWithCorrectPortNumber(locatorPort);

    if (locator.getConfig().getEnableClusterConfiguration()) {
      Awaitility.await().atMost(65, TimeUnit.SECONDS).until(() -> assertTrue(locator.isSharedConfigurationRunning()));
    }
  }

  @Override
  public void after(){
    if(locator!=null){
      locator.stop();
    }
  }
}
