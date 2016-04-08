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
package com.gemstone.gemfire;

import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

import java.util.*;

import org.junit.experimental.categories.Category;

/**
 * Tests the functionality of JOM {@link Statistics}.
 */
@Category(IntegrationTest.class)
public class LocalStatisticsJUnitTest extends StatisticsTestCase {

  /**
   * Returns a distributed system configured to not use shared
   * memory.
   */
  protected DistributedSystem getSystem() {
    if (this.system == null) {
      Properties props = new Properties();
      props.setProperty("statistic-sampling-enabled", "true");
      props.setProperty("statistic-archive-file", "StatisticsTestCase-localTest.gfs");
      props.setProperty("mcast-port", "0");
      props.setProperty("locators", "");
      props.setProperty(DistributionConfig.NAME_NAME, getName());
      this.system = DistributedSystem.connect(props);
    }

    return this.system;
  }
}
