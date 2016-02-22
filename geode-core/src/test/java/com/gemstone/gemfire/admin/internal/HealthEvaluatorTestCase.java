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
package com.gemstone.gemfire.admin.internal;

import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;

/**
 * Superclass of tests for the {@linkplain
 * com.gemstone.gemfire.admin.internal.AbstractHealthEvaluator health
 * evaluator} classes.
 *
 *
 * @since 3.5
 */
public abstract class HealthEvaluatorTestCase {

  /** The DistributedSystem used for this test */
  protected InternalDistributedSystem system;

  /**
   * Creates a "loner" <code>DistributedSystem</code> for this test.
   */
  @Before
  public void setUp() {
    Properties props = getProperties();
    system = (InternalDistributedSystem)
      DistributedSystem.connect(props);
  }

  /**
   * Closes the "loner" <code>DistributedSystem</code>
   */
  @After
  public void tearDown() {
    if (this.system != null) {
      this.system.disconnect();
    }

    this.system = null;
  }

  /**
   * Creates the <code>Properties</code> objects used to connect to
   * the distributed system.
   */
  protected Properties getProperties() {
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    props.setProperty("statistic-sampling-enabled", "true");

    return props;
  }

}
