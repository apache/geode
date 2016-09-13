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
package com.gemstone.gemfire.internal.cache;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;

import java.util.Properties;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.DistributedTest;

/**
 * Test all the PartitionedRegion api calls when ConserveSockets is set to false
 *
 * @since GemFire 5.0
 * @see com.gemstone.gemfire.distributed.DistributedSystem#setThreadsSocketPolicy(boolean)
 */
@Category(DistributedTest.class)
public class PartitionedRegionAPIConserveSocketsFalseDUnitTest extends PartitionedRegionAPIDUnitTest {

  @Override
  public Properties getDistributedSystemProperties() {
    Properties ret = new Properties();
    ret.setProperty(CONSERVE_SOCKETS, "false");
    return ret; 
  }
}
