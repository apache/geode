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
package org.apache.geode.management.internal.configuration;

import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.SharedConfiguration;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.internal.JUnit3DistributedTestCase;

public class SharedConfigurationTestUtils {

  public static final SerializableRunnable cleanupLocator = new SerializableRunnable() {
    @Override
    public void run() {
      InternalLocator locator = InternalLocator.getLocator();
      if (locator != null) {
        SharedConfiguration sharedConfig = locator.getSharedConfiguration();
        if (sharedConfig != null) {
          sharedConfig.destroySharedConfiguration();
        }
        locator.stop();
      }
      JUnit3DistributedTestCase.disconnectAllFromDS();
    }
  };
}
