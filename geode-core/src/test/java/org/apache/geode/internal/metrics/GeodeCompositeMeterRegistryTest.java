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
package org.apache.geode.internal.metrics;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.logging.log4j.Logger;
import org.junit.Test;

public class GeodeCompositeMeterRegistryTest {

  @Test
  public void close_closesAllOwnedResources() throws Exception {
    AutoCloseable resource1 = mock(AutoCloseable.class);
    AutoCloseable resource2 = mock(AutoCloseable.class);

    GeodeCompositeMeterRegistry registry = new GeodeCompositeMeterRegistry(resource1, resource2);

    registry.close();

    verify(resource1).close();
    verify(resource2).close();
  }

  @Test
  public void close_logsException_ifResourceCloseThrows() throws Exception {
    AutoCloseable throwingResource = mock(AutoCloseable.class);
    Exception thrownByResource = new Exception("thrown by resource.close()");

    doThrow(thrownByResource).when(throwingResource).close();

    Logger logger = mock(Logger.class);
    GeodeCompositeMeterRegistry registry =
        new GeodeCompositeMeterRegistry(logger, throwingResource);

    registry.close();

    verify(logger).warn(any(String.class), same(thrownByResource));
  }
}
