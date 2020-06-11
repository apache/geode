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

package org.apache.geode.modules.session.catalina;


import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleState;
import org.apache.catalina.Pipeline;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.internal.cache.GemFireCacheImpl;

public class Tomcat9DeltaSessionManagerTest extends AbstractDeltaSessionManagerTest {
  private Pipeline pipeline;

  @Before
  public void setup() {
    manager = spy(new Tomcat9DeltaSessionManager());
    initTest();
    pipeline = mock(Pipeline.class);
    doReturn(context).when(manager).getContext();
  }

  @Test
  public void startInternalSucceedsInitialRun()
      throws LifecycleException, IOException, ClassNotFoundException {
    doNothing().when((Tomcat9DeltaSessionManager) manager).startInternalBase();
    doReturn(true).when(manager).isCommitValveEnabled();
    doReturn(cache).when(manager).getAnyCacheInstance();
    doReturn(true).when((GemFireCacheImpl) cache).isClient();
    doNothing().when(manager).initSessionCache();
    doReturn(pipeline).when(manager).getPipeline();

    // Unit testing for load is handled in the parent DeltaSessionManagerJUnitTest class
    doNothing().when(manager).load();

    doNothing().when((Tomcat9DeltaSessionManager) manager)
        .setLifecycleState(LifecycleState.STARTING);

    assertThat(manager.started).isFalse();
    ((Tomcat9DeltaSessionManager) manager).startInternal();
    assertThat(manager.started).isTrue();
    verify((Tomcat9DeltaSessionManager) manager).setLifecycleState(LifecycleState.STARTING);
  }

  @Test
  public void startInternalDoesNotReinitializeManagerOnSubsequentCalls()
      throws LifecycleException, IOException, ClassNotFoundException {
    doNothing().when((Tomcat9DeltaSessionManager) manager).startInternalBase();
    doReturn(true).when(manager).isCommitValveEnabled();
    doReturn(cache).when(manager).getAnyCacheInstance();
    doReturn(true).when((GemFireCacheImpl) cache).isClient();
    doNothing().when(manager).initSessionCache();
    doReturn(pipeline).when(manager).getPipeline();

    // Unit testing for load is handled in the parent DeltaSessionManagerJUnitTest class
    doNothing().when(manager).load();

    doNothing().when((Tomcat9DeltaSessionManager) manager)
        .setLifecycleState(LifecycleState.STARTING);

    assertThat(manager.started).isFalse();
    ((Tomcat9DeltaSessionManager) manager).startInternal();

    // Verify that various initialization actions were performed
    assertThat(manager.started).isTrue();
    verify(manager).initializeSessionCache();
    verify((Tomcat9DeltaSessionManager) manager).setLifecycleState(LifecycleState.STARTING);

    // Rerun startInternal
    ((Tomcat9DeltaSessionManager) manager).startInternal();

    // Verify that the initialization actions were still only performed one time
    verify(manager).initializeSessionCache();
    verify((Tomcat9DeltaSessionManager) manager).setLifecycleState(LifecycleState.STARTING);
  }

  @Test
  public void stopInternal() throws LifecycleException, IOException {
    doNothing().when((Tomcat9DeltaSessionManager) manager).startInternalBase();
    doNothing().when((Tomcat9DeltaSessionManager) manager).destroyInternalBase();
    doReturn(true).when(manager).isCommitValveEnabled();

    // Unit testing for unload is handled in the parent DeltaSessionManagerJUnitTest class
    doNothing().when(manager).unload();

    doNothing().when((Tomcat9DeltaSessionManager) manager)
        .setLifecycleState(LifecycleState.STOPPING);

    ((Tomcat9DeltaSessionManager) manager).stopInternal();

    assertThat(manager.started).isFalse();
    verify((Tomcat9DeltaSessionManager) manager).setLifecycleState(LifecycleState.STOPPING);
  }

}
