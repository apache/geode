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

public class Tomcat7DeltaSessionManagerJUnitTest extends DeltaSessionManagerJUnitTest {
  private Pipeline pipeline;

  @Before
  public void setup() {
    manager = spy(new Tomcat7DeltaSessionManager());
    initTest();
    pipeline = mock(Pipeline.class);
  }

  @Test
  public void startInternalsucceedsInitialRun()
      throws LifecycleException, IOException, ClassNotFoundException {
    doNothing().when((Tomcat7DeltaSessionManager) manager).startInternalBase();
    doReturn(true).when(manager).isCommitValveEnabled();
    doReturn(cache).when(manager).getAnyCacheInstance();
    doReturn(true).when((GemFireCacheImpl) cache).isClient();
    doNothing().when(manager).initSessionCache();
    doReturn(pipeline).when(manager).getPipeline();

    // Unit testing for load is handled in the parent DeltaSessionManagerJUnitTest class
    doNothing().when(manager).load();

    doNothing().when((Tomcat7DeltaSessionManager) manager)
        .setLifecycleState(LifecycleState.STARTING);

    assertThat(manager.started).isFalse();
    ((Tomcat7DeltaSessionManager) manager).startInternal();
    assertThat(manager.started).isTrue();
    verify((Tomcat7DeltaSessionManager) manager).setLifecycleState(LifecycleState.STARTING);
  }

  @Test
  public void startInternalDoesNotReinitializeManagerOnSubsequentCalls()
      throws LifecycleException, IOException, ClassNotFoundException {
    doNothing().when((Tomcat7DeltaSessionManager) manager).startInternalBase();
    doReturn(true).when(manager).isCommitValveEnabled();
    doReturn(cache).when(manager).getAnyCacheInstance();
    doReturn(true).when((GemFireCacheImpl) cache).isClient();
    doNothing().when(manager).initSessionCache();
    doReturn(pipeline).when(manager).getPipeline();

    // Unit testing for load is handled in the parent DeltaSessionManagerJUnitTest class
    doNothing().when(manager).load();

    doNothing().when((Tomcat7DeltaSessionManager) manager)
        .setLifecycleState(LifecycleState.STARTING);

    assertThat(manager.started).isFalse();
    ((Tomcat7DeltaSessionManager) manager).startInternal();

    // Verify that various initialization actions were performed
    assertThat(manager.started).isTrue();
    verify(manager).initializeSessionCache();
    verify((Tomcat7DeltaSessionManager) manager).setLifecycleState(LifecycleState.STARTING);

    // Rerun startInternal
    ((Tomcat7DeltaSessionManager) manager).startInternal();

    // Verify that the initialization actions were still only performed one time
    verify(manager).initializeSessionCache();
    verify((Tomcat7DeltaSessionManager) manager).setLifecycleState(LifecycleState.STARTING);
  }

  @Test
  public void stopInternal() throws LifecycleException, IOException {
    doNothing().when((Tomcat7DeltaSessionManager) manager).startInternalBase();
    doNothing().when((Tomcat7DeltaSessionManager) manager).destroyInternalBase();
    doReturn(true).when(manager).isCommitValveEnabled();

    // Unit testing for unload is handled in the parent DeltaSessionManagerJUnitTest class
    doNothing().when(manager).unload();

    doNothing().when((Tomcat7DeltaSessionManager) manager)
        .setLifecycleState(LifecycleState.STOPPING);

    ((Tomcat7DeltaSessionManager) manager).stopInternal();

    assertThat(manager.started).isFalse();
    verify((Tomcat7DeltaSessionManager) manager).setLifecycleState(LifecycleState.STOPPING);
  }

}
