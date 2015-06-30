package com.gemstone.gemfire.distributed;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.gemfire.support.SpringContextBootstrappingInitializer;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.distributed.AbstractLauncher.Status;
import com.gemstone.gemfire.distributed.ServerLauncher.Builder;
import com.gemstone.gemfire.internal.process.ProcessType;

/**
 * Extracted from ServerLauncherLocalJUnitTest.
 * 
 * @author John Blum
 * @author Kirk Lund
 */
public class ServerLauncherWithSpringJUnitTest extends AbstractServerLauncherJUnitTestCase {

  @Before
  public final void setUpServerLauncherWithSpringTest() throws Exception {
    disconnectFromDS();
    System.setProperty(ProcessType.TEST_PREFIX_PROPERTY, getUniqueName()+"-");
  }

  @After
  public final void tearDownServerLauncherWithSpringTest() throws Exception {    
    disconnectFromDS();
    SpringContextBootstrappingInitializer.getApplicationContext().close();
  }

  // NOTE make sure bugs like Trac #51201 never happen again!!!
  @Test
  public void testBootstrapGemFireServerWithSpring() throws Throwable {
    this.launcher = new Builder()
      .setDisableDefaultServer(true)
      .setForce(true)
      .setMemberName(getUniqueName())
      .setSpringXmlLocation("spring/spring-gemfire-context.xml")
      .build();

    assertNotNull(this.launcher);

    try {
      assertEquals(Status.ONLINE, this.launcher.start().getStatus());

      waitForServerToStart(this.launcher);

      Cache cache = this.launcher.getCache();

      assertNotNull(cache);
      assertTrue(cache.getCopyOnRead());
      assertEquals(0.95f, cache.getResourceManager().getCriticalHeapPercentage(), 0);
      assertEquals(0.85f, cache.getResourceManager().getEvictionHeapPercentage(), 0);
      assertFalse(cache.getPdxIgnoreUnreadFields());
      assertTrue(cache.getPdxPersistent());
      assertTrue(cache.getPdxReadSerialized());
    }
    catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      assertNull(this.launcher.getCache());
    }
    catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }
}
