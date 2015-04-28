package com.gemstone.gemfire.admin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.junit.IntegrationTest;

/**
 * Tests {@link com.gemstone.gemfire.admin.internal.DistributedSystemFactory}.
 * 
 * Created by extract two tests from DistributedSystemFactoryJUnitTest
 *
 * @author Kirk Lund
 */
@SuppressWarnings("deprecation")
@Category(IntegrationTest.class)
public class DistributedSystemFactoryIntegrationJUnitTest {

  @Test // IntegrationTest
  public void testGetDistributedSystem() throws Exception {
    String locators = "";
    
    DistributedSystemConfig config = 
        AdminDistributedSystemFactory.defineDistributedSystem();
    assertNotNull(config);

    config.setMcastPort(0);
    config.setLocators(locators); 
    
    AdminDistributedSystem distSys = 
        AdminDistributedSystemFactory.getDistributedSystem(config);
    assertNotNull(distSys);
    distSys.disconnect();
  }

  @Test // IntegrationTest
  public void testConnect() throws Exception {
    String locators = "";
    
    DistributedSystemConfig config = 
        AdminDistributedSystemFactory.defineDistributedSystem();
    assertNotNull(config);

    config.setMcastPort(0);
    config.setLocators(locators);
    config.setSystemName("testConnect");
    
    AdminDistributedSystem distSys = 
        AdminDistributedSystemFactory.getDistributedSystem(config);
    assertNotNull(distSys);
    boolean origIsDedicatedAdminVM = DistributionManager.isDedicatedAdminVM;
    try {
      AdminDistributedSystemFactory.setEnableAdministrationOnly(true);
      assertTrue(DistributionManager.isDedicatedAdminVM);
      distSys.connect();
      distSys.waitToBeConnected(30000);
      AdminTestHelper.checkEnableAdministrationOnly(true, true);
      AdminTestHelper.checkEnableAdministrationOnly(false, true);
      
      InternalDistributedSystem ids = InternalDistributedSystem.getAnyInstance();
      assertEquals("testConnect", ids.getName());
      
      distSys.disconnect();
      AdminTestHelper.checkEnableAdministrationOnly(false, false);
      AdminTestHelper.checkEnableAdministrationOnly(true, false);
    } finally {
      DistributionManager.isDedicatedAdminVM = origIsDedicatedAdminVM;
    }
  }
}
