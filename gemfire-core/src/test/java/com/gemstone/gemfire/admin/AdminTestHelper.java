package com.gemstone.gemfire.admin;

import static org.junit.Assert.*;
import com.gemstone.gemfire.distributed.internal.DistributionManager;

public class AdminTestHelper {
  private AdminTestHelper() {}
  
  public static void checkEnableAdministrationOnly(boolean v, boolean expectException) {
    boolean origIsDedicatedAdminVM = DistributionManager.isDedicatedAdminVM;
    if (expectException) {
      try {
        AdminDistributedSystemFactory.setEnableAdministrationOnly(v);
        fail("expected IllegalStateException");
      } catch (IllegalStateException expected) {
        assertEquals(origIsDedicatedAdminVM, DistributionManager.isDedicatedAdminVM);
      } finally {
        DistributionManager.isDedicatedAdminVM = origIsDedicatedAdminVM;
      }
    } else {
      try {
        AdminDistributedSystemFactory.setEnableAdministrationOnly(v);
        assertEquals(v, DistributionManager.isDedicatedAdminVM);
      } finally {
        DistributionManager.isDedicatedAdminVM = origIsDedicatedAdminVM;
      }
    }
  }
}
