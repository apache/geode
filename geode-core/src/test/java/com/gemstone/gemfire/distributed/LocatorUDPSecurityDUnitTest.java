package com.gemstone.gemfire.distributed;

import java.util.Properties;

import org.junit.Test;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;

public class LocatorUDPSecurityDUnitTest extends LocatorDUnitTest{

  public LocatorUDPSecurityDUnitTest(String name) {
    super(name);
  }
  
  @Test
  public void testLoop() throws Exception {
    for(int i=0; i < 2; i++) {
      testMultipleLocatorsRestartingAtSameTime();
      tearDown();
      setUp();
    }
  }
  
  @Override
  protected void addDSProps(Properties p) {
    p.setProperty(DistributionConfig.SECURITY_CLIENT_DHALGO_NAME, "AES:128");
  }
}
