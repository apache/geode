package com.gemstone.gemfire.distributed;

import java.util.Properties;

import org.junit.Test;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.*;

public class LocatorUDPSecurityDUnitTest extends LocatorDUnitTest{

  public LocatorUDPSecurityDUnitTest() {
  }
  
  @Test
  public void testLoop() throws Exception {
    for(int i=0; i < 1; i++) {
      testMultipleLocatorsRestartingAtSameTime();
      tearDown();
      setUp();
    }
  }
  
  @Override
  protected void addDSProps(Properties p) {
    p.setProperty(SECURITY_UDP_DHALGO, "AES:128");
  }
}
