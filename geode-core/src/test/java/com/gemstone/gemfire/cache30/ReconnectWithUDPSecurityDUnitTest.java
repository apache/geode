package com.gemstone.gemfire.cache30;

import java.util.Properties;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;

public class ReconnectWithUDPSecurityDUnitTest extends ReconnectDUnitTest{

  public ReconnectWithUDPSecurityDUnitTest() {
    super();
  }
  
  @Override
  protected void addDSProps(Properties p) {
    p.setProperty(DistributionConfig.SECURITY_CLIENT_DHALGO_NAME, "AES:128");
  }
}
