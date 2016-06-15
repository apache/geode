package com.gemstone.gemfire.cache30;

import java.util.Properties;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
public class ReconnectWithUDPSecurityDUnitTest extends ReconnectDUnitTest{

  public ReconnectWithUDPSecurityDUnitTest() {
    super();
  }
  
  @Override
  protected void addDSProps(Properties p) {
    p.setProperty(SECURITY_UDP_DHALGO, "AES:128");
  }
}
