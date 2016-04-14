package com.gemstone.gemfire.management.internal.security;

import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.management.internal.cli.HeadlessGfsh;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class GfshCommandsSecurityTest {
  private static int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
  private static int jmxPort = ports[0];
  private static int httpPort = ports[1];

  private HeadlessGfsh gfsh = null;

  @ClassRule
  public static JsonAuthorizationCacheStartRule serverRule = new JsonAuthorizationCacheStartRule(
      jmxPort, httpPort, "cacheServer.json");

  @Rule
  public GfshShellConnectionRule gfshConnection = new GfshShellConnectionRule(jmxPort, httpPort, false);


  @Before
  public void before(){
    gfsh = gfshConnection.getGfsh();
  }

 // @Test(expected=SecurityException.class)
  @Test
  @JMXConnectionConfiguration(user = "data-admin", password = "1234567")
  public void test() throws Exception {

  }

}
