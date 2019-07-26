package org.apache.geode.management.internal.rest;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.junit.rules.GeodeHttpClientRule;
import org.apache.geode.test.junit.rules.LocatorStarterRule;
import org.apache.geode.test.junit.rules.RequiresGeodeHome;

public class CommandForwardIntegrationTest {
  @ClassRule
  public static LocatorStarterRule locator = new LocatorStarterRule().withHttpService().withAutoStart();

  @Rule
  public GeodeHttpClientRule client = new GeodeHttpClientRule(locator::getHttpPort);

  @Rule
  public RequiresGeodeHome requiresGeodeHome = new RequiresGeodeHome();

  @Test
  public void name() throws Exception {
    client.get("/management/v2/cli", "command", "list region");
  }
}