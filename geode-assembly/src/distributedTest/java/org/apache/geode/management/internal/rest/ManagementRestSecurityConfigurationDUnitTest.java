package org.apache.geode.management.internal.rest;

import static org.apache.geode.test.junit.rules.HttpResponseAssert.assertResponse;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GeodeDevRestClient;
import org.apache.geode.test.junit.rules.MemberStarterRule;

public class ManagementRestSecurityConfigurationDUnitTest {

  private MemberVM locator;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Test
  public void testWithSecurityManager() {
    this.locator = cluster.startLocatorVM(0,
        x -> x.withHttpService().withSecurityManager(SimpleSecurityManager.class));
    GeodeDevRestClient client =
        new GeodeDevRestClient("/management", "localhost", locator.getHttpPort(), false);

    // Unsecured no credentials
    assertResponse(client.doGet("/swagger-ui.html", null, null)).hasStatusCode(200);
    assertResponse(client.doGet("/v1/api-docs", null, null)).hasStatusCode(200);
    assertResponse(client.doGet("/v1/ping", null, null)).hasStatusCode(200); // fails with 403

    // unsecured with credentials
    assertResponse(client.doGet("/swagger-ui.html", "cluster", "cluster")).hasStatusCode(200);
    assertResponse(client.doGet("/v1/api-docs", "cluster", "cluster")).hasStatusCode(200);
    assertResponse(client.doGet("/v1/ping", "cluster", "cluster")).hasStatusCode(200);

    // secured with credentials
    assertResponse(client.doGet("/v1/regions", "cluster", "cluster")).hasStatusCode(200);

    // secured no credentials
    assertResponse(client.doGet("/v1/regions", null, null)).hasStatusCode(401);
  }

  @Test
  public void testWithoutSecurityManager() {
    this.locator = cluster.startLocatorVM(1, MemberStarterRule::withHttpService);
    GeodeDevRestClient client =
        new GeodeDevRestClient("/management", "localhost", locator.getHttpPort(), false);

    // Unsecured no credentials
    assertResponse(client.doGet("/swagger-ui.html", null, null)).hasStatusCode(200);
    assertResponse(client.doGet("/v1/api-docs", null, null)).hasStatusCode(200);
    assertResponse(client.doGet("/v1/ping", null, null)).hasStatusCode(200);

    // unsecured with credentials
    assertResponse(client.doGet("/swagger-ui.html", "cluster", "cluster")).hasStatusCode(200);
    assertResponse(client.doGet("/v1/api-docs", "cluster", "cluster")).hasStatusCode(200);
    assertResponse(client.doGet("/v1/ping", "cluster", "cluster")).hasStatusCode(200);

    // secured with credentials
    assertResponse(client.doGet("/v1/regions", "cluster", "cluster")).hasStatusCode(200);

    // secured no credentials
    assertResponse(client.doGet("/v1/regions", null, null)).hasStatusCode(200);
  }
}
