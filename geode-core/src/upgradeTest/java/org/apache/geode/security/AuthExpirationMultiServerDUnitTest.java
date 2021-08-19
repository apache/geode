package org.apache.geode.security;

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ClientCacheRule;

@Category({SecurityTest.class})
public class AuthExpirationMultiServerDUnitTest implements Serializable {
  public static final String REPLICATE_REGION = "replicateRegion";
  public static final String PARTITION_REGION = "partitionRegion";
  private MemberVM locator, server1, server2;
  private int locatorPort;

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Before
  public void setup() {
    locator = lsRule.startLocatorVM(0, l -> l.withSecurityManager(ExpirableSecurityManager.class));
    locatorPort = locator.getPort();
    server1 = lsRule.startServerVM(1, s -> s.withSecurityManager(ExpirableSecurityManager.class)
        .withCredential("test", "test")
        .withRegion(RegionShortcut.REPLICATE, REPLICATE_REGION)
        .withRegion(RegionShortcut.PARTITION, PARTITION_REGION)
        .withConnectionToLocator(locatorPort));
    server2 = lsRule.startServerVM(2, s -> s.withSecurityManager(ExpirableSecurityManager.class)
        .withCredential("test", "test")
        .withRegion(RegionShortcut.REPLICATE, REPLICATE_REGION)
        .withRegion(RegionShortcut.PARTITION, PARTITION_REGION)
        .withConnectionToLocator(locatorPort));
  }

  @Test
  public void clientReAuthenticationWorksOnMultipleServers() throws Exception {
    UpdatableUserAuthInitialize.setUser("user1");
    clientCacheRule
        .withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
        .withPoolSubscription(true)
        .withLocatorConnection(locatorPort);
    clientCacheRule.createCache();
    Region<Object, Object> region1 = clientCacheRule.createProxyRegion(REPLICATE_REGION);
    Region<Object, Object> region2 = clientCacheRule.createProxyRegion(PARTITION_REGION);
    region1.put("0", "value0");
    region2.put("0", "value0");

    expireUserOnAllVms("user1");

    UpdatableUserAuthInitialize.setUser("user2");
    region1.put("1", "value1");
    region2.put("1", "value1");

    // locator only validates peer
    locator.invoke(() -> {
      Map<String, List<String>> authorizedOps = ExpirableSecurityManager.getAuthorizedOps();
      assertThat(authorizedOps.keySet().contains("test")).isTrue();
      assertThat(authorizedOps.keySet().size()).isEqualTo(1);
      Map<String, List<String>> unAuthorizedOps = ExpirableSecurityManager.getUnAuthorizedOps();
      assertThat(unAuthorizedOps.keySet().size()).isEqualTo(0);
    });

    // server1 is the first server, gets all the initial contact, authorirzation checks happens here
    server1.invoke(() -> {
      Map<String, List<String>> authorizedOps = ExpirableSecurityManager.getAuthorizedOps();
      assertThat(authorizedOps.get("user1")).asList().containsExactlyInAnyOrder(
          "DATA:WRITE:replicateRegion:0", "DATA:WRITE:partitionRegion:0");
      assertThat(authorizedOps.get("user2")).asList().containsExactlyInAnyOrder(
          "DATA:WRITE:replicateRegion:1", "DATA:WRITE:partitionRegion:1");
      Map<String, List<String>> unAuthorizedOps = ExpirableSecurityManager.getUnAuthorizedOps();
      assertThat(unAuthorizedOps.get("user1")).asList()
          .containsExactly("DATA:WRITE:replicateRegion:1");
    });

    // server2 performs no authorization checks
    server2.invoke(() -> {
      Map<String, List<String>> authorizedOps = ExpirableSecurityManager.getAuthorizedOps();
      Map<String, List<String>> unAuthorizedOps = ExpirableSecurityManager.getUnAuthorizedOps();
      assertThat(authorizedOps.size()).isEqualTo(0);
      assertThat(unAuthorizedOps.size()).isEqualTo(0);
    });

    MemberVM.invokeInEveryMember(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      Region<Object, Object> serverRegion1 = cache.getRegion(REPLICATE_REGION);
      assertThat(serverRegion1.size()).isEqualTo(2);
      Region<Object, Object> serverRegion2 = cache.getRegion(PARTITION_REGION);
      assertThat(serverRegion2.size()).isEqualTo(2);
    }, server1, server2);
  }

  private void expireUserOnAllVms(String user) {
    MemberVM.invokeInEveryMember(() -> {
      ExpirableSecurityManager.addExpiredUser(user);
    }, locator, server1, server2);
  }


}
