package com.gemstone.gemfire.internal;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.management.GemFireProperties;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.MemberMXBean;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Test that DistributionConfigImpl handles SSL options correctly.
 * 
 */
@Category(IntegrationTest.class)
public class SSLConfigIntegrationJUnitTest {

  @Test
  public void test51531() {
    Cache mCache = new CacheFactory().set("mcast-port", "0").set("jmx-manager", "true").create();
    ManagementService mService = ManagementService.getManagementService(mCache);
    MemberMXBean mMemberBean = mService.getMemberMXBean();
    GemFireProperties mGemFireProperties = mMemberBean.listGemFireProperties();

    assertTrue(mGemFireProperties.isServerSSLRequireAuthentication());
    assertTrue(mGemFireProperties.isClusterSSLRequireAuthentication());
    assertTrue(mGemFireProperties.isGatewaySSLRequireAuthentication());
    assertTrue(mGemFireProperties.isJmxManagerSSLRequireAuthentication());
    mCache.close();
  }
}
