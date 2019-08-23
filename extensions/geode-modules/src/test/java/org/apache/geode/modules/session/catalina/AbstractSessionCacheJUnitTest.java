package org.apache.geode.modules.session.catalina;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.servlet.http.HttpSession;

import org.apache.juli.logging.Log;
import org.junit.Test;

import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.modules.util.RegionConfiguration;

public abstract class AbstractSessionCacheJUnitTest {

  protected String sessionRegionName = "sessionRegion";
  private String sessionRegionAttributesId = RegionShortcut.PARTITION.toString();
  private int nonDefaultMaxInactiveInterval = RegionConfiguration.DEFAULT_MAX_INACTIVE_INTERVAL+1;
  private boolean gatewayDeltaReplicationEnabled = true;
  private boolean gatewayReplicationEnabled = true;
  private boolean enableDebugListener = true;


  protected SessionManager sessionManager = mock(SessionManager.class);
  @SuppressWarnings("unchecked")
  protected Region<String, HttpSession> sessionRegion = mock(Region.class);
  protected DistributedSystem distributedSystem = mock(DistributedSystem.class);
  protected Log logger = mock(Log.class);
  protected Execution emptyExecution = mock(Execution.class);

  protected AbstractSessionCache sessionCache;

  @Test
  public void createRegionConfigurationSetsAppropriateValuesWithDefaultMaxInactiveInterval() {
    RegionConfiguration config = spy(new RegionConfiguration());
    doReturn(config).when(sessionCache).getNewRegionConfiguration();

    when(sessionManager.getRegionName()).thenReturn(sessionRegionName);
    when(sessionManager.getRegionAttributesId()).thenReturn(sessionRegionAttributesId);
    when(sessionManager.getMaxInactiveInterval()).thenReturn(RegionConfiguration.DEFAULT_MAX_INACTIVE_INTERVAL);
    when(sessionManager.getEnableGatewayDeltaReplication()).thenReturn(gatewayDeltaReplicationEnabled);
    when(sessionManager.getEnableGatewayReplication()).thenReturn(gatewayReplicationEnabled);
    when(sessionManager.getEnableDebugListener()).thenReturn(enableDebugListener);

    sessionCache.createRegionConfiguration();

    verify(config).setRegionName(sessionRegionName);
    verify(config).setRegionAttributesId(sessionRegionAttributesId);
    verify(config, times(0)).setMaxInactiveInterval(anyInt());
    verify(config, times(0)).setCustomExpiry(any(CustomExpiry.class));
    verify(config).setEnableGatewayDeltaReplication(gatewayDeltaReplicationEnabled);
    verify(config).setEnableGatewayReplication(gatewayReplicationEnabled);
    verify(config).setEnableDebugListener(enableDebugListener);
  }

  @Test
  public void createRegionConfigurationSetsAppropriateValuesWithNonDefaultMaxInactiveInterval() {
    RegionConfiguration config = spy(new RegionConfiguration());
    doReturn(config).when(sessionCache).getNewRegionConfiguration();

    when(sessionManager.getRegionName()).thenReturn(sessionRegionName);
    when(sessionManager.getRegionAttributesId()).thenReturn(sessionRegionAttributesId);
    when(sessionManager.getMaxInactiveInterval()).thenReturn(nonDefaultMaxInactiveInterval);
    when(sessionManager.getEnableGatewayDeltaReplication()).thenReturn(gatewayDeltaReplicationEnabled);
    when(sessionManager.getEnableGatewayReplication()).thenReturn(gatewayReplicationEnabled);
    when(sessionManager.getEnableDebugListener()).thenReturn(enableDebugListener);

    sessionCache.createRegionConfiguration();

    verify(config).setRegionName(sessionRegionName);
    verify(config).setRegionAttributesId(sessionRegionAttributesId);
    verify(config).setMaxInactiveInterval(nonDefaultMaxInactiveInterval);
    verify(config).setCustomExpiry(any(CustomExpiry.class));
    verify(config).setEnableGatewayDeltaReplication(gatewayDeltaReplicationEnabled);
    verify(config).setEnableGatewayReplication(gatewayReplicationEnabled);
    verify(config).setEnableDebugListener(enableDebugListener);
  }

  @Test
  public void destroySessionDoesNotThrowExceptionWhenGetOperatingRegionThrowsEntryNotFoundException() {
    EntryNotFoundException exception = new EntryNotFoundException("Entry not found.");
    String sessionId = "sessionId";
    //For Client/Server the operating Region is always the session Region, for peer to peer this is only true when
    //local caching is not enabled. For the purposes of this test the behavior is equivalent regardless of local
    //caching.
    when(sessionCache.getOperatingRegion()).thenReturn(sessionRegion);
    doThrow(exception).when(sessionRegion).destroy(sessionId);

    sessionCache.destroySession(sessionId);
    verify(sessionCache).getOperatingRegion();
  }
}
