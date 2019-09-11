package org.apache.geode.modules.session.catalina;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpSession;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.Statistics;
import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.internal.InternalClientCache;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.modules.session.catalina.callback.SessionExpirationCacheListener;
import org.apache.geode.modules.util.BootstrappingFunction;
import org.apache.geode.modules.util.CreateRegionFunction;
import org.apache.geode.modules.util.DebugCacheListener;
import org.apache.geode.modules.util.RegionConfiguration;
import org.apache.geode.modules.util.RegionStatus;
import org.apache.geode.modules.util.SessionCustomExpiry;
import org.apache.geode.modules.util.TouchPartitionedRegionEntriesFunction;
import org.apache.geode.modules.util.TouchReplicatedRegionEntriesFunction;

public class ClientServerSessionCacheJUnitTest extends AbstractSessionCacheJUnitTest {

  private List<RegionStatus> regionStatusResultList = new ArrayList<>();
  private ClientCache cache = mock(GemFireCacheImpl.class);
  private ResultCollector collector = mock(ResultCollector.class);
  private Statistics stats = mock(Statistics.class);
  @SuppressWarnings("unchecked")
  private ClientRegionFactory<String, HttpSession> regionFactory = mock(ClientRegionFactory.class);
  @SuppressWarnings("unchecked")
  private RegionAttributes<String, HttpSession> attributes = mock(RegionAttributes.class);

  @Before
  public void setUp() {
    sessionCache = spy(new ClientServerSessionCache(sessionManager, cache));
    doReturn(emptyExecution).when((ClientServerSessionCache) sessionCache)
        .getExecutionForFunctionOnServers();
    doReturn(emptyExecution).when((ClientServerSessionCache) sessionCache)
        .getExecutionForFunctionOnServersWithArguments(any());
    doReturn(emptyExecution).when((ClientServerSessionCache) sessionCache)
        .getExecutionForFunctionOnServerWithRegionConfiguration(any());
    doReturn(emptyExecution).when((ClientServerSessionCache) sessionCache)
        .getExecutionForFunctionOnRegionWithFilter(any());

    when(sessionManager.getLogger()).thenReturn(logger);
    when(sessionManager.getEnableLocalCache()).thenReturn(true);
    when(sessionManager.getRegionName()).thenReturn(sessionRegionName);
    when(sessionManager.getMaxInactiveInterval())
        .thenReturn(RegionConfiguration.DEFAULT_MAX_INACTIVE_INTERVAL);

    when(cache.getDistributedSystem()).thenReturn(distributedSystem);
    doReturn(regionFactory).when(cache)
        .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY_HEAP_LRU);
    when(((InternalClientCache) cache).isClient()).thenReturn(true);

    when(emptyExecution.execute(any(Function.class))).thenReturn(collector);
    when(emptyExecution.execute(any(String.class))).thenReturn(collector);

    when(collector.getResult()).thenReturn(regionStatusResultList);

    when(distributedSystem.createAtomicStatistics(any(), any())).thenReturn(stats);

    regionStatusResultList.clear();
    regionStatusResultList.add(RegionStatus.VALID);
  }

  @Test
  public void initializeSessionCacheSucceeds() {
    sessionCache.initialize();

    verify(emptyExecution).execute(any(BootstrappingFunction.class));
    verify(emptyExecution).execute(CreateRegionFunction.ID);
    verify(cache).createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY_HEAP_LRU);
    verify(regionFactory, times(0)).setStatisticsEnabled(true);
    verify(regionFactory, times(0)).setCustomEntryIdleTimeout(any(SessionCustomExpiry.class));
    verify(regionFactory, times(0)).addCacheListener(any(SessionExpirationCacheListener.class));
    verify(regionFactory).create(sessionRegionName);
  }

  @Test
  public void bootstrappingFunctionThrowsException() {
    FunctionException exception = new FunctionException();

    ResultCollector exceptionCollector = mock(ResultCollector.class);

    when(emptyExecution.execute(new BootstrappingFunction())).thenReturn(exceptionCollector);
    when(exceptionCollector.getResult()).thenThrow(exception);

    sessionCache.initialize();

    verify(logger).warn("Caught unexpected exception:", exception);
  }


  @Test
  public void createOrRetrieveRegionThrowsException() {
    RuntimeException exception = new RuntimeException();
    doThrow(exception).when((ClientServerSessionCache) sessionCache).createLocalSessionRegion();

    assertThatThrownBy(() -> sessionCache.initialize()).hasCause(exception)
        .isInstanceOf(IllegalStateException.class);

    verify(logger).fatal("Unable to create or retrieve region", exception);

  }

  @Test
  public void createRegionFunctionFailsOnServer() {
    ArgumentCaptor<String> stringCaptor = ArgumentCaptor.forClass(String.class);

    regionStatusResultList.clear();
    regionStatusResultList.add(RegionStatus.INVALID);

    assertThatThrownBy(() -> sessionCache.initialize()).isInstanceOf(IllegalStateException.class)
        .hasCauseInstanceOf(IllegalStateException.class).hasMessageContaining(
            "An exception occurred on the server while attempting to create or validate region named "
                + sessionRegionName
                + ". See the server log for additional details.");

    verify(logger).fatal(stringCaptor.capture(), any(Exception.class));
    assertThat(stringCaptor.getValue()).isEqualTo("Unable to create or retrieve region");
  }

  @Test
  public void nonDefaultMaxTimeoutIntervalSetsExpirationDetails() {
    // Setting the mocked return value of getMaxInactiveInterval to something distinctly not equal
    // to the default
    when(sessionManager.getMaxInactiveInterval())
        .thenReturn(RegionConfiguration.DEFAULT_MAX_INACTIVE_INTERVAL + 1);

    sessionCache.initialize();

    verify(regionFactory).setStatisticsEnabled(true);
    verify(regionFactory).setCustomEntryIdleTimeout(any(SessionCustomExpiry.class));
    verify(regionFactory).addCacheListener(any(SessionExpirationCacheListener.class));
  }

  @Test
  public void createLocalSessionRegionWithoutEnableLocalCache() {
    when(sessionManager.getEnableLocalCache()).thenReturn(false);
    doReturn(regionFactory).when(cache).createClientRegionFactory(ClientRegionShortcut.PROXY);
    when(regionFactory.create(sessionRegionName)).thenReturn(sessionRegion);

    sessionCache.initialize();

    verify(regionFactory).addCacheListener(any(SessionExpirationCacheListener.class));
    verify(sessionRegion).registerInterest("ALL_KEYS", InterestResultPolicy.KEYS);
  }

  @Test
  public void createOrRetrieveRegionWithNonNullSessionRegionDoesNotCreateRegion() {
    @SuppressWarnings("unchecked")
    CacheListener<String, HttpSession>[] cacheListeners =
        new CacheListener[] {new SessionExpirationCacheListener()};
    doReturn(sessionRegion).when(cache).getRegion(sessionRegionName);
    doReturn(attributes).when(sessionRegion).getAttributes();
    doReturn(cacheListeners).when(attributes).getCacheListeners();

    sessionCache.initialize();

    verify((ClientServerSessionCache) sessionCache, times(0)).createSessionRegionOnServers();
    verify((ClientServerSessionCache) sessionCache, times(0)).createLocalSessionRegion();
  }

  @Test
  public void createOrRetrieveRegionWithNonNullSessionRegionAndNoSessionExpirationCacheListenerCreatesListener() {
    @SuppressWarnings("unchecked")
    CacheListener<String, HttpSession>[] cacheListeners =
        new CacheListener[] {new DebugCacheListener()};
    @SuppressWarnings("unchecked")
    AttributesMutator<String, HttpSession> attributesMutator = mock(AttributesMutator.class);
    doReturn(sessionRegion).when(cache).getRegion(sessionRegionName);
    doReturn(attributes).when(sessionRegion).getAttributes();
    doReturn(cacheListeners).when(attributes).getCacheListeners();
    doReturn(attributesMutator).when(sessionRegion).getAttributesMutator();

    sessionCache.initialize();

    verify(attributesMutator).addCacheListener(any(SessionExpirationCacheListener.class));
  }

  @Test
  public void createOrRetrieveRegionWithNonNullSessionProxyRegionRegistersInterestForAllKeys() {
    @SuppressWarnings("unchecked")
    CacheListener<String, HttpSession>[] cacheListeners =
        new CacheListener[] {new SessionExpirationCacheListener()};
    doReturn(sessionRegion).when(cache).getRegion(sessionRegionName);
    doReturn(attributes).when(sessionRegion).getAttributes();
    doReturn(cacheListeners).when(attributes).getCacheListeners();
    when(attributes.getDataPolicy()).thenReturn(DataPolicy.EMPTY);

    sessionCache.initialize();

    verify(sessionRegion).registerInterest("ALL_KEYS", InterestResultPolicy.KEYS);
  }

  @Test
  public void touchSessionsInvokesPRFunctionForPRAndDoesNotThrowExceptionWhenFunctionDoesNotThrowException() {
    Set<String> sessionIds = new HashSet<>();

    when(sessionManager.getRegionAttributesId()).thenReturn(RegionShortcut.PARTITION.toString());

    sessionCache.touchSessions(sessionIds);

    verify(emptyExecution).execute(TouchPartitionedRegionEntriesFunction.ID);
  }

  @Test
  public void touchSessionsInvokesPRFunctionForPRAndThrowsExceptionWhenFunctionThrowsException() {
    Set<String> sessionIds = new HashSet<>();
    FunctionException exception = new FunctionException();
    ResultCollector exceptionCollector = mock(ResultCollector.class);

    when(sessionManager.getRegionAttributesId()).thenReturn(RegionShortcut.PARTITION.toString());
    when(emptyExecution.execute(TouchPartitionedRegionEntriesFunction.ID))
        .thenReturn(exceptionCollector);
    when(exceptionCollector.getResult()).thenThrow(exception);

    sessionCache.touchSessions(sessionIds);
    verify(logger).warn("Caught unexpected exception:", exception);
  }

  @Test
  public void touchSessionsInvokesRRFunctionForRRAndDoesNotThrowExceptionWhenFunctionDoesNotThrowException() {
    // Need to invoke this to set the session region
    when(regionFactory.create(sessionRegionName)).thenReturn(sessionRegion);
    sessionCache.initialize();

    Set<String> sessionIds = new HashSet<>();

    when(sessionRegion.getFullPath()).thenReturn("/" + sessionRegionName);
    when(sessionManager.getRegionAttributesId()).thenReturn(RegionShortcut.REPLICATE.toString());

    sessionCache.touchSessions(sessionIds);
    verify(emptyExecution).execute(TouchReplicatedRegionEntriesFunction.ID);
  }

  @Test
  public void touchSessionsInvokesRRFunctionForRRAndThrowsExceptionWhenFunctionThrowsException() {
    // Need to invoke this to set the session region
    when(regionFactory.create(sessionRegionName)).thenReturn(sessionRegion);
    sessionCache.initialize();

    Set<String> sessionIds = new HashSet<>();
    FunctionException exception = new FunctionException();
    ResultCollector exceptionCollector = mock(ResultCollector.class);

    when(sessionRegion.getFullPath()).thenReturn("/" + sessionRegionName);
    when(sessionManager.getRegionAttributesId()).thenReturn(RegionShortcut.REPLICATE.toString());
    when(emptyExecution.execute(TouchReplicatedRegionEntriesFunction.ID))
        .thenReturn(exceptionCollector);
    when(exceptionCollector.getResult()).thenThrow(exception);

    sessionCache.touchSessions(sessionIds);
    verify(logger).warn("Caught unexpected exception:", exception);
  }

  @Test
  public void isBackingCacheEnabledReturnsTrueWhenCommitValveFailfastDisabled() {
    assertThat(sessionCache.isBackingCacheAvailable()).isTrue();
  }

  @Test
  public void isBackingCacheEnabledReturnsValueWhenCommitValveFailfastEnabled() {
    boolean backingCacheEnabled = false;
    PoolImpl pool = mock(PoolImpl.class);

    when(sessionManager.isCommitValveFailfastEnabled()).thenReturn(true);
    doReturn(pool).when((ClientServerSessionCache) sessionCache).findPoolInPoolManager();
    when(pool.isPrimaryUpdaterAlive()).thenReturn(backingCacheEnabled);

    assertThat(sessionCache.isBackingCacheAvailable()).isEqualTo(backingCacheEnabled);
  }
}
