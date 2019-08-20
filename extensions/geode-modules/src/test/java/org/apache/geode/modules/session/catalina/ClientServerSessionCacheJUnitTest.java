package org.apache.geode.modules.session.catalina;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpSession;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.internal.InternalClientCache;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.modules.session.catalina.callback.SessionExpirationCacheListener;
import org.apache.geode.modules.util.RegionStatus;
import org.apache.geode.modules.util.BootstrappingFunction;
import org.apache.geode.modules.util.CreateRegionFunction;
import org.apache.geode.modules.util.SessionCustomExpiry;

import org.apache.juli.logging.Log;

public class ClientServerSessionCacheJUnitTest extends AbstractSessionCacheJUnitTest {

  private String regionName = "localSessionRegion";
  private ClientCache cache = mock(GemFireCacheImpl.class);
  private Execution emptyExecution = mock(Execution.class);
  private ResultCollector collector = mock(ResultCollector.class);
  private Log logger = mock(Log.class);
  private List<RegionStatus> emptyResultList = new ArrayList<>();
  private DistributedSystem distributedSystem = mock(DistributedSystem.class);
  private Statistics stats = mock(Statistics.class);
  private ClientRegionFactory<String, HttpSession> regionFactory = mock(ClientRegionFactory.class);

  @Before
  public void setUp() {
    when(cache.getDistributedSystem()).thenReturn(distributedSystem);
    doReturn(regionFactory).when(cache).createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY_HEAP_LRU);
    when(((InternalClientCache)cache).isClient()).thenReturn(true);

    when(emptyExecution.execute(any(Function.class))).thenReturn(collector);
    when(emptyExecution.execute(any(String.class))).thenReturn(collector);

    when(collector.getResult()).thenReturn(emptyResultList);

    when(sessionManager.getLogger()).thenReturn(logger);
    when(sessionManager.getEnableLocalCache()).thenReturn(true);
    when(sessionManager.getRegionName()).thenReturn(regionName);
    when(sessionManager.getMaxInactiveInterval()).thenReturn(1);

    when(distributedSystem.createAtomicStatistics(any(), any())).thenReturn(stats);

    sessionCache = spy(new ClientServerSessionCache(sessionManager, cache));
    doReturn(emptyExecution).when((ClientServerSessionCache)sessionCache).getExecutionForFunctionOnServers();
    doReturn(emptyExecution).when((ClientServerSessionCache)sessionCache).getExecutionForFunctionOnServersWithArguments(any());
    doReturn(emptyExecution).when((ClientServerSessionCache)sessionCache).getExecutionForFunctionOnServerWithRegionConfiguration(any());
    doReturn(emptyExecution).when((ClientServerSessionCache)sessionCache).getExecutionForFunctionOnRegionWithFilter(any());

    emptyResultList.clear();
    emptyResultList.add(RegionStatus.VALID);
  }

  @Test
  public void initializeSessionCacheSucceeds() {
    sessionCache.initialize();

    verify(emptyExecution).execute(any(BootstrappingFunction.class));
    verify(emptyExecution).execute(CreateRegionFunction.ID);
    verify(cache).createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY_HEAP_LRU);
    verify(regionFactory).setStatisticsEnabled(true);
    verify(regionFactory).setCustomEntryIdleTimeout(new SessionCustomExpiry());
    verify(regionFactory).addCacheListener(new SessionExpirationCacheListener());
    verify(regionFactory).create(regionName);
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
    doThrow(exception).when((ClientServerSessionCache)sessionCache).createLocalSessionRegion();

    assertThatThrownBy(() -> {sessionCache.initialize();}).hasCause(exception).isInstanceOf(IllegalStateException.class);

    verify(logger).fatal("Unable to create or retrieve region", exception);

  }

  @Test
  public void TouchSessionsInvokesPRFunctionForPR() {

  }
}
