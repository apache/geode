package org.apache.geode.modules.util;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.logging.LocalLogWriter;

public class TouchReplicatedRegionEntriesFunctionJUnitTest {
  private TouchReplicatedRegionEntriesFunction function =
      spy(new TouchReplicatedRegionEntriesFunction());
  private FunctionContext context = mock(RegionFunctionContext.class);
  private Cache cache = mock(GemFireCacheImpl.class);
  private LocalLogWriter logger = mock(LocalLogWriter.class);
  private Region region = mock(Region.class);
  private ResultSender resultSender = mock(ResultSender.class);
  private String regionName = "regionName";
  private HashSet<String> keys = new HashSet<>();
  private Object[] arguments = new Object[] {regionName, keys};

  @Before
  public void setUp() {
    when(context.getArguments()).thenReturn(arguments);
    when(context.getCache()).thenReturn(cache);
    when(context.getResultSender()).thenReturn(resultSender);
    when(cache.getLogger()).thenReturn(logger);
    when(logger.fineEnabled()).thenReturn(false);
  }

  @Test
  public void executeDoesNotThrowExceptionWithProperlyDefinedContext() {
    when(cache.getRegion(regionName)).thenReturn(region);

    function.execute(context);

    verify(region).getAll(keys);
    verify(resultSender).lastResult(true);
  }

  @Test
  public void executeDoesNotThrowExceptionWithProperlyDefinedContextAndNullRegion() {
    when(cache.getRegion(regionName)).thenReturn(null);

    function.execute(context);

    verify(region, times(0)).getAll(keys);
    verify(resultSender).lastResult(true);
  }
}
