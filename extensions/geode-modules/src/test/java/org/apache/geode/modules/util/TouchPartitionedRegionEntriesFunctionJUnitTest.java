package org.apache.geode.modules.util;


import static org.assertj.core.internal.bytebuddy.matcher.ElementMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;

import org.apache.geode.LogWriter;
import org.apache.geode.internal.logging.LocalLogWriter;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.internal.cache.GemFireCacheImpl;

public class TouchPartitionedRegionEntriesFunctionJUnitTest {

  private TouchPartitionedRegionEntriesFunction function = spy(new TouchPartitionedRegionEntriesFunction());
  private FunctionContext context = mock(RegionFunctionContext.class);
  private Cache cache = mock(GemFireCacheImpl.class);
  private LocalLogWriter logger = mock(LocalLogWriter.class);
  private Region primaryDataSet = mock(Region.class);
  private ResultSender resultSender = mock(ResultSender.class);

  @Before
  public void setUp() {
    when(context.getCache()).thenReturn(cache);
    when(context.getResultSender()).thenReturn(resultSender);
    when(cache.getLogger()).thenReturn(logger);
    when(logger.fineEnabled()).thenReturn(false);
    doReturn(primaryDataSet).when(function).getLocalDataForContextViaRegionHelper((RegionFunctionContext)context);
  }

  @Test
  public void executeDoesNotThrowExceptionWithProperlyDefinedContext() {
    doReturn(new HashSet() {}).when((RegionFunctionContext)context).getFilter();

    function.execute(context);

    verify(primaryDataSet, times(0)).get(any());
    verify(resultSender).lastResult(true);
  }

  @Test
  public void executeDoesNotThrowExceptionWithProperlyDefinedContextAndMultipleKeys() {
    HashSet<String> keys = new HashSet();
    keys.add("Key1");
    keys.add("Key2");

    doReturn(keys).when((RegionFunctionContext)context).getFilter();

    function.execute(context);

    verify(primaryDataSet, times(keys.size())).get(anyString());
    verify(resultSender).lastResult(true);
  }
}
