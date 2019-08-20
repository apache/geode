package org.apache.geode.modules.session.catalina.internal;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

import org.apache.geode.Statistics;
import org.apache.geode.distributed.internal.InternalDistributedSystem;

public class DeltaSessionStatisticsJUnitTest {

  @Test
  public void CreatedDeltaSessionStatisticsAccessProperStats() {
    String appName = "DeltaSessionStatisticsTest";

    InternalDistributedSystem internalDistributedSystem = mock(InternalDistributedSystem.class);
    Statistics statistics = mock(Statistics.class);

    when(internalDistributedSystem.createAtomicStatistics(any(), any())).thenReturn(statistics);

    DeltaSessionStatistics deltaSessionStatistics = new DeltaSessionStatistics(internalDistributedSystem, appName);

    deltaSessionStatistics.incSessionsCreated();
    deltaSessionStatistics.incSessionsExpired();
    deltaSessionStatistics.incSessionsInvalidated();

    deltaSessionStatistics.getSessionsCreated();
    deltaSessionStatistics.getSessionsExpired();
    deltaSessionStatistics.getSessionsInvalidated();

    verify(statistics, times(3)).incLong(anyInt(), anyLong());
    verify(statistics, times(3)).getLong(anyInt());
  }
}
