package org.apache.geode.modules.session.catalina.internal;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.logging.ManagerLogWriter;
import org.apache.geode.modules.session.catalina.DeltaSessionInterface;

public class DeltaSessionAttributeEventBatchJUnitTest {
  String regionName = "regionName";
  String sessionId = "sessionId";
  LogWriter logWriter = mock(ManagerLogWriter.class);

  @Test
  public void TestApplyForBatch() {

    List<DeltaSessionAttributeEvent> eventList = new ArrayList<>();
    DeltaSessionAttributeEvent event1 = mock(DeltaSessionAttributeEvent.class);
    DeltaSessionAttributeEvent event2 = mock(DeltaSessionAttributeEvent.class);
    eventList.add(event1);
    eventList.add(event2);


    Cache cache = mock(GemFireCacheImpl.class);
    Region<String, DeltaSessionInterface> region = mock(Region.class);
    DeltaSessionInterface deltaSessionInterface = mock(DeltaSessionInterface.class);

    when(((GemFireCacheImpl) cache).getRegion(regionName)).thenReturn(region);
    when(cache.getLogger()).thenReturn(logWriter);
    when(logWriter.fineEnabled()).thenReturn(false);
    when(region.get(sessionId)).thenReturn(deltaSessionInterface);

    DeltaSessionAttributeEventBatch batch = new DeltaSessionAttributeEventBatch(regionName, sessionId, eventList);

    batch.apply(cache);

    verify(deltaSessionInterface).applyAttributeEvents(region, eventList);
  }
}
