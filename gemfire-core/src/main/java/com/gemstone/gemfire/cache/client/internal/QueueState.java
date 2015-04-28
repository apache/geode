package com.gemstone.gemfire.cache.client.internal;

import java.util.concurrent.ScheduledExecutorService;
import com.gemstone.gemfire.internal.cache.EventID;

public interface QueueState {

  public void processMarker();
  public boolean getProcessedMarker();
  public void incrementInvalidatedStats();
  public boolean verifyIfDuplicate(EventID eventId, boolean addToMap);
  public boolean verifyIfDuplicate(EventID eventId);
  /** test hook
   */
  public java.util.Map getThreadIdToSequenceIdMap();
  public void start(ScheduledExecutorService timer, int interval);
}
