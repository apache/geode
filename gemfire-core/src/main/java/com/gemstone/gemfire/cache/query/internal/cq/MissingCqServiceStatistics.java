package com.gemstone.gemfire.cache.query.internal.cq;

import com.gemstone.gemfire.cache.query.CqServiceStatistics;

public class MissingCqServiceStatistics implements CqServiceStatistics {

  @Override
  public long numCqsActive() {
    return 0;
  }

  @Override
  public long numCqsCreated() {
    return 0;
  }

  @Override
  public long numCqsClosed() {
    return 0;
  }

  @Override
  public long numCqsStopped() {
    return 0;
  }

  @Override
  public long numCqsOnClient() {
    return 0;
  }

  @Override
  public long numCqsOnRegion(String regionFullPath) {
    return 0;
  }
}
