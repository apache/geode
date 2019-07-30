package org.apache.geode.internal.cache;

import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;

public abstract class OnRequestImageMessageObserver extends DistributionMessageObserver {
  private final String regionName;
  private final Runnable onMessageReceived;
  protected boolean hasExecutedOnMessageReceived = false;

  public OnRequestImageMessageObserver(String regionName, Runnable onMessageReceived) {
    this.regionName = regionName;
    this.onMessageReceived = onMessageReceived;
  }

  @Override
  public void beforeProcessMessage(ClusterDistributionManager dm, DistributionMessage message) {
    if (message instanceof InitialImageOperation.RequestImageMessage) {
      InitialImageOperation.RequestImageMessage rim =
          (InitialImageOperation.RequestImageMessage) message;
      synchronized (this) {
        if (!hasExecutedOnMessageReceived && rim.regionPath.contains(regionName)) {
          onMessageReceived.run();
          hasExecutedOnMessageReceived = true;
        }
      }
    }
  }
}
