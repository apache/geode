package com.gemstone.gemfire.internal.cache.wan;

import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.client.internal.LocatorDiscoveryCallback;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.cache.wan.GatewaySenderFactory;

public interface InternalGatewaySenderFactory extends GatewaySenderFactory {

  public GatewaySenderFactory setForInternalUse(boolean b);

  public GatewaySenderFactory addAsyncEventListener(AsyncEventListener listener);

  public GatewaySenderFactory setBucketSorted(boolean bucketSorted);

  public GatewaySender create(String senderIdFromAsyncEventQueueId);

  public void configureGatewaySender(GatewaySender senderCreation);

  public GatewaySenderFactory setLocatorDiscoveryCallback(
      LocatorDiscoveryCallback myLocatorCallback);
}
