package com.gemstone.gemfire.mgmt.DataBrowser.query.cq.event;

import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.EventData;

public class RowUpdated implements ICQEvent {
  private EventData data;

  public RowUpdated(EventData evtData) {
    super();
    data = evtData;
  }

  public EventData getEventData() {
    return data;
  }

  public Throwable getThrowable() {
    return null;
  }

}
