package com.gemstone.gemfire.mgmt.DataBrowser.query.cq.event;

import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.EventData;

public class RowDeleted implements ICQEvent {
  private EventData data;

  public RowDeleted(EventData evtData) {
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
