package com.gemstone.gemfire.mgmt.DataBrowser.query.cq.event;

import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.EventData;

public class ErrorEvent implements ICQEvent {

  private Throwable cause;
  
  public ErrorEvent(Throwable thrw) {
   this.cause = thrw; 
  }
  
  public EventData getEventData() {    
    return null;
  }

  public Throwable getThrowable() {
    return cause;
  }
}
