/*========================================================================= 
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.query.cq;

import java.util.concurrent.atomic.AtomicLong;

import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;

public final class EventDataImpl implements EventData, Cloneable {
  private static volatile AtomicLong _id = new AtomicLong(0);

  private Object               key;
  private Object               value;
  private IntrospectionResult  metaInfo;
  // This is used for ordering the events.
  private long                 id;

  public EventDataImpl(Object k, Object v, IntrospectionResult mInf) {
    this.key = k;
    this.value = v;
    this.metaInfo = mInf;
    id = _id.getAndIncrement();
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    EventDataImpl obj;
    obj = (EventDataImpl) super.clone();

    obj.key = key;
    obj.value = value;
    obj.metaInfo = metaInfo;
    obj.id = id;
    return obj;
  }

  public Object getKey() {
    return key;
  }

  public synchronized Object getValue() {
    return value;
  }

  public synchronized long getId() {
    return id;
  }

  public synchronized void setValue(IntrospectionResult mInf, Object val) {
    this.metaInfo = mInf;
    this.value = val;
    id = _id.getAndIncrement();
  }

  public synchronized IntrospectionResult getIntrospectionResult() {
    return metaInfo;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof EventData) {
      EventData data = (EventData) obj;

      return this.key.equals(data.getKey());
    }

    return super.equals(obj);
  }

  @Override
  public int hashCode() {
    return this.key.hashCode();
  }

  public int compareTo(EventData o) {
    if (this.equals(o))
      return 0;

    return (this.getId() < o.getId()) ? -1 : +1;
  }
}
