/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management;

/**
 * 
 * @author rishim
 * @since  8.0
 */
public class ClientQueueDetail {

  /**
   * Client ID
   */
  private String clientId;
  
  /**
   * Current queue size of client which is derived by the formula queueSize = eventsEnqued -
   * eventsRemoved - eventsConflated - markerEventsConflated - eventsExpired -
   * eventsRemovedByQrm - eventsTaken - numVoidRemovals
   */
  private long queueSize;
  /**
   * Number of events added to queue.
   */
  private long eventsEnqued;

  /**
   * Number of events removed from the queue.
   */
  private long eventsRemoved;

  /**
   * Number of events conflated for the queue.
   */
  private long eventsConflated;

  /**
   * Number of marker events conflated for the queue.
   */
  private long markerEventsConflated;

  /**
   * Number of events expired from the queue.
   */
  private long eventsExpired;

  /**
   * Number of events removed by QRM message.
   */
  private long eventsRemovedByQrm;

  /**
   * Number of events taken from the queue.
   */
  private long eventsTaken;

  /**
   * Number of void removals from the queue.
   */
  private long numVoidRemovals;

  public String getClientId() {
    return clientId;
  }

  public void setClientId(String clientId) {
    this.clientId = clientId;
  }

  public long getQueueSize() {
    return queueSize;
  }

  public void setQueueSize(long queueSize) {
    this.queueSize = queueSize;
  }

  public long getEventsEnqued() {
    return eventsEnqued;
  }

  public void setEventsEnqued(long eventsEnqued) {
    this.eventsEnqued = eventsEnqued;
  }

  public long getEventsRemoved() {
    return eventsRemoved;
  }

  public void setEventsRemoved(long eventsRemoved) {
    this.eventsRemoved = eventsRemoved;
  }

  public long getEventsConflated() {
    return eventsConflated;
  }

  public void setEventsConflated(long eventsConflated) {
    this.eventsConflated = eventsConflated;
  }

  public long getMarkerEventsConflated() {
    return markerEventsConflated;
  }

  public void setMarkerEventsConflated(long markerEventsConflated) {
    this.markerEventsConflated = markerEventsConflated;
  }

  public long getEventsExpired() {
    return eventsExpired;
  }

  public void setEventsExpired(long eventsExpired) {
    this.eventsExpired = eventsExpired;
  }

  public long getEventsRemovedByQrm() {
    return eventsRemovedByQrm;
  }

  public void setEventsRemovedByQrm(long eventsRemovedByQrm) {
    this.eventsRemovedByQrm = eventsRemovedByQrm;
  }

  public long getEventsTaken() {
    return eventsTaken;
  }

  public void setEventsTaken(long eventsTaken) {
    this.eventsTaken = eventsTaken;
  }

  public long getNumVoidRemovals() {
    return numVoidRemovals;
  }

  public void setNumVoidRemovals(long numVoidRemovals) {
    this.numVoidRemovals = numVoidRemovals;
  }

  @Override
  public String toString() {
    return "ClientQueueDetail [clientId=" + clientId + ", queueSize=" + queueSize + ", eventsEnqued=" + eventsEnqued + ", eventsRemoved=" + eventsRemoved
        + ", eventsConflated=" + eventsConflated + ", markerEventsConflated=" + markerEventsConflated + ", eventsExpired=" + eventsExpired
        + ", eventsRemovedByQrm=" + eventsRemovedByQrm + ", eventsTaken=" + eventsTaken + ", numVoidRemovals=" + numVoidRemovals + "]";
  }
  

}
