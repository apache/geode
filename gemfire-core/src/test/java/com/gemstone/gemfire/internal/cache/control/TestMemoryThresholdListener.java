/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.control;


public class TestMemoryThresholdListener implements ResourceListener<MemoryEvent>{
  private int normalCalls = 0;
  private int criticalThresholdCalls = 0;
  private int evictionThresholdCalls = 0;
  private int evictionDisabledCalls = 0;
  private int criticalDisabledCalls = 0;
  private long bytesFromThreshold = 0;
  private int currentHeapPercentage = 0;
  private int allCalls = 0;

  public  long getBytesFromThreshold() {
    synchronized (this) {
      return this.bytesFromThreshold;
    }
  }

  public  int getCurrentHeapPercentage() {
    synchronized (this) {
      return this.currentHeapPercentage;
    }
  }
  
  public  int getAllCalls() {
    synchronized (this) {
      return this.allCalls;
    }
  }
  public  int getNormalCalls() {
    synchronized(this){
      return this.normalCalls;
    }
  }
  public  int getCriticalThresholdCalls() {
    synchronized(this){
      return this.criticalThresholdCalls;
    }
  }
  public  int getCriticalDisabledCalls() {
    synchronized (this) {
      return this.criticalDisabledCalls;
    }
  }
  public  int getEvictionThresholdCalls() {
    synchronized(this){
      return this.evictionThresholdCalls;
    }
  }
  public  int getEvictionDisabledCalls() {
    synchronized (this) {
      return this.evictionDisabledCalls;
    }
  }

  public  void resetThresholdCalls(){
    synchronized (this) {
      this.normalCalls = 0;
      this.criticalThresholdCalls = 0;
      this.evictionThresholdCalls = 0;
      this.bytesFromThreshold = 0;
      this.currentHeapPercentage = 0;
      this.evictionDisabledCalls = 0;
      this.criticalDisabledCalls = 0;
      this.allCalls = 0;
    }
  }
  @Override
  public  String toString() {
    return new StringBuilder("TestListenerStatus:")
          .append(" normalCalls :"+this.normalCalls)
          .append(" allCalls :"+this.allCalls)
          .append(" criticalThresholdCalls :"+this.criticalThresholdCalls)
          .append(" evictionThresholdCalls :"+this.evictionThresholdCalls)
          .append(" previousNormalCalls :"+this.normalCalls)
          .append(" bytesFromThreshold :"+this.bytesFromThreshold)
          .append(" currentHeapPercentage :"+this.currentHeapPercentage)
          .append(" evictionDisabledCalls :"+this.evictionDisabledCalls)
          .append(" criticalDisabledCalls :"+this.criticalDisabledCalls).toString();
  }
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.control.ResourceListener#onEvent(java.lang.Object)
   */
  @Override
  public void onEvent(MemoryEvent event) {
    synchronized (this) {
      if (event.getState().isNormal()) {
        this.normalCalls++;
      } 
      if (event.getState().isCritical() && event.getState() != event.getPreviousState()) {
        this.criticalThresholdCalls++;
      } 
      if (event.getState().isEviction() && event.getState() != event.getPreviousState()) {
        this.evictionThresholdCalls++;
      }
      if (event.getState().isCriticalDisabled() && event.getState() != event.getPreviousState()) {
        this.criticalDisabledCalls++;
      }
      if (event.getState().isEvictionDisabled() && event.getState() != event.getPreviousState()) {
          this.evictionDisabledCalls++;
      }
      
      this.allCalls++;
      
      if (event.getState().isCritical()) {
        this.bytesFromThreshold = event.getBytesUsed() - event.getThresholds().getCriticalThresholdBytes();
      } else if (event.getState().isEviction()) {
        if (event.getPreviousState().isCritical()) {
          this.bytesFromThreshold = event.getThresholds().getCriticalThresholdBytes() - event.getBytesUsed();
        } else {
          this.bytesFromThreshold = event.getBytesUsed() - event.getThresholds().getEvictionThresholdBytes();
        }
      } else {
        this.bytesFromThreshold = event.getThresholds().getEvictionThresholdBytes() - event.getBytesUsed();
      }
      
      if (event.getThresholds().getMaxMemoryBytes() == 0) {
        this.currentHeapPercentage = 0;
      } else if (event.getBytesUsed() > event.getThresholds().getMaxMemoryBytes()) {
        this.currentHeapPercentage = 1;
      } else {
        this.currentHeapPercentage = convertToIntPercent((double) event.getBytesUsed() / event.getThresholds().getMaxMemoryBytes());
      }
    }
  }
  
  /**
   * Convert a percentage as a double to an integer e.g. 0.09 => 9
   * also legal is 0.095 => 9
   * @param percentHeap a percentage value expressed as a double e.g. 9.5% => 0.095
   * @return the calculated percent as an integer >= 0 and <= 100
   */
  private int convertToIntPercent(final double percentHeap) {
    assert percentHeap >= 0.0 && percentHeap <= 1.0;
    int ret = (int) Math.ceil(percentHeap * 100.0);
    assert ret >= 0 && ret <= 100;
    return ret;
  }
}
