/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.statistics;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Kirk Lund
 * @since 7.0
 */
public class TestSampleHandler implements SampleHandler {

  private final List<Info> notifications = new ArrayList<Info>();

  public TestSampleHandler() {
  }
  
  public synchronized void clearAllNotifications() {
    this.notifications.clear();
  }
  
  public synchronized int getNotificationCount() {
    return this.notifications.size();
  }
  
  public synchronized List<Info> getNotifications() {
    return this.notifications;
  }
  
  @Override
  public synchronized void sampled(long timeStamp, List<ResourceInstance> resourceInstances) {
    int resourceCount = resourceInstances.size();
    this.notifications.add(new SampledInfo("sampled", timeStamp, resourceCount));
  }

  @Override
  public synchronized void allocatedResourceType(ResourceType resourceType) {
    this.notifications.add(new ResourceTypeInfo("allocatedResourceType", resourceType));
  }

  @Override
  public synchronized void allocatedResourceInstance(ResourceInstance resourceInstance) {
    this.notifications.add(new ResourceInstanceInfo("allocatedResourceInstance", resourceInstance));
  }

  @Override
  public synchronized void destroyedResourceInstance(ResourceInstance resourceInstance) {
    this.notifications.add(new ResourceInstanceInfo("destroyedResourceInstance", resourceInstance));
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getName());
    sb.append("@").append(System.identityHashCode(this)).append("{");
    sb.append("notificationCount=").append(this.notifications.size());
    sb.append(", notifications=").append(this.notifications);
    sb.append("}");
    return sb.toString();
  }
  
  /**
   * @author Kirk Lund
   * @since 7.0
   */
  public static class Info {
    private final String name;
    
    public Info(String name) {
      this.name = name;
    }
    
    public String getName() {
      return this.name;
    }
    
    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(getClass().getName());
      sb.append("@").append(System.identityHashCode(this)).append("{");
      sb.append("name=").append(this.name);
      appendToString(sb);
      return sb.append("}").toString();
    }
    
    protected void appendToString(StringBuilder sb) {
    }
  }
  
  /**
   * @author Kirk Lund
   * @since 7.0
   */
  public static class ResourceInstanceInfo extends Info {
    private final ResourceInstance resource;

    public ResourceInstanceInfo(String name, ResourceInstance resource) {
      super(name);
      this.resource = resource;
    }
    
    public ResourceInstance getResourceInstance() {
      return this.resource;
    }
    
    @Override
    protected void appendToString(StringBuilder sb) {
      sb.append(", resource=").append(this.resource);
    }
  }
  
  /**
   * @author Kirk Lund
   * @since 7.0
   */
  public static class ResourceTypeInfo extends Info {
    private final ResourceType type;

    public ResourceTypeInfo(String name, ResourceType type) {
      super(name);
      this.type = type;
    }
    
    public ResourceType getResourceType() {
      return this.type;
    }
    
    @Override
    protected void appendToString(StringBuilder sb) {
      sb.append(", type=").append(this.type);
    }
  }
  
  /**
   * @author Kirk Lund
   * @since 7.0
   */
  public static class SampledInfo extends Info {
    private final long timeStamp;
    private final int resourceCount;
    
    public SampledInfo(String name, long timeStamp, int resourceCount) {
      super(name);
      this.timeStamp = timeStamp;
      this.resourceCount = resourceCount;
    }
    
    public long getTimeStamp() {
      return this.timeStamp;
    }
    
    public int getResourceCount() {
      return this.resourceCount;
    }
    
    @Override
    protected void appendToString(StringBuilder sb) {
      sb.append(", timeStamp=").append(this.timeStamp);
      sb.append(", resourceCount=").append(this.resourceCount);
    }
  }
  
}
