/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.statistics;

import java.util.ArrayList;
import java.util.List;

/**
 * @since GemFire 7.0
 */
public class TestSampleHandler implements SampleHandler {

  private final List<Info> notifications = new ArrayList<Info>();

  public TestSampleHandler() {}

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
   * @since GemFire 7.0
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

    protected void appendToString(StringBuilder sb) {}
  }

  /**
   * @since GemFire 7.0
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
   * @since GemFire 7.0
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
   * @since GemFire 7.0
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
