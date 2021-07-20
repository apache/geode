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
package org.apache.geode.internal.cache;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionMembershipListener;
import org.apache.geode.cache.RegionRoleListener;
import org.apache.geode.cache.RoleEvent;

@Immutable
public abstract class EnumListenerEvent {

  private final String name;

  protected EnumListenerEvent(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return name;
  }

  public abstract byte getEventCode();

  /**
   * Invoke the correct listener method for this event.
   *
   * @since GemFire 5.0
   */
  public abstract void dispatchEvent(CacheEvent event, CacheListener listener);

  @Immutable
  public static final EnumListenerEvent AFTER_CREATE = new AFTER_CREATE(); // 1

  @Immutable
  public static final EnumListenerEvent AFTER_UPDATE = new AFTER_UPDATE(); // 2

  @Immutable
  public static final EnumListenerEvent AFTER_INVALIDATE = new AFTER_INVALIDATE(); // 3

  @Immutable
  public static final EnumListenerEvent AFTER_DESTROY = new AFTER_DESTROY(); // 4

  @Immutable
  public static final EnumListenerEvent AFTER_REGION_CREATE = new AFTER_REGION_CREATE(); // 5

  @Immutable
  public static final EnumListenerEvent AFTER_REGION_INVALIDATE = new AFTER_REGION_INVALIDATE(); // 6

  @Immutable
  public static final EnumListenerEvent AFTER_REGION_CLEAR = new AFTER_REGION_CLEAR(); // 7

  @Immutable
  public static final EnumListenerEvent AFTER_REGION_DESTROY = new AFTER_REGION_DESTROY(); // 8

  @Immutable
  public static final EnumListenerEvent AFTER_REMOTE_REGION_CREATE =
      new AFTER_REMOTE_REGION_CREATE();// 9

  @Immutable
  public static final EnumListenerEvent AFTER_REMOTE_REGION_DEPARTURE =
      new AFTER_REMOTE_REGION_DEPARTURE(); // 10

  @Immutable
  public static final EnumListenerEvent AFTER_REMOTE_REGION_CRASH = new AFTER_REMOTE_REGION_CRASH();// 11

  @Immutable
  public static final EnumListenerEvent AFTER_ROLE_GAIN = new AFTER_ROLE_GAIN();// 12

  @Immutable
  public static final EnumListenerEvent AFTER_ROLE_LOSS = new AFTER_ROLE_LOSS();// 13

  @Immutable
  public static final EnumListenerEvent AFTER_REGION_LIVE = new AFTER_REGION_LIVE();// 14

  @Immutable
  public static final EnumListenerEvent AFTER_REGISTER_INSTANTIATOR =
      new AFTER_REGISTER_INSTANTIATOR();// 15

  @Immutable
  public static final EnumListenerEvent AFTER_REGISTER_DATASERIALIZER =
      new AFTER_REGISTER_DATASERIALIZER();// 16

  @Immutable
  public static final EnumListenerEvent AFTER_TOMBSTONE_EXPIRATION =
      new AFTER_TOMBSTONE_EXPIRATION(); // 17

  @Immutable
  public static final EnumListenerEvent TIMESTAMP_UPDATE = new TIMESTAMP_UPDATE(); // 18

  @Immutable
  public static final EnumListenerEvent AFTER_UPDATE_WITH_GENERATE_CALLBACKS =
      new AFTER_UPDATE_WITH_GENERATE_CALLBACKS(); // 19

  @Immutable
  private static final EnumListenerEvent[] instances =
      new EnumListenerEvent[] {AFTER_CREATE, AFTER_UPDATE, AFTER_INVALIDATE, AFTER_DESTROY,
          AFTER_REGION_CREATE, AFTER_REGION_INVALIDATE, AFTER_REGION_CLEAR, AFTER_REGION_DESTROY,
          AFTER_REMOTE_REGION_CREATE, AFTER_REMOTE_REGION_DEPARTURE, AFTER_REMOTE_REGION_CRASH,
          AFTER_ROLE_GAIN, AFTER_ROLE_LOSS, AFTER_REGION_LIVE, AFTER_REGISTER_INSTANTIATOR,
          AFTER_REGISTER_DATASERIALIZER, AFTER_TOMBSTONE_EXPIRATION, TIMESTAMP_UPDATE,
          AFTER_UPDATE_WITH_GENERATE_CALLBACKS};

  static {
    for (int i = 0; i < instances.length; i++) {
      assert instances[i].getEventCode() == i + 1 : "event is in the wrong place: " + (i + 1);
    }
  }


  private static class AFTER_CREATE extends EnumListenerEvent {
    protected AFTER_CREATE() {
      super("AFTER_CREATE");
    }

    @Override
    public void dispatchEvent(CacheEvent event, CacheListener listener) {
      listener.afterCreate((EntryEvent) event);
    }

    @Override
    public byte getEventCode() {
      return 1;
    }
  }

  private static class AFTER_UPDATE extends EnumListenerEvent {
    protected AFTER_UPDATE() {
      super("AFTER_UPDATE");
    }

    @Override
    public void dispatchEvent(CacheEvent event, CacheListener listener) {
      listener.afterUpdate((EntryEvent) event);
    }

    @Override
    public byte getEventCode() {
      return 2;
    }
  }

  private static class AFTER_INVALIDATE extends EnumListenerEvent {
    protected AFTER_INVALIDATE() {
      super("AFTER_INVALIDATE");
    }

    @Override
    public void dispatchEvent(CacheEvent event, CacheListener listener) {
      listener.afterInvalidate((EntryEvent) event);
    }

    @Override
    public byte getEventCode() {
      return 3;
    }
  }

  private static class AFTER_DESTROY extends EnumListenerEvent {
    protected AFTER_DESTROY() {
      super("AFTER_DESTROY");
    }

    @Override
    public void dispatchEvent(CacheEvent event, CacheListener listener) {
      listener.afterDestroy((EntryEvent) event);
    }

    @Override
    public byte getEventCode() {
      return 4;
    }
  }

  private static class AFTER_REGION_CREATE extends EnumListenerEvent {
    protected AFTER_REGION_CREATE() {
      super("AFTER_REGION_CREATE");
    }

    @Override
    public void dispatchEvent(CacheEvent event, CacheListener listener) {
      listener.afterRegionCreate((RegionEvent) event);
    }

    @Override
    public byte getEventCode() {
      return 5;
    }
  }

  private static class AFTER_REGION_INVALIDATE extends EnumListenerEvent {
    protected AFTER_REGION_INVALIDATE() {
      super("AFTER_REGION_INVALIDATE");
    }

    @Override
    public void dispatchEvent(CacheEvent event, CacheListener listener) {
      listener.afterRegionInvalidate((RegionEvent) event);
    }

    @Override
    public byte getEventCode() {
      return 6;
    }
  }

  private static class AFTER_REGION_CLEAR extends EnumListenerEvent {
    protected AFTER_REGION_CLEAR() {
      super("AFTER_REGION_CLEAR");
    }

    @Override
    public void dispatchEvent(CacheEvent event, CacheListener listener) {
      listener.afterRegionClear((RegionEvent) event);
    }

    @Override
    public byte getEventCode() {
      return 7;
    }
  }

  private static class AFTER_REGION_DESTROY extends EnumListenerEvent {
    protected AFTER_REGION_DESTROY() {
      super("AFTER_REGION_DESTROY");
    }

    @Override
    public void dispatchEvent(CacheEvent event, CacheListener listener) {
      listener.afterRegionDestroy((RegionEvent) event);
      // only now is the right time to call close() on the listener
      ((LocalRegion) event.getRegion()).closeCacheCallback(listener);
    }

    @Override
    public byte getEventCode() {
      return 8;
    }
  }

  private static class AFTER_REMOTE_REGION_CREATE extends EnumListenerEvent {
    protected AFTER_REMOTE_REGION_CREATE() {
      super("AFTER_REMOTE_REGION_CREATE");
    }

    @Override
    public void dispatchEvent(CacheEvent event, CacheListener listener) {
      if (listener instanceof RegionMembershipListener) {
        ((RegionMembershipListener) listener).afterRemoteRegionCreate((RegionEvent) event);
      }
    }

    @Override
    public byte getEventCode() {
      return 9;
    }
  }

  private static class AFTER_REMOTE_REGION_DEPARTURE extends EnumListenerEvent {
    protected AFTER_REMOTE_REGION_DEPARTURE() {
      super("AFTER_REMOTE_REGION_DEPARTURE");
    }

    @Override
    public void dispatchEvent(CacheEvent event, CacheListener listener) {
      if (listener instanceof RegionMembershipListener) {
        ((RegionMembershipListener) listener).afterRemoteRegionDeparture((RegionEvent) event);
      }
    }

    @Override
    public byte getEventCode() {
      return 10;
    }
  }

  private static class AFTER_REMOTE_REGION_CRASH extends EnumListenerEvent {
    protected AFTER_REMOTE_REGION_CRASH() {
      super("AFTER_REMOTE_REGION_CRASH");
    }

    @Override
    public void dispatchEvent(CacheEvent event, CacheListener listener) {
      if (listener instanceof RegionMembershipListener) {
        ((RegionMembershipListener) listener).afterRemoteRegionCrash((RegionEvent) event);
      }
    }

    @Override
    public byte getEventCode() {
      return 11;
    }
  }

  private static class AFTER_ROLE_GAIN extends EnumListenerEvent {
    protected AFTER_ROLE_GAIN() {
      super("AFTER_ROLE_GAIN");
    }

    @Override
    public void dispatchEvent(CacheEvent event, CacheListener listener) {
      if (listener instanceof RegionRoleListener) {
        ((RegionRoleListener) listener).afterRoleGain((RoleEvent) event);
      }
    }

    @Override
    public byte getEventCode() {
      return 12;
    }
  }

  private static class AFTER_ROLE_LOSS extends EnumListenerEvent {
    protected AFTER_ROLE_LOSS() {
      super("AFTER_ROLE_LOSS");
    }

    @Override
    public void dispatchEvent(CacheEvent event, CacheListener listener) {
      if (listener instanceof RegionRoleListener) {
        ((RegionRoleListener) listener).afterRoleLoss((RoleEvent) event);
      }
    }

    @Override
    public byte getEventCode() {
      return 13;
    }
  }
  private static class AFTER_REGION_LIVE extends EnumListenerEvent {
    protected AFTER_REGION_LIVE() {
      super("AFTER_REGION_LIVE");
    }

    @Override
    public void dispatchEvent(CacheEvent event, CacheListener listener) {
      listener.afterRegionLive((RegionEvent) event);
    }

    @Override
    public byte getEventCode() {
      return 14;
    }
  }

  private static class AFTER_REGISTER_INSTANTIATOR extends EnumListenerEvent {
    protected AFTER_REGISTER_INSTANTIATOR() {
      super("AFTER_REGISTER_INSTANTIATOR");
    }

    @Override
    public void dispatchEvent(CacheEvent event, CacheListener listener) {}

    @Override
    public byte getEventCode() {
      return 15;
    }
  }

  private static class AFTER_REGISTER_DATASERIALIZER extends EnumListenerEvent {
    protected AFTER_REGISTER_DATASERIALIZER() {
      super("AFTER_REGISTER_DATASERIALIZER");
    }

    @Override
    public void dispatchEvent(CacheEvent event, CacheListener listener) {}

    @Override
    public byte getEventCode() {
      return 16;
    }
  }

  private static class AFTER_TOMBSTONE_EXPIRATION extends EnumListenerEvent {
    protected AFTER_TOMBSTONE_EXPIRATION() {
      super("AFTER_TOMBSTONE_EXPIRATION");
    }

    @Override
    public void dispatchEvent(CacheEvent event, CacheListener listener) {}

    @Override
    public byte getEventCode() {
      return 17;
    }
  }

  private static class TIMESTAMP_UPDATE extends EnumListenerEvent {

    protected TIMESTAMP_UPDATE() {
      super("TIMESTAMP_UPDATE");
    }

    @Override
    public void dispatchEvent(CacheEvent event, CacheListener listener) {}

    @Override
    public byte getEventCode() {
      return 18;
    }
  }

  private static class AFTER_UPDATE_WITH_GENERATE_CALLBACKS extends EnumListenerEvent {
    protected AFTER_UPDATE_WITH_GENERATE_CALLBACKS() {
      super("AFTER_UPDATE_WITH_GENERATE_CALLBACKS");
    }

    @Override
    public void dispatchEvent(CacheEvent event, CacheListener listener) {}

    @Override
    public byte getEventCode() {
      return 19;
    }
  }


  /**
   *
   * This method returns the EnumListenerEvent object corresponding to the cCode given.
   *
   * The mapping of cCode to EnumListenerEvent is :
   * <ul>
   * <li>1 - AFTER_CREATE
   * <li>2 - AFTER_UPDATE
   * <li>3 - AFTER_INVALIDATE
   * <li>4 - AFTER_DESTROY
   * <li>5 - AFTER_REGION_CREATE
   * <li>6 - AFTER_REGION_INVALIDATE
   * <li>7 - AFTER_REGION_CLEAR
   * <li>8 - AFTER_REGION_DESTROY
   * <li>9 - AFTER_REMOTE_REGION_CREATE
   * <li>10 - AFTER_REMOTE_REGION_DEPARTURE
   * <li>11 - AFTER_REMOTE_REGION_CRASH
   * <li>12 - AFTER_ROLE_GAIN
   * <li>13 - AFTER_ROLE_LOSS
   * <li>14 - AFTER_REGION_LIVE
   * <li>15 - AFTER_REGISTER_INSTANTIATOR
   * <li>16 - AFTER_REGISTER_DATASERIALIZER
   * <li>17 - AFTER_TOMBSTONE_EXPIRATION
   * <li>18 - TIMESTAMP_UPDATE
   * <li>19 - AFTER_UPDATE_WITH_GENERATE_CALLBACKS
   * </ul>
   *
   * @param eventCode the eventCode corresponding to the EnumListenerEvent object desired
   * @return the EnumListenerEvent object corresponding to the cCode
   */
  public static EnumListenerEvent getEnumListenerEvent(int eventCode) {
    if (eventCode > 0 && eventCode <= instances.length) {
      return instances[eventCode - 1];
    }
    return null;
  }
}
