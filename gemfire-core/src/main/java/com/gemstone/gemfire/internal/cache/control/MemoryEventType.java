/*
 * ========================================================================= 
 * (c)Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */

package com.gemstone.gemfire.internal.cache.control;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.RegionEvictorTask;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.Thresholds;
import com.gemstone.gemfire.internal.cache.lru.HeapEvictor;

/**
 * @author sbawaska
 *
 */
public enum MemoryEventType {

  /**
   * when we don't yet know what sort of event this is
   */
  UNKNOWN {
    @Override
    public MemoryEventImpl[] getMissingEvents(MemoryEventImpl newEvent) {
      MemoryEventImpl[] retVal = null;
      switch (newEvent.getType()) {
      case EVICTION_UP:
        retVal = new MemoryEventImpl[] {newEvent};
        break;
      case EVICTION_DOWN:
        //ignore
        break;
      case CRITICAL_UP:
        if (newEvent.getThresholds().isEvictionThresholdEnabled()) {
          retVal = new MemoryEventImpl[2];
          retVal[0] = new MemoryEventImpl(newEvent, MemoryEventType.EVICTION_UP);
          retVal[1] = newEvent;
        } else {
          retVal = new MemoryEventImpl[] {newEvent};
        }
        break;
      case CRITICAL_DOWN:
        //ignore
        break;
      case UNKNOWN:
        // the unknown event should never "slip through" from the caller
        Assert.assertTrue(false);
        break;
      default:
        throw new IllegalStateException("MemoryEventType " +newEvent.getType()+
                              "not considered in state machine");
      }
      return retVal;
    }
  },
  /**
   * eviction threshold exceeded
   */
  EVICTION_UP {
    @Override
    public MemoryEventImpl[] getMissingEvents(MemoryEventImpl newEvent) {
      MemoryEventImpl[] retVal = null;
      switch (newEvent.getType()) {
      case EVICTION_UP:
        //ignore
        break;
      case EVICTION_DOWN:
        retVal = new MemoryEventImpl[] {newEvent};
        break;
      case CRITICAL_UP:
        retVal = new MemoryEventImpl[] {newEvent};
        break;
      case CRITICAL_DOWN:
        if (newEvent.isLocal()) {
          // buildEvent should never have built a critical_down event
          Assert.assertTrue(false);
        } else {
          //Since both these events are in same memory range, we could
          //have ignored this event, and still deliver EVICTION_DOWN or
          //CRITICAL_UP which are valid for both EVICTION_UP and CRITICAL_DOWN
          retVal = new MemoryEventImpl[] {MemoryEventImpl.NO_DELIVERY};
        }
        break;
      case UNKNOWN:
        // the unknown event should never "slip through" from the caller
        Assert.assertTrue(false);
        break;
      default:
        throw new IllegalStateException("MemoryEventType " +newEvent.getType()+
                            "not considered in state machine");
      }
      return retVal;
    }
  },
  /**
   * triggers a new burst of eviction. Does not influence state machine decisions,
   * and is not distributed to remote hosts.
   */
  EVICT_MORE {
    @Override
    public MemoryEventImpl[] getMissingEvents(MemoryEventImpl newEvent) {
      throw new UnsupportedOperationException("EVICT_MORE should not be previous event");
    }
    
  },
  /**
   * dropped below eviction threshold
   */
  EVICTION_DOWN {
    @Override
    public MemoryEventImpl[] getMissingEvents(MemoryEventImpl newEvent) {
      MemoryEventImpl[] retVal = null;
      switch (newEvent.getType()) {
      case EVICTION_UP:
        retVal = new MemoryEventImpl[] {newEvent};
        break;
      case EVICTION_DOWN:
        //ignore
        break;
      case CRITICAL_UP:
        if (newEvent.getThresholds().isEvictionThresholdEnabled()) {
          retVal = new MemoryEventImpl[2];
          retVal[0] = new MemoryEventImpl(newEvent, MemoryEventType.EVICTION_UP);
          retVal[1] = newEvent;
        } else {
          retVal = new MemoryEventImpl[] {newEvent};
        }
        break;
      case CRITICAL_DOWN:
        if (newEvent.isLocal()) {
          // buildEvent should never have built a critical_down event
          Assert.assertTrue(false);
        } else {
          // This can occur in two scenarios
          // 1. This event was late. In this case, we did already generate a missing
          //    critical_down event, and this event should be ignored
          // 2. missed two events: eviction_up|eviction_disabled, critical_up
          // It will be impossible to distinguish, unless we maintain a history
          // of at least two previous events
          //TODO fix this?
        }
        break;
      case UNKNOWN:
        // the unknown event should never "slip through" from the caller
        Assert.assertTrue(false);
        break;
      default:
        throw new IllegalStateException("MemoryEventType " +newEvent.getType()+
                              "not considered in state machine");
      }
      return retVal;
    }
  },
  /**
   * Eviction threshold set to zero, disabling eviction events
   */
  EVICTION_DISABLED {
    @Override
    public MemoryEventImpl[] getMissingEvents(MemoryEventImpl newEvent) {
      //old state should not be DISABLED
      Assert.assertTrue(false);
      return null;
    }
  },
  /**
   * critical threshold exceeded
   */
  CRITICAL_UP {
    @Override
    public MemoryEventImpl[] getMissingEvents(MemoryEventImpl newEvent) {
      MemoryEventImpl[] retVal = null;
      switch (newEvent.getType()) {
      case EVICTION_UP:
        if (newEvent.isLocal()) {
          // buildEvent should never have built an eviction_up event
          Assert.assertTrue(false);
        } else {
          // This can occur in two scenarios
          // 1. This event was late. In this case, we did already generate a missing
          //    eviction_up event, and this event should be ignored
          // 2. missed two events: critical_down|critical_disabled, eviction_down
          // It will be impossible to distinguish, unless we maintain a history
          // of at least two previous events
          //TODO fix this?
        }
        break;
      case EVICTION_DOWN:
        if (newEvent.getThresholds().isCriticalThresholdEnabled()) {
          retVal = new MemoryEventImpl[2];
          retVal[0] = new MemoryEventImpl(newEvent, MemoryEventType.CRITICAL_DOWN);
          retVal[1] = newEvent;
        } else {
          retVal = new MemoryEventImpl[] {newEvent};
        }
        break;
      case CRITICAL_UP:
        //ignore
        break;
      case CRITICAL_DOWN:
        retVal = new MemoryEventImpl[] {newEvent};
        break;
      case UNKNOWN:
        // the unknown event should never "slip through" from the caller
        Assert.assertTrue(false);
        break;
      default:
        throw new IllegalStateException("MemoryEventType " +newEvent.getType()+
                          "not considered in state machine");
      }
      return retVal;
    }
  },
  /**
   * dropped below critical threshold
   */
  CRITICAL_DOWN {
    @Override
    public MemoryEventImpl[] getMissingEvents(MemoryEventImpl newEvent) {
      MemoryEventImpl[] retVal = null;
      switch (newEvent.getType()) {
      case EVICTION_UP:
        assert newEvent.getThresholds().isEvictionThresholdEnabled();
        retVal = new MemoryEventImpl[2];
        retVal[0] = new MemoryEventImpl(newEvent, MemoryEventType.EVICTION_DOWN);
        retVal[1] = newEvent;
        break;
      case EVICTION_DOWN:
        retVal = new MemoryEventImpl[] {newEvent};
        break;
      case CRITICAL_UP:
        retVal = new MemoryEventImpl[] {newEvent};
        break;
      case CRITICAL_DOWN:
        //ignore
        break;
      case UNKNOWN:
        // the unknown event should never "slip through" from the caller
        Assert.assertTrue(false);
        break;
      default:
        throw new IllegalStateException("MemoryEventType " +newEvent.getType()+
                          "not considered in state machine");
      }
      return retVal;
    }
  },
  /**
   * Critical threshold set to zero, disabling critical events
   */
  CRITICAL_DISABLED {
    @Override
    public MemoryEventImpl[] getMissingEvents(MemoryEventImpl newEvent) {
      //old state should not be DISABLED
      Assert.assertTrue(false);
      return null;
    }
  };

  /**
   * @param newEvent the new event
   * @return any events that may have been missed between this and the newEvent,
   * null if the newEvent should not be delivered
   */
  public abstract MemoryEventImpl[] getMissingEvents(MemoryEventImpl newEvent);

  public boolean isDisabledType() {
    if (this == EVICTION_DISABLED ||
        this == CRITICAL_DISABLED) {
      return true;
    }
    return false;
  }

  public boolean isEvictionDisabled() {
    return this == EVICTION_DISABLED;
  }

  public boolean isCriticalDisabled() {
    return this == CRITICAL_DISABLED;
  }

  public boolean isCriticalType() {
    return this == CRITICAL_UP ||
          this == CRITICAL_DOWN;
  }

  public boolean isEvictionType() {
    return this == EVICTION_UP ||
          this == EVICTION_DOWN;
  }

  public boolean isEvictionUp() {
    return this == EVICTION_UP;
  }

  public boolean isEvictionDown() {
    return this == EVICTION_DOWN;
  }

  public boolean isCriticalUp() {
    return this == CRITICAL_UP;
  }

  public boolean isCriticalDown() {
    return this == CRITICAL_DOWN;
  }

  public boolean isUnknown() {
    return this == UNKNOWN;
  }

  public boolean isEvictMore() {
    return this == EVICT_MORE;
  }

  public boolean isEvictionOrCriticalUpEvent() {
    return this == EVICTION_UP || this == CRITICAL_UP;
  }

  public static MemoryEventType fromData(DataInput in) throws IOException, ClassNotFoundException {
    return (MemoryEventType) DataSerializer.readObject(in);
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(this, out);
  }
  
  public static long getThresholdBytesForForcedEvents(MemoryEventType type, Thresholds t) {
    long retVal = 0;
    switch (type) {
    case EVICTION_UP:
      retVal = t.getEvictionThresholdBytes();
      break;
    case CRITICAL_UP:
      retVal = t.getCriticalThresholdBytes();
      break;
    default:
      throw new IllegalStateException("EVICTION_UP and CRITICAL_UP are the only forced events");
    }
    return retVal;
  }
}
