/*========================================================================= 
 * (c)Copyright 2002-2011, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.query.cq;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.query.CqEvent;
import com.gemstone.gemfire.cache.query.CqListener;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnNotFoundException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnValueNotAvailableException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionRepository;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryUtil;
import com.gemstone.gemfire.mgmt.DataBrowser.query.ResultSet;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.event.ErrorEvent;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.event.ICQEvent;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.event.RowAdded;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.event.RowDeleted;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.event.RowUpdated;
import com.gemstone.gemfire.mgmt.DataBrowser.query.internal.PdxHelper;

public final class CQResult implements ResultSet, CqListener {
  private List<CQEventListener>            listeners;
  private List<TypeListener>               type_listeners;
  private Map<Object, IntrospectionResult> types;
  private Map<Object, EventDataImpl>       key_to_value_map;

  public CQResult() {
    this.types = Collections
        .synchronizedMap(new HashMap<Object, IntrospectionResult>());
    this.key_to_value_map = Collections
        .synchronizedMap(new HashMap<Object, EventDataImpl>());

    this.listeners = Collections
        .synchronizedList(new ArrayList<CQEventListener>());
    this.type_listeners = Collections
        .synchronizedList(new ArrayList<TypeListener>());
  }

  public void close() {
    Collection<CQEventListener> _listeners = getCQEventListeners();
    for (CQEventListener listener : _listeners) {
      listener.close();
    }
  }

  public Object getColumnValue(Object tuple, int index)
      throws ColumnNotFoundException, ColumnValueNotAvailableException {
    Object type = (tuple instanceof Struct) ? ((Struct) tuple).getStructType()
        : tuple.getClass();

    if (this.types.containsKey(type)) {
      IntrospectionResult metaInfo = this.types.get(type);
      return metaInfo.getColumnValue(tuple, index);
    }

    throw new ColumnNotFoundException("Could not identify type :" + type);
  }

  public IntrospectionResult[] getIntrospectionResult() {
    return this.types.values().toArray(new IntrospectionResult[0]);
  }

  public Collection<?> getQueryResult() {
    throw new UnsupportedOperationException();
  }

  public Collection<?> getQueryResult(IntrospectionResult metaInfo) {
    throw new UnsupportedOperationException();
  }

  public boolean isEmpty() {
    return this.key_to_value_map.isEmpty();
  }

  public void addCQEventListener(CQEventListener listener) {
    this.listeners.add(listener);
  }

  public void removeCQEventListener(CQEventListener listener) {
    this.listeners.remove(listener);
  }

  public void addTypeListener(TypeListener listener) {
    this.type_listeners.add(listener);
  }

  public void removeTypeListener(TypeListener listener) {
    this.type_listeners.remove(listener);
  }

  public void onEvent(CqEvent cqEvent) {
    Object key = cqEvent.getKey();

    // Here the entry is destroyed from the result set.
    if (cqEvent.getQueryOperation().isDestroy()) {
      // Clear the key_to_value map.
      EventData data = this.key_to_value_map.remove(key);
      if (data != null) {
        RowDeleted delEvent = new RowDeleted(data);
        invokeListeners(delEvent);
      }
    } else {
      // Add/Update the entry operation.
      Object value = cqEvent.getNewValue();
      // The new entry value should be not-null for this.
      if (value != null) {
        try {
          // Get the type of the object and invoke listeners if the new type is
          // detected.
          IntrospectionResult metaInfo = getObjectType(value);

          EventDataImpl oldEvent = this.key_to_value_map.get(key);
          // Old entry is available for this key.
          if (oldEvent != null) {
            IntrospectionResult oldMetaInfo = oldEvent.getIntrospectionResult();
            // Types are same. Hence just update the record.
            if (metaInfo.equals(oldMetaInfo)) {
              oldEvent.setValue(metaInfo, value);
              invokeListeners(new RowUpdated(oldEvent));

            } else {
              // Types are different. Delete the old record and add the new one.
              EventDataImpl temp = (EventDataImpl) oldEvent.clone();
              invokeListeners(new RowDeleted(temp));

              oldEvent.setValue(metaInfo, value);
              invokeListeners(new RowAdded(oldEvent));
            }

          } else {
            // There is no old entry for this key. Add it as a new one.
            EventDataImpl newValue = new EventDataImpl(key, value, metaInfo);
            this.key_to_value_map.put(key, newValue);
            invokeListeners(new RowAdded(newValue));
          }
        } catch (Exception e) {
          ErrorEvent errorEvent = new ErrorEvent(e);
          invokeListeners(errorEvent);
        }
      }
    }
  }

  public void onError(CqEvent cqEvent) {
    // Propagate the exception.
    invokeListeners(new ErrorEvent(cqEvent.getThrowable()));
  }

  public EventData getValueForKey(Object key) {
    return this.key_to_value_map.get(key);
  }

  public Set<Object> getKeys() {
    Set<Object> keys = new HashSet<Object>();
    synchronized (this.key_to_value_map) {
      keys.addAll(this.key_to_value_map.keySet());
    }

    return keys;
  }

  private void invokeListeners(ICQEvent event) {
    Collection<CQEventListener> _listeners = getCQEventListeners();

    for (CQEventListener listener : _listeners) {
      listener.onEvent(event);
    }
  }

  private IntrospectionResult getObjectType(Object value)
      throws IntrospectionException {
    Object type = value.getClass();
    if (QueryUtil.isPdxInstance(value)) {
      //PdxInfoType is used for PdxInstance
      type = PdxHelper.getInstance().getPdxInfoType(value);
    }
    IntrospectionResult metaInfo = this.types.get(type);
    if (metaInfo == null) {
      metaInfo = IntrospectionRepository.singleton().introspectTypeByObject(value);
      this.types.put(type, metaInfo);

      notifyTypeListeners(metaInfo);
    }

    return metaInfo;
  }

  /**
   * Notifies the available TypeListeners about newly added IntrospectionResult
   * 
   * @param metaInfo
   *          newly added IntrospectionResult
   */
  private void notifyTypeListeners(IntrospectionResult metaInfo) {
    Collection<TypeListener> typeListeners = getTypeListeners();

    for (TypeListener listener : typeListeners) {
      listener.onNewTypeAdded(metaInfo);
    }
  }

  private Collection<CQEventListener> getCQEventListeners() {
    List<CQEventListener> result = new ArrayList<CQEventListener>();

    synchronized (this.listeners) {
      result.addAll(this.listeners);
    }

    return result;
  }

  private Collection<TypeListener> getTypeListeners() {
    List<TypeListener> result = new ArrayList<TypeListener>();

    synchronized (this.listeners) {
      result.addAll(this.type_listeners);
    }

    return result;
  }

}
