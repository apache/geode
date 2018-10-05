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
package org.apache.geode.cache.query.internal.cq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.Logger;

import org.apache.geode.SystemFailure;
import org.apache.geode.cache.CacheCallback;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesMutator;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.internal.logging.LogService;

public class CqAttributesImpl implements CqAttributes, CqAttributesMutator, Cloneable {
  private static final Logger logger = LogService.getLogger();

  private volatile ArrayList<CqListener> cqListeners = null;

  private static final CqListener[] EMPTY_LISTENERS = new CqListener[0];

  /**
   * Used to synchronize access to cqListeners
   */
  private final Object clSync = new Object();

  /**
   * Returns the CqListeners set with the CQ
   *
   * @return CqListener[]
   */
  public CqListener[] getCqListeners() {
    final ArrayList listeners = this.cqListeners;
    if (listeners == null) {
      return CqAttributesImpl.EMPTY_LISTENERS;
    }

    CqListener[] result = null;
    synchronized (this.clSync) {
      result = new CqListener[listeners.size()];
      listeners.toArray(result);
    }
    return result;

  }

  /**
   * Returns the CqListener set with the CQ
   *
   */
  public CqListener getCqListener() {
    ArrayList<CqListener> listeners = this.cqListeners;
    if (listeners == null) {
      return null;
    }
    synchronized (this.clSync) {
      if (listeners.size() == 0) {
        return null;
      }
      if (listeners.size() == 1) {
        return listeners.get(0);
      }
    }
    throw new IllegalStateException(
        "More than one Cqlistener exists.");
  }

  @Override
  public Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException e) {
      throw new InternalError(
          "CloneNotSupportedException thrown in class that implements cloneable");
    }
  }

  /**
   * Adds a Cqlistener to the end of the list of Cqlisteners on this CqQuery.
   *
   * @param cql the user defined cq listener to add to the CqQuery.
   * @throws IllegalArgumentException if <code>aListener</code> is null
   */
  public void addCqListener(CqListener cql) {
    if (cql == null) {
      throw new IllegalArgumentException(
          "addCqListener parameter was null");
    }
    synchronized (this.clSync) {
      ArrayList<CqListener> oldListeners = this.cqListeners;
      if (oldListeners == null || oldListeners.size() == 0) {
        ArrayList<CqListener> al = new ArrayList<CqListener>(1);
        al.add(cql);
        this.setCqListeners(al);
      } else {
        if (!oldListeners.contains(cql)) {
          oldListeners.add(cql);
        }
      }
    }
  }

  /**
   * Removes all Cqlisteners, calling on each of them, and then adds each listener in the specified
   * array.
   *
   * @param addedListeners a possibly null or empty array of listeners to add to this CqQuery.
   * @throws IllegalArgumentException if the <code>newListeners</code> array has a null element
   */
  public void initCqListeners(CqListener[] addedListeners) {
    ArrayList<CqListener> oldListeners;
    synchronized (this.clSync) {
      oldListeners = this.cqListeners;
      if (addedListeners == null || addedListeners.length == 0) {
        this.setCqListeners(null);
      } else { // we have some listeners to add
        List nl = Arrays.asList(addedListeners);
        if (nl.contains(null)) {
          throw new IllegalArgumentException(
              "initCqListeners parameter had a null element");
        }
        this.setCqListeners(new ArrayList(nl));
      }
    }

    if (oldListeners != null) {
      CqListener cql = null;
      for (Iterator<CqListener> iter = oldListeners.iterator(); iter.hasNext();) {
        try {
          cql = iter.next();
          cql.close();
          // Handle client side exceptions.
        } catch (Exception ex) {
          logger.warn("Exception occurred while closing CQ Listener Error: {}",
              ex.getLocalizedMessage());
          if (logger.isDebugEnabled()) {
            logger.debug(ex.getMessage(), ex);
          }
        } catch (VirtualMachineError err) {
          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error. We're poisoned
          // now, so don't let this thread continue.
          throw err;
        } catch (Throwable t) {
          // Whenever you catch Error or Throwable, you must also
          // catch VirtualMachineError (see above). However, there is
          // _still_ a possibility that you are dealing with a cascading
          // error condition, so you also need to check to see if the JVM
          // is still usable:
          SystemFailure.checkFailure();
          logger.warn("Runtime Exception occurred while closing CQ Listener Error: {}",
              t.getLocalizedMessage());
          if (logger.isDebugEnabled()) {
            logger.debug(t.getMessage(), t);
          }
        }
      }
    }
  }

  /**
   * Removes a Cqlistener from the list of Cqlisteners on this CqQuery. Does nothing if the
   * specified listener has not been added. If the specified listener has been added then
   * {@link CacheCallback#close()} will be called on it; otherwise does nothing.
   *
   * @param cql the Cqlistener to remove from the CqQuery.
   * @throws IllegalArgumentException if <code>cl</code> is null
   */
  public void removeCqListener(CqListener cql) {
    if (cql == null) {
      throw new IllegalArgumentException(
          "removeCqListener parameter was null");
    }
    synchronized (this.clSync) {
      ArrayList<CqListener> oldListeners = this.cqListeners;
      if (oldListeners != null) {
        if (oldListeners.remove(cql)) {
          if (oldListeners.isEmpty()) {
            this.setCqListeners(null);
          }
          try {
            cql.close();
            // Handle client side exceptions.
          } catch (Exception ex) {
            logger.warn("Exception closing CQ Listener Error: {}",
                ex.getLocalizedMessage());
            if (logger.isDebugEnabled()) {
              logger.debug(ex.getMessage(), ex);
            }
          } catch (VirtualMachineError err) {
            SystemFailure.initiateFailure(err);
            // If this ever returns, rethrow the error. We're poisoned
            // now, so don't let this thread continue.
            throw err;
          } catch (Throwable t) {
            // Whenever you catch Error or Throwable, you must also
            // catch VirtualMachineError (see above). However, there is
            // _still_ a possibility that you are dealing with a cascading
            // error condition, so you also need to check to see if the JVM
            // is still usable:
            SystemFailure.checkFailure();
            logger.warn("Runtime Exception occurred closing CQ Listener Error: {}",
                t.getLocalizedMessage());
            if (logger.isDebugEnabled()) {
              logger.debug(t.getMessage(), t);
            }
          }
        }
      }
    }
  }

  public void setCqListeners(ArrayList<CqListener> cqListeners) {
    synchronized (this.clSync) {
      this.cqListeners = cqListeners;
    }
  }
}
