/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.gemstone.gemfire.modules.session.internal.filter.attributes;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.Instantiator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements delayed attribute delta propagation. Updates to
 * attributes are only propagated once the session goes out of scope - i.e. as
 * the request is done being processed.
 */
public class DeltaQueuedSessionAttributes extends AbstractDeltaSessionAttributes {

  private static final Logger LOG =
      LoggerFactory.getLogger(DeltaQueuedSessionAttributes.class.getName());

  private Trigger trigger = Trigger.SET;

  private enum Trigger {
    SET,
    SET_AND_GET;
  }

  /**
   * Register ourselves for de-serialization
   */
  static {
    Instantiator.register(
        new Instantiator(DeltaQueuedSessionAttributes.class, 3479) {
          @Override
          public DataSerializable newInstance() {
            return new DeltaQueuedSessionAttributes();
          }
        });
  }

  /**
   * Default constructor
   */
  public DeltaQueuedSessionAttributes() {
  }

  public void setReplicationTrigger(String trigger) {
    this.trigger = Trigger.valueOf(trigger.toUpperCase());
  }

  @Override
  public Object getAttribute(String attr) {
    if (trigger == Trigger.SET_AND_GET) {
      deltas.put(attr, new DeltaEvent(session, attr));
    }
    return super.getAttribute(attr);
  }

  /**
   * {@inheritDoc} Put an attribute, setting the dirty flag. The changes are
   * flushed at the end of filter processing.
   */
  @Override
  public Object putAttribute(String attr, Object value) {
    Object obj = attributes.put(attr, value);
    deltas.put(attr, new DeltaEvent(true, attr, value));
    return obj;
  }

  /**
   * {@inheritDoc} Remove an attribute, setting the dirty flag. The changes are
   * flushed at the end of filter processing.
   */
  @Override
  public Object removeAttribute(String attr) {
    Object obj = attributes.remove(attr);
    deltas.put(attr, new DeltaEvent(false, attr, null));
    return obj;
  }
}
