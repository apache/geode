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
 * This class implements synchronous attribute delta propagation. Updates to
 * attributes are immediately propagated.
 */
public class DeltaSessionAttributes extends AbstractDeltaSessionAttributes {

  private static final Logger LOG =
      LoggerFactory.getLogger(DeltaSessionAttributes.class.getName());

  /**
   * Register ourselves for de-serialization
   */
  static {
    Instantiator.register(new Instantiator(DeltaSessionAttributes.class, 347) {
      @Override
      public DataSerializable newInstance() {
        return new DeltaSessionAttributes();
      }
    });
  }

  /**
   * Default constructor
   */
  public DeltaSessionAttributes() {
  }

  /**
   * {@inheritDoc} Put an attribute, setting the dirty flag and immediately
   * flushing the delta queue.
   */
  @Override
  public Object putAttribute(String attr, Object value) {
    Object obj = attributes.put(attr, value);
    deltas.put(attr, new DeltaEvent(true, attr, value));
    flush();
    return obj;
  }

  /**
   * {@inheritDoc} Remove an attribute, setting the dirty flag and immediately
   * flushing the delta queue.
   */
  @Override
  public Object removeAttribute(String attr) {
    Object obj = attributes.remove(attr);
    deltas.put(attr, new DeltaEvent(false, attr, null));
    flush();
    return obj;
  }
}
