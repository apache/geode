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
 * This class implements an attribute container which delays sending updates
 * until the session goes out of scope. All attributes are transmitted during
 * the update.
 */
public class QueuedSessionAttributes extends AbstractSessionAttributes {

  private static final Logger LOG =
      LoggerFactory.getLogger(QueuedSessionAttributes.class.getName());

  /**
   * Register ourselves for de-serialization
   */
  static {
    Instantiator.register(new Instantiator(QueuedSessionAttributes.class, 347) {
      @Override
      public DataSerializable newInstance() {
        return new QueuedSessionAttributes();
      }
    });
  }

  /**
   * Default constructor
   */
  public QueuedSessionAttributes() {
  }

  @Override
  public Object putAttribute(String attr, Object value) {
    Object obj = attributes.put(attr, value);
    return obj;
  }

  @Override
  public Object removeAttribute(String attr) {
    Object obj = attributes.remove(attr);
    return obj;
  }
}
