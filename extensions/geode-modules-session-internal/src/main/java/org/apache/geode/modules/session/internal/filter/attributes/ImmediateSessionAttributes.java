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
 * This class implements immediately transmitted attributes. All attributes are
 * transmitted for every attribute update. This is bound to be a performance hit
 * in some cases but ensures much higher data availability.
 */
public class ImmediateSessionAttributes extends AbstractSessionAttributes {

  private static final Logger LOG =
      LoggerFactory.getLogger(ImmediateSessionAttributes.class.getName());

  /**
   * Register ourselves for de-serialization
   */
  static {
    Instantiator.register(
        new Instantiator(ImmediateSessionAttributes.class, 347) {
          @Override
          public DataSerializable newInstance() {
            return new ImmediateSessionAttributes();
          }
        });
  }

  /**
   * Default constructor
   */
  public ImmediateSessionAttributes() {
  }

  @Override
  public Object putAttribute(String attr, Object value) {
    Object obj = attributes.put(attr, value);
    flush();
    return obj;
  }

  @Override
  public Object removeAttribute(String attr) {
    Object obj = attributes.remove(attr);
    flush();
    return obj;
  }
}
