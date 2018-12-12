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

package org.apache.geode.cache.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.geode.cache.query.internal.cq.CqAttributesImpl;

/**
 * The factory class for the CqAttributes instance. This provides the CqListener setter methods.
 * This class maintains state for and creates new instances of the CqAttributes interface for new
 * CqQuery instances. If you create a factory with the default constructor, then the factory is set
 * up to create attributes with all default settings. You can also create a factory by providing a
 * <code>CqAttributes</code>, which will set up the new factory with the settings provided in that
 * attributes instance.
 *
 * <p>
 * Once a <code>CqAttributes</code> is created, it can only be modified after it has been used to
 * create a <code>CqQuery</code>.
 *
 * @see CqAttributes
 *
 * @since GemFire 5.5
 */
public class CqAttributesFactory {

  /* Handle for CqAttributes. */
  private final CqAttributesImpl cqAttributes = new CqAttributesImpl();

  /**
   * Creates a new instance of AttributesFactory ready to create a <code>CqAttributes</code> with
   * default settings.
   */
  public CqAttributesFactory() {}

  /**
   * Creates a new instance of CqAttributesFactory ready to create a <code>CqAttributes</code> with
   * the same settings as those in the specified <code>CqAttributes</code>.
   *
   * @param cqAttributes the <code>CqAttributes</code> used to initialize this AttributesFactory
   */
  public CqAttributesFactory(CqAttributes cqAttributes) {
    this.cqAttributes.setCqListeners(new ArrayList(Arrays.asList(cqAttributes.getCqListeners())));
  }

  /**
   * Adds a CQ listener to the end of the list of cq listeners on this factory.
   *
   * @param cqListener the CqListener to add to the factory.
   * @throws IllegalArgumentException if <code>cqListener</code> is null
   */
  public void addCqListener(CqListener cqListener) {
    if (cqListener == null) {
      throw new IllegalArgumentException(
          "addCqListener parameter was null");
    }
    this.cqAttributes.addCqListener(cqListener);
  }

  /**
   * Removes all Cq listeners and then adds each listener in the specified array.
   *
   * @param cqListeners a possibly null or empty array of listeners to add to this factory.
   * @throws IllegalArgumentException if the <code>cqListeners</code> array has a null element
   */
  public void initCqListeners(CqListener[] cqListeners) {
    if (cqListeners == null || cqListeners.length == 0) {
      this.cqAttributes.setCqListeners(null);
    } else {
      List nl = Arrays.asList(cqListeners);
      if (nl.contains(null)) {
        throw new IllegalArgumentException(
            "initCqListeners parameter had a null element");
      }
      this.cqAttributes.setCqListeners(new ArrayList(nl));
    }
  }

  /**
   * Creates a <code>CqAttributes</code> with the current settings.
   *
   * @return the newly created <code>CqAttributes</code>
   */
  public CqAttributes create() {
    return (CqAttributes) this.cqAttributes.clone();
  }


}
