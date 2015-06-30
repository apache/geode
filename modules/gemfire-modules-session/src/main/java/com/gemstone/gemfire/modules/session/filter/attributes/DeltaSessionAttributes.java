/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.session.filter.attributes;

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
