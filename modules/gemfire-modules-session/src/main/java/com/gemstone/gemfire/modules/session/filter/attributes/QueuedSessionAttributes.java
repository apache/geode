/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.session.filter.attributes;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.Instantiator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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
