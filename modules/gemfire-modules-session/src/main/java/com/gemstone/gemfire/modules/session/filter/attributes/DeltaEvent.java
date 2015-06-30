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
import com.gemstone.gemfire.internal.util.BlobHelper;
import com.gemstone.gemfire.modules.session.filter.GemfireHttpSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Capture the update to a particular name
 */
public class DeltaEvent implements DataSerializable {

  private static final Logger LOG =
      LoggerFactory.getLogger(DeltaEvent.class.getName());
  /**
   * The event is either an update (true) or a remove (false)
   */
  private boolean update;

  private String name;

  private Object value = null;

  private GemfireHttpSession session = null;

  /**
   * Constructor for de-serialization only
   */
  public DeltaEvent() {
  }

  /**
   * Constructor which creates a 'deferred' event. This is used when the value
   * should only be applied when the object is serialized.
   *
   * @param session   the session from which the value ultimately will be
   *                  retrieved
   * @param attribute the name of the attribute
   */
  public DeltaEvent(GemfireHttpSession session, String attribute) {
    this.session = session;
    this.name = attribute;
    this.update = true;
  }

  public DeltaEvent(boolean update, String attribute, Object value) {
    this.update = update;
    this.name = attribute;
    this.value = value;
    blobifyValue();
  }

  private void blobifyValue() {
    if (value instanceof byte[]) {
      LOG.warn("Session attribute is already a byte[] - problems may "
          + "occur transmitting this delta.");
    }
    try {
      value = BlobHelper.serializeToBlob(value);
    } catch (IOException iox) {
      LOG.error("Attribute '" + name + "' value: " + value
          + " cannot be serialized due to the following exception", iox);
    }
  }

  public boolean isUpdate() {
    return update;
  }

  public String getName() {
    return name;
  }

  public Object getValue() {
    return value;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    if (session != null) {
      value = session.getNativeSession().getAttribute(name);
      blobifyValue();
    }
    out.writeBoolean(update);
    DataSerializer.writeString(name, out);
    DataSerializer.writeObject(value, out);
  }

  @Override
  public void fromData(
      DataInput in) throws IOException, ClassNotFoundException {
    update = in.readBoolean();
    name = DataSerializer.readString(in);
    value = DataSerializer.readObject(in);
  }
}
