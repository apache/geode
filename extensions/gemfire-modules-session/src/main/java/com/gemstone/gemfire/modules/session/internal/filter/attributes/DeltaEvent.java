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
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.util.BlobHelper;
import com.gemstone.gemfire.modules.session.internal.filter.GemfireHttpSession;
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
