/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.security;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

public class ObjectWithAuthz implements DataSerializable {
  private static final long serialVersionUID = -9016665470672291858L;

  public static final byte CLASSID = (byte)57;

  private Object val;

  private Object authz;

  public ObjectWithAuthz() {
    this.val = null;
    this.authz = null;
  }

  public ObjectWithAuthz(Object val, Object authz) {

    this.val = val;
    this.authz = authz;
  }

  public Object getVal() {

    return this.val;
  }

  public Object getAuthz() {

    return this.authz;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {

    this.val = DataSerializer.readObject(in);
    this.authz = DataSerializer.readObject(in);
  }

  public void toData(DataOutput out) throws IOException {

    DataSerializer.writeObject(this.val, out);
    DataSerializer.writeObject(this.authz, out);
  }

}
