/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal;

import java.io.*;

/**
 * Abstract class for subclasses that want to be Externalizable in
 * addition to being DataSerializableFixedID.
 * <p> Note: subclasses must also provide a zero-arg constructor
 *
 * @author Darrel Schneider
 * @since 5.7 
 */
public abstract class ExternalizableDSFID
  implements DataSerializableFixedID, Externalizable
{
  public abstract int getDSFID();
  public abstract void toData(DataOutput out) throws IOException;
  public abstract void fromData(DataInput in) throws IOException, ClassNotFoundException;

  public final void writeExternal(ObjectOutput out) throws IOException {
    toData(out);
  }
  public final void readExternal(ObjectInput in)
    throws IOException, ClassNotFoundException {
    fromData(in);
  }
}
