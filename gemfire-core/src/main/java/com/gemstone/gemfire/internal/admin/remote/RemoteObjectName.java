/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
   
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.*;
//import com.gemstone.gemfire.cache.*;
//import com.gemstone.gemfire.internal.*;
//import com.gemstone.gemfire.internal.admin.*;
//import com.gemstone.gemfire.distributed.internal.*;
import java.io.*;
//import java.util.*;

/**
 * Used to name an object in a region. This class is needed so that the
 * console will not need to load the user defined classes.
 */
public class RemoteObjectName implements DataSerializable {
  private static final long serialVersionUID = 5076319310507575418L;
  private String className;
  private String value;
  private int hashCode;

  public RemoteObjectName(Object name) {
    className = name.getClass().getName();
    value = name.toString();
    hashCode = name.hashCode();
  }

  /**
   * This constructor is only for use by the DataSerializable mechanism
   */
  public RemoteObjectName() {}

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (o instanceof RemoteObjectName) {
      RemoteObjectName n = (RemoteObjectName)o;
      return (hashCode == n.hashCode)
        && className.equals(n.className) && value.equals(n.value);
    } else {
      // this should only happen on the server side when we are trying
      // to find the original object
      if (hashCode != o.hashCode()) {
        return false;
      }
      if (!className.equals(o.getClass().getName())) {
        return false;
      }
      return value.equals(o.toString());
    }
  }
  
  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public String toString() {
    return className + " \"" + value + "\"";
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.className, out);
    DataSerializer.writeString(this.value, out);
    out.writeInt(this.hashCode);
  }

  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    this.className = DataSerializer.readString(in);
    this.value = DataSerializer.readString(in);
    this.hashCode = in.readInt();
  }

}
