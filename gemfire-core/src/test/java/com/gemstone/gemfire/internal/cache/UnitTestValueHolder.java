package com.gemstone.gemfire.internal.cache;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class UnitTestValueHolder implements Externalizable {
  private Object value;
  public UnitTestValueHolder() {
  }
  public UnitTestValueHolder(Object v) {
    this.value = v;
  }
  public Object getValue() {
    return this.value;
  }
  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(this.value);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    this.value = in.readObject();
  }
}