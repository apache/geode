package com.gemstone.gemfire.internal.redis;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

/**
 * This is a wrapper class for doubles, similar to {@link ByteArrayWrapper}
 * 
 * @author Vitaliy Gavrilov
 *
 */
public class DoubleWrapper implements DataSerializable, Comparable<Object> {

  private static final long serialVersionUID = 6946858357297398633L;

  public Double score;
  private String toString;
  
  public DoubleWrapper() {}

  public DoubleWrapper(Double dubs) {
    this.score = dubs;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeDouble(score, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.score = DataSerializer.readDouble(in);
  }

  @Override
  public int compareTo(Object arg0) {
    Double other;
    if (arg0 instanceof DoubleWrapper) {
      other = ((DoubleWrapper) arg0).score;
    } else if (arg0 instanceof Double) {
      other = (Double) arg0;
    } else
      return 0;
    Double diff = this.score - other;
    if (diff > 0)
      return 1;
    else if (diff < 0)
      return -1;
    else
      return 0;
  }

  public String toString() {
    if (this.toString == null)
      this.toString = Coder.doubleToString(score);
    return this.toString;
  }
  
}
