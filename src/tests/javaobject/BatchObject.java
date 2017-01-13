/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package javaobject;

import org.apache.geode.*;
import org.apache.geode.cache.Declarable;
import org.apache.geode.internal.NanoTimer;
import java.util.*;
import java.io.*;


public class BatchObject implements Declarable,Serializable,DataSerializable{
  private int index;
  private long timestamp;
  private int batch;
  private byte[] byteArray;

  
  static {
     Instantiator.register(new Instantiator(BatchObject.class, (byte)25) {
      public DataSerializable newInstance() {
        return new BatchObject();
      }
    });
  }

  public void init(Properties props) {
	this.index = Integer.parseInt(props.getProperty("index"));
	this.batch = Integer.parseInt(props.getProperty("batch"));
  }
  
  public BatchObject() {
  }

  /**
   * Initializes a BatchObject.
   * @param index the value to encode into the BatchObject.
   *
   * @throws ObjectCreationException
   *         <code>index</code> cannot be encoded
   */
  public void BatchObject(int anIndex,int batchSize,int size) {
    this.index = anIndex;
    this.timestamp = NanoTimer.getTime();
    this.batch = anIndex/batchSize;
    this.byteArray = new byte[size];
  }

  /**
   * Returns the index.
   */
  public int getIndex() {
    return this.index;
  }

  /**
   * Returns the batch.
   */
  public int getBatch() {
    return this.batch;
  }

   public long getTimestamp() {
    return this.timestamp;
  }

  public void resetTimestamp() {
    this.timestamp = NanoTimer.getTime();
  }

  public String toString() {
    if (this.byteArray == null) {
      return "BatchObject@" + this.timestamp;
    } else {
      return "BatchObject(" + this.getIndex() + ", " + this.getBatch() + ")@"
                            + this.timestamp;
    }
  }

  public boolean equals(Object o) {
    if (o instanceof BatchObject) {
      BatchObject other = (BatchObject) o;
      if (this.index == other.index) {
        if (this.timestamp == other.timestamp) {
          if (this.batch == other.batch) {
	    return true;
	  }
	}
      }
    }

    return false;
  }

  // DataSerializable
  public void toData(DataOutput out)
  throws IOException {
    out.writeInt(this.index);
    out.writeLong(this.timestamp);
    out.writeInt(this.batch);
    DataSerializer.writeObject(this.byteArray, out);
  }
  public void fromData(DataInput in)
  throws IOException, ClassNotFoundException {
    this.index = in.readInt();
    this.timestamp = in.readLong();
    this.batch = in.readInt();
    this.byteArray = DataSerializer.readObject(in);
  }
}
