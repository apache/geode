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
