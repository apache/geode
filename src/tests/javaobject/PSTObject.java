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

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
//import org.apache.geode.Instantiator;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.cache.Declarable;
import java.io.*;
import java.util.Properties;
import java.lang.Exception;
import org.apache.geode.*;

/**
 * An object containing a timestamp, a byte array of configurable size and primitive fields.
 */

public class PSTObject implements  Declarable,Serializable,DataSerializable
{
  protected long timestamp;
  protected int field1;
  protected char field2;
  protected byte[] byteArray;

  // INSTANTIATORS DISABLED due to bug 35646
  //
  //static {
  //  Instantiator.register(new Instantiator(PSTObject.class, (byte)23) {
  //    public DataSerializable newInstance() {
  //      return new PSTObject();
  //    }
  //  });
  //}

  public PSTObject() {
  }


  static {
	Instantiator.register(new Instantiator(PSTObject.class, (byte)4) {
	  public DataSerializable newInstance() {
	    return new PSTObject();
	  }
	});
   }
  /**
   * Initializes a PSTObject.
   */
  public void init( Properties props ) {
	  this.field1 = Integer.parseInt(props.getProperty("field1"));
  }

  

  public void incrementField1() {
    ++this.field1;
  }

  public synchronized void update() {
    incrementField1();
    resetTimestamp();
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  public void resetTimestamp() {
    this.timestamp = NanoTimer.getTime();
  }

  public String toString() {
    return "PSTObject";
  }

  /**
   * Two <code>PSTObject</code>s are considered to be equal if they have
   * the same values for field1, field2 and timestamp.
   * This provides stronger validation than the {@link #validate}
   * method that only considers the index.
   */
  public boolean equals(Object o) {
    if (o instanceof PSTObject) {
      PSTObject other = (PSTObject) o;
      if (this.timestamp == other.timestamp) {
	if ((this.field1 == other.field1) &&
	    (this.field2 == other.field2) ) {
	  return true;
	}
      }
    }

    return false;
  }

  // ObjectSizer
  public int sizeof(Object o) {
    if (o instanceof PSTObject) {
      PSTObject obj = (PSTObject)o;
      return ObjectSizer.DEFAULT.sizeof(obj.timestamp)
                   + ObjectSizer.DEFAULT.sizeof(obj.field1)
                   + ObjectSizer.DEFAULT.sizeof(obj.field2)
                   + ObjectSizer.DEFAULT.sizeof(obj.byteArray);
    } else {
      return ObjectSizer.DEFAULT.sizeof(o);
    }
  }
  public int hashCode() {
    int result = 17;
    result = 37 * result + (int)timestamp;
    result = 37 * result + field1;
    result = 37 * result + field2;
    return result;
  }
  // DataSerializable
  public void toData( DataOutput out )
  throws IOException {
    out.writeLong( this.timestamp );
    out.writeInt( this.field1 );
    out.writeChar( this.field2 );
    DataSerializer.writeByteArray(this.byteArray, out);
  }
  public void fromData( DataInput in )
  throws IOException, ClassNotFoundException {
    this.timestamp = in.readLong();
    this.field1 = in.readInt();
    this.field2 = in.readChar();
    this.byteArray = DataSerializer.readByteArray( in );
  }
}
