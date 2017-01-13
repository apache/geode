/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;

import org.apache.geode.DataSerializable;
import org.apache.geode.Delta;
import org.apache.geode.DataSerializer;
import org.apache.geode.Instantiator;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.cache.Declarable;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.*;
import java.io.*;
import java.util.*;


public class DeltaPSTObject implements Declarable,Serializable, DataSerializable, Delta, Cloneable
{
	private long timestamp;
	private int field1;
	private char field2;
	private byte[] byteArray;
	
	static {
	     Instantiator.register(new Instantiator(DeltaPSTObject.class, (byte) 42) {
	       public DataSerializable newInstance() {
	           return new DeltaPSTObject();
	       }
	    });
	  }

	  public void init(Properties props) {
	    this.field1 = Integer.parseInt(props.getProperty("field1"));
	  }

 public DeltaPSTObject() {
 }
 // no need to write init or other constructor . 
 // we just need to register the PSTObject/ DeltaPSTObject at server side via cache xml.
	  public boolean equals(Object o) {
	    if (o instanceof DeltaPSTObject) {
	    	DeltaPSTObject other = (DeltaPSTObject) o;
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
	    if (o instanceof DeltaPSTObject) {
	    	DeltaPSTObject obj = (DeltaPSTObject)o;
	      return ObjectSizer.DEFAULT.sizeof(obj.timestamp)
	                   + ObjectSizer.DEFAULT.sizeof(obj.field1)
	                   + ObjectSizer.DEFAULT.sizeof(obj.field2)
	                   + ObjectSizer.DEFAULT.sizeof(obj.byteArray);
	    } else {
	      return ObjectSizer.DEFAULT.sizeof(o);
	    }
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
//------------------------------------------------------------------------------
//Delta

 public boolean hasDelta() {
   return true;
 }

 public void toDelta(DataOutput out)
 throws IOException {
   out.writeInt(this.field1);
   out.writeLong(this.timestamp);
 }

 public void fromDelta(DataInput in)
 throws IOException {
   this.field1 = in.readInt();
   this.timestamp = in.readLong();
 }

 
//------------------------------------------------------------------------------
//Cloneable

 /**
  * Makes a deep copy of this object.
  */
 public Object clone() throws CloneNotSupportedException {
   DeltaPSTObject obj = (DeltaPSTObject)super.clone();
   this.byteArray = new byte[this.byteArray.length];
   System.arraycopy(this.byteArray, 0, obj.byteArray, 0,
                    this.byteArray.length);
   return obj;
 }
}
