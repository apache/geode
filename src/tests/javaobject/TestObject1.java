/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Instantiator;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;


public class TestObject1 implements DataSerializable
{
  static {
     Instantiator.register(new Instantiator(TestObject1.class, (byte) 31) {
     public DataSerializable newInstance() {
        return new TestObject1();
     }
   });
  }
  
	String name;

	byte arr[] = new byte[1024*4];

	int identifier;

	public TestObject1() { }

	public TestObject1(String objectName, int objectIndentifier)
	{
		this.name = objectName;
		Arrays.fill(this.arr, (byte)'A');
		this.identifier = objectIndentifier;
	}

	public int hashCode()
	{
		return this.identifier;
	}

	public boolean equals(TestObject1 obj)
	{
		return (this.name.equals(obj.name)
			  && Arrays.equals(this.arr, obj.arr));
	}

	public void toData(DataOutput out) throws IOException
	{
		DataSerializer.writeByteArray(this.arr, out);
		DataSerializer.writeString(this.name, out);
		out.writeInt(this.identifier);
	}

	public void fromData(DataInput in) throws IOException, ClassNotFoundException
	{
		this.arr = DataSerializer.readByteArray(in);
		this.name = DataSerializer.readString(in);
		this.identifier = in.readInt();
	}
}
