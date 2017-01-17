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
