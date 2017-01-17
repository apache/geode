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
import org.apache.geode.Delta;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.Declarable;
import org.apache.geode.Instantiator;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;

public class DeltaExample implements DataSerializable, Delta, Declarable
{
	private int m_field1;
    private int m_field2;
    private int m_field3;
    private int index;

	private transient boolean m_f1set;
  private transient boolean m_f2set;
  private transient boolean m_f3set;

	static
	{
		Instantiator.register(new Instantiator(DeltaExample.class, (byte)2)
		{
			public DataSerializable newInstance()
			{
				return new DeltaExample();
			}
		});
	}
	public DeltaExample()
	{
    reset();
	}
	public DeltaExample(int field1, int field2, int field3)
	{
		m_field1 = field1;
		m_field2 = field2;
		m_field3 = field3;
    reset();
	}
	public boolean hasDelta()
	{
		return m_f1set || m_f2set || m_f3set;
	}
  
  private void reset()
  {
    m_f1set = false;
    m_f2set = false;
    m_f3set = false;
  }

	public void fromData(DataInput in) throws IOException
	{
		m_field1 = in.readInt();
		m_field2 = in.readInt();
		m_field3 = in.readInt();
    reset();
	}

	public void toData(DataOutput out) throws IOException
	{
		out.writeInt(m_field1);
		out.writeInt(m_field2);
		out.writeInt(m_field3);
	}

	public void fromDelta(DataInput in) throws IOException
	{
    m_f1set = in.readBoolean();
    if (m_f1set) {
      m_field1 = in.readInt();
    }
    
    m_f2set = in.readBoolean();
    if (m_f2set) {
      m_field2 = in.readInt();
    }
    
    m_f3set = in.readBoolean();
    if (m_f3set) {
      m_field3 = in.readInt();
    }
    
    reset();
	}

	public void toDelta(DataOutput out) throws IOException
	{
		out.writeBoolean(m_f1set);
    if (m_f1set) {
      out.writeInt(m_field1);
    }
    
    out.writeBoolean(m_f2set);
    if (m_f2set) {
      out.writeInt(m_field2);
    }
    
    out.writeBoolean(m_f3set);
    if (m_f3set) {
      out.writeInt(m_field3);
    }
		
    reset();
	}

	public void init(Properties props)
	{
		m_field1 = 0;
		m_field2 = 0;
		m_field3 = 0;
    reset();
	}
};
