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

public class DeltaEx implements DataSerializable, Delta, Declarable {

  private int counter = 0;

  private boolean isDelta = false;

  static {
     Instantiator.register(new Instantiator(DeltaEx.class, (byte) 1) {
     public DataSerializable newInstance() {
        return new DeltaEx();
     }
   });
  }

  public boolean hasDelta( )
  {
    return isDelta;
  }

  public void fromData( DataInput in ) throws IOException
  {
    counter = in.readInt( );
    isDelta = false;
  }

  public void toData( DataOutput out ) throws IOException
  {
    out.writeInt( counter );
  }

  public void fromDelta( DataInput in ) throws IOException
  {
    int delta1 = in.readInt( );
    if ( delta1 == 0 )
      throw new InvalidDeltaException( );

    counter += delta1;
    isDelta = true;
  }

  public void toDelta( DataOutput out ) throws IOException
  {
    out.writeInt( 1 );
  }

  public void init( Properties props )
  {
    counter = 0;
    isDelta = false;
  }

};
