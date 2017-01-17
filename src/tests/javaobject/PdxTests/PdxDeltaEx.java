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
package PdxTests;

import org.apache.geode.DataSerializable;
import org.apache.geode.Delta;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.Declarable;
import org.apache.geode.Instantiator;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;

import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;

public class PdxDeltaEx implements PdxSerializable, Delta {

  private int counter = 0;

  private boolean isDelta = false;

  public PdxDeltaEx() { }
  public PdxDeltaEx(int cnt)
  {
    counter = cnt;
  }
  
  public boolean hasDelta( )
  {
    return isDelta;
  }

  public void fromData( PdxReader reader ) 
  {
    counter = reader.readInt("counter");
    isDelta = false;
  }

  public void toData(PdxWriter writer) 
  {
    writer.writeInt("counter", counter);
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
