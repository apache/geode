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

import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;

  public class PdxDelta implements PdxSerializable, Delta
  {
    public static int GotDelta = 0;
    int _delta = 0;
    int _id;

    public PdxDelta() { }
    public PdxDelta(int id)
    {
      _id = id;
    }

    
   public void fromData(PdxReader reader)
    {
      _id = reader.readInt("_id");
      _delta = reader.readInt("_delta");
    }

    public void toData(PdxWriter writer) 
    {
      writer.writeInt("_id", _id);
      writer.writeInt("_delta", _delta);
    }
    


    public void fromDelta( DataInput input ) throws IOException
    {
      _delta = input.readInt();
      GotDelta++;
    }

    public boolean hasDelta()
    {
      if (_delta > 0)
      {
        _delta++;
        return true;
      }
      else
      {
        _delta++;
        return false;
      }
    }

    public void toDelta( DataOutput output ) throws IOException
    {
      output.writeInt(_delta);
    }

    
  }
