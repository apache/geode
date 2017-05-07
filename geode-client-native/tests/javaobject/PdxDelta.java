/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.Delta;
import com.gemstone.gemfire.InvalidDeltaException;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.Instantiator;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;

import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxSerializable;
import com.gemstone.gemfire.pdx.PdxWriter;

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