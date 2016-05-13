/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package PdxTests;

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
