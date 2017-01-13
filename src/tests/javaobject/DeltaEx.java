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
