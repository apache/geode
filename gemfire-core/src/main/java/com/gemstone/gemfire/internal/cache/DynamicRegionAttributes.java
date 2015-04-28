/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

/**
 * Class <code>DynamicRegionAttributes</code> encapsulates the
 * <code>RegionAttributes</code> for dynamically-created regions.
 */
public class DynamicRegionAttributes implements DataSerializable {
  private static final long serialVersionUID = 1787461488256727050L;
  public String name = null;
//   public Scope scope = null;
//   public MirrorType mirror_type = null;
  public String rootRegionName = null;

  public void toData( DataOutput out ) throws IOException {
    DataSerializer.writeString( this.name, out );
    DataSerializer.writeString( this.rootRegionName, out );
//     DataSerializer.writeObject( this.scope, out );
//     DataSerializer.writeObject( this.mirror_type, out );
  }

  public void fromData( DataInput in ) throws IOException, ClassNotFoundException {
    this.name = DataSerializer.readString( in );
    this.rootRegionName = DataSerializer.readString( in );
//     this.scope = ( Scope ) DataSerializer.readObject( in );
//     this.mirror_type = ( MirrorType ) DataSerializer.readObject( in );
  }

}
