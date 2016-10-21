/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;

/**
 * Class <code>DynamicRegionAttributes</code> encapsulates the <code>RegionAttributes</code> for
 * dynamically-created regions.
 */
public class DynamicRegionAttributes implements DataSerializable {
  private static final long serialVersionUID = 1787461488256727050L;
  public String name = null;
  // public Scope scope = null;
  // public MirrorType mirror_type = null;
  public String rootRegionName = null;

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.name, out);
    DataSerializer.writeString(this.rootRegionName, out);
    // DataSerializer.writeObject( this.scope, out );
    // DataSerializer.writeObject( this.mirror_type, out );
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.name = DataSerializer.readString(in);
    this.rootRegionName = DataSerializer.readString(in);
    // this.scope = ( Scope ) DataSerializer.readObject( in );
    // this.mirror_type = ( MirrorType ) DataSerializer.readObject( in );
  }

}
