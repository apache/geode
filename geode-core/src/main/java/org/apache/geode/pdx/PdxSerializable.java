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
package org.apache.geode.pdx;

/**
 * When a domain class implements PdxSerializable it marks 
 * itself as a PDX. 
 * The implementation of {@link #toData toData} provides the serialization 
 * code and {@link #fromData fromData} provides the deserialization code. 
 * These methods also define each field name and field type 
 * of the PDX. Domain classes should serialize and deserialize 
 * all its member fields in the same order in toData and fromData 
 * method. Also fromData and toData must read and write the same fields
 * each time they are called.
 * A domain class that implements this interface must also have 
 * a public zero-arg constructor which is used during deserialization.
 * 
 *
 *<p>Simple example:
 * <PRE>
public class User implements PdxSerializable {
  private String name;
  private int userId;

  public User() {
  }

  public void toData(PdxWriter out) {
    out.writeString("name", this.name);
    out.writeInt("userId", this.userId);
  }

  public void fromData(PdxReader in) {
    this.name = in.readString("name");
    this.userId = in.readInt("userId");
  }
}
 * </PRE>
 * 
 * @since GemFire 6.6
 */

public interface PdxSerializable {
  
  /**
   * Serializes the PDX fields using the given writer.
   * @param writer the {@link PdxWriter} to use to write the PDX fields.
   */
  public void toData(PdxWriter writer);

  /**
   * Deserializes the PDX fields using the given reader.
   * @param reader the {@link PdxReader} to use to read the PDX fields.
   */
  public void fromData(PdxReader reader);
}
