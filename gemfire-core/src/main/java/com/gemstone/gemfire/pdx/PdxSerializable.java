/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.pdx;

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
 * @author darrel
 * @since 6.6
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
