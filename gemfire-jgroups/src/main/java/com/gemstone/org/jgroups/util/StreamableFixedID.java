package com.gemstone.org.jgroups.util;

/**
 * This is a parallel class to DataSerializableFixedID providing fixed ID
 * serialization for specific JGroups classes for better performance.
 * 
 * @author bschuchardt
 *
 */
public interface StreamableFixedID extends VersionedStreamable {

  public static final byte JGROUPS_VIEW = 1;
  public static final byte JGROUPS_JOIN_RESP = 2;
  public static final byte IP_ADDRESS = 70;

  /** returns the ID used to identify the class when serialized with GemFire DataSerialization */ 
  int getDSFID();
  
}
