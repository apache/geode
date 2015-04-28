package com.gemstone.gemfire.pdx.internal;

@SuppressWarnings("rawtypes")
public interface ComparableEnum extends Comparable {
  public String getClassName();
  public String getName();
  public int getOrdinal();
}
