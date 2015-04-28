package com.gemstone.gemfire.internal.cache.persistence;

public enum OplogType { 
  BACKUP("BACKUP"), 
  OVERFLOW("OVERFLOW");
  
  private final String prefix;
  
  OplogType(String prefix) {
    this.prefix = prefix;
  }
  
  public String getPrefix() {
    return prefix;
  }
}