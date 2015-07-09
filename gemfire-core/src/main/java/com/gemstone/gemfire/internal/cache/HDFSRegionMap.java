package com.gemstone.gemfire.internal.cache;

/**
 * Interface implemented by RegionMap implementations that
 * read from HDFS.
 * 
 * @author sbawaska
 *
 */
public interface HDFSRegionMap {

  /**
   * @return the {@link HDFSRegionMapDelegate} that does
   * all the work
   */
  public HDFSRegionMapDelegate getDelegate();
}
