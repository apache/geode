/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import java.io.File;
import java.io.IOException;
import java.util.EnumMap;

import com.gemstone.gemfire.internal.cache.persistence.soplog.Compactor.MetadataCompactor;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReader.Metadata;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReader.SerializedComparator;

/**
 * Provides a means to construct a soplog.
 */
public interface SortedOplogFactory {
  /**
   * Configures a <code>SortedOplog</code>.
   * 
   * @author bakera
   */
  public class SortedOplogConfiguration {
    /** the default metadata compactor */
    public static MetadataCompactor DEFAULT_METADATA_COMPACTOR = new MetadataCompactor() {
      @Override
      public byte[] compact(byte[] metadata1, byte[] metadata2) {
        return metadata1;
      }
    };
    
    /**
     * Defines the available checksum algorithms.
     */
    public enum Checksum {
      NONE,
      CRC32
    }
    
    /**
     * Defines the available compression algorithms.
     */
    public enum Compression { 
      NONE, 
    }
    
    /**
     * Defines the available key encodings.
     */
    public enum KeyEncoding { 
      NONE, 
    }

    /** the soplog name */
    private final String name;
    
    /** the statistics */
    private final SortedOplogStatistics stats;
    
    /** true if bloom filters are enabled */
    private boolean bloom;
    
    /** the soplog block size */
    private int blockSize;
    
    /** the number of bytes for each checksum */
    private int bytesPerChecksum;
    
    /** the checksum type */
    private Checksum checksum;
    
    /** the compression type */
    private Compression compression;
    
    /** the key encoding type */
    private KeyEncoding keyEncoding;
    
    /** the comparator */
    private SerializedComparator comparator;

    /** metadata comparers */
    private EnumMap<Metadata, MetadataCompactor> metaCompactors;
    
    public SortedOplogConfiguration(String name) {
      this(name, new SortedOplogStatistics("GridDBRegionStatistics", name));
    }
    
    public SortedOplogConfiguration(String name, SortedOplogStatistics stats) {
      this.name = name;
      this.stats = stats;
      
      // defaults
      bloom = true;
      blockSize = 1 << 16;
      bytesPerChecksum = 1 << 14;
      checksum = Checksum.NONE;
      compression = Compression.NONE;
      keyEncoding = KeyEncoding.NONE;
      comparator = new ByteComparator();
    }
    
    public SortedOplogConfiguration setBloomFilterEnabled(boolean enabled) {
      this.bloom = enabled;
      return this;
    }
    
    public SortedOplogConfiguration setBlockSize(int size) {
      this.blockSize = size;
      return this;
    }
    
    public SortedOplogConfiguration setBytesPerChecksum(int bytes) {
      this.bytesPerChecksum = bytes;
      return this;
    }
    
    public SortedOplogConfiguration setChecksum(Checksum type) {
      this.checksum = type;
      return this;
    }
    
    public SortedOplogConfiguration setCompression(Compression type) {
      this.compression = type;
      return this;
    }
    
    public SortedOplogConfiguration setKeyEncoding(KeyEncoding type) {
      this.keyEncoding = type;
      return this;
    }
    
    public SortedOplogConfiguration setComparator(SerializedComparator comp) {
      this.comparator = comp;
      return this;
    }
    
    public SortedOplogConfiguration addMetadataCompactor(Metadata name, MetadataCompactor compactor) {
      metaCompactors.put(name, compactor);
      return this;
    }
    
    /**
     * Returns the soplog name.
     * @return the name
     */
    public String getName() {
      return name;
    }

    /**
     * Returns the statistics.
     * @return the statistics
     */
    public SortedOplogStatistics getStatistics() {
      return stats;
    }
    
    /**
     * Returns true if the bloom filter is enabled.
     * @return true if enabled
     */
    public boolean isBloomFilterEnabled() {
      return bloom;
    }

    /**
     * Returns the block size in bytes.
     * @return the block size
     */
    public int getBlockSize() {
      return blockSize;
    }

    /**
     * Returns the number of bytes per checksum.
     * @return the bytes
     */
    public int getBytesPerChecksum() {
      return bytesPerChecksum;
    }

    /**
     * Returns the checksum type.
     * @return the checksum
     */
    public Checksum getChecksum() {
      return checksum;
    }

    /**
     * Returns the compression type.
     * @return the compression
     */
    public Compression getCompression() {
      return compression;
    }

    /**
     * Returns the key encoding type.
     * @return the key encoding
     */
    public KeyEncoding getKeyEncoding() {
      return keyEncoding;
    }

    /**
     * Returns the comparator.
     * @return the comparator
     */
    public SerializedComparator getComparator() {
      return comparator;
    }
    
    /**
     * Returns the metadata compactor for the given name. 
     * @param name the metadata name
     * @return the compactor
     */
    public MetadataCompactor getMetadataCompactor(Metadata name) {
      MetadataCompactor mc = metaCompactors.get(name);
      if (mc != null) {
        return mc;
      }
      return DEFAULT_METADATA_COMPACTOR;
    }
  }
  
  /**
   * Returns the configuration.
   * @return the configuration
   */
  SortedOplogConfiguration getConfiguration();
  
  /**
   * Creates a new soplog.
   * 
   * @param name the filename
   * @return the soplog
   * @throws IOException error creating soplog
   */
  SortedOplog createSortedOplog(File name) throws IOException;
}
