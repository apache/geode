/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.compression;

/**
 * Interface for a compression codec.
 * 
 * @author rholmes
 */
public interface Compressor {

  /**
   * Compresses the input byte array.
   * 
   * @param input The data to be compressed.
   * 
   * @return A compressed version of the input parameter.
   * 
   * @throws CompressionException
   */
  public byte[] compress(byte[] input);
  
  /**
   * Decompresses a compressed byte array.
   * 
   * @param input A compressed byte array.
   * 
   * @return an uncompressed version of compressed input byte array data.
   * 
   * @throws CompressionException
   */
  public byte[] decompress(byte[] input);
}