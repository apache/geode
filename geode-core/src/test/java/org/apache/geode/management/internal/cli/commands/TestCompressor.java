package org.apache.geode.management.internal.cli.commands;

import org.apache.geode.compression.Compressor;

public class TestCompressor implements Compressor {
  @Override
  public byte[] compress(byte[] input) {
    return new byte[0];
  }

  @Override
  public byte[] decompress(byte[] input) {
    return new byte[0];
  }
}
