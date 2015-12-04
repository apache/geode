package org.apache.geode.jvsd;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class Copy_2_of_Test {
  private static int count = 10485760; // 10 MB

  public static void main(String[] args) throws IOException {
    try (final RandomAccessFile memoryMappedFile = new RandomAccessFile("/tmp/test.vsd.stat1.vss", "rw")) {

      final MappedByteBuffer out = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, count * 16);
      testCursor(out);
      testIndex(out);
    }
  }

  protected static void testCursor(final MappedByteBuffer out) {
    System.out.println("cursor");
    for (int j = 0; j < 100; j++) {

      out.position(0);
      long start = System.currentTimeMillis();
      long time = 0;
      double value = 0;
      // reading from memory file in Java
      for (int i = 0; i < count; i++) {
        time = out.getLong();
        value = out.getDouble();
      }

      System.out.println(System.currentTimeMillis() - start);
      System.out.println(time + "-" + value);

    }
  }

  protected static void testIndex(final MappedByteBuffer out) {
    System.out.println("index");
    for (int j = 0; j < 100; j++) {

      out.position(0);
      long start = System.currentTimeMillis();
      long time = 0;
      double value = 0;
      // reading from memory file in Java
      for (int i = 0; i < count; i++) {
        time = out.getLong(i * 16);
        value = out.getDouble(i * 16 + 8);
      }

      System.out.println(System.currentTimeMillis() - start);
      System.out.println(time + "-" + value);

    }
  }
}
