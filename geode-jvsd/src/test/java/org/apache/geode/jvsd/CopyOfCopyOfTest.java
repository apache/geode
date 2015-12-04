package org.apache.geode.jvsd;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

public class CopyOfCopyOfTest {
  private static int count = 10000000; //10485760; // 10 MB

  public static void main(String[] args) throws IOException {
    try (final RandomAccessFile memoryMappedFile = new RandomAccessFile("/tmp/test.vsd.stat1.vss", "rw")) {

      final MappedByteBuffer out = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, count * 16);

      long time = System.currentTimeMillis();
      Random random = new Random();
      
      // Writing into Memory Mapped File
      double value = random.nextDouble() * 100;
      for (int i = 0; i < count; i++) {
        out.putLong(time += 1000);
        out.putDouble(value += ((random.nextDouble() - 0.5) * 5000));
      }

      System.out.println("Writing to Memory Mapped File is completed");

      out.position(0);
      
      // reading from memory file in Java
      for (int i = 0; i < 10; i++) {
        System.out.println(out.getLong() + " - " + out.getDouble());
      }

      System.out.println("Reading from Memory Mapped File is completed");
    }

  }
}
