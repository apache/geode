package org.apache.geode.jvsd;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

public class Test {
  private static final int DATA_X_OFFSET = 0;
  private static final int DATA_X_LENGTH = 8;
  private static final int DATA_Y_OFFSET = DATA_X_OFFSET + DATA_X_LENGTH;
  private static final int DATA_Y_LENGTH = 8;
  private static final int DATA_WIDTH = DATA_Y_OFFSET + DATA_Y_LENGTH;
  
  private static int count = 10485760; // 10 MB

  public static void main(String[] args) throws IOException {
    try (final RandomAccessFile memoryMappedFile = new RandomAccessFile("/tmp/test.vsd.stat1.vss", "rw")) {

      final MappedByteBuffer in = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, count * DATA_WIDTH);
      for (int j = 0; j < 10; j++) {

        int threshold = 1000;
        int begin = 0;
        int end = 0;
        while (end - begin < threshold) {
          begin = new Random().nextInt(count);
          end = new Random().nextInt(count);
        }
        in.position(0);

        long start = System.currentTimeMillis();

        downsample(in, begin, end, threshold);

        System.out.println(System.currentTimeMillis() - start);

      }
    }
  }

  public static final void downsample(final ByteBuffer data, final int begin, final int end, final int threshold) {

    final int dataLength = end - begin + 1;
    if (threshold >= dataLength || 0 == threshold) {
      // TODO dump all
      return;
    }

    int sampledIndex = 0;

    // Bucket size. Leave room for start and end data points
    final double bucketSize = (double) (dataLength - 2) / (threshold - 2);

    int a = begin; // Initially a is the first point in the triangle
    int nextA = begin;

    System.out.println(sampledIndex++ + " : " + getX(data, a) + " : " + getY(data, a));

    for (int i = 0; i < threshold - 2; i++) {

      // Calculate point average for next bucket (containing c)
      double pointCX = 0;
      double pointCY = 0;
      int pointCStart = (int) Math.floor((i + 1) * bucketSize) + begin + 1;
      int pointCEnd = (int) Math.floor((i + 2) * bucketSize) + begin + 1;
      pointCEnd = pointCEnd < dataLength ? pointCEnd : dataLength;
      final int pointCSize = pointCEnd - pointCStart;
      for (; pointCStart < pointCEnd; pointCStart++) {
        pointCX += getX(data, pointCStart);
        pointCY += getY(data, pointCStart);
      }
      pointCX /= pointCSize;
      pointCY /= pointCSize;

      // Point a
      double pointAX = getX(data, a); // TODO Date
      double pointAY = getY(data, a);

      // Get the range for bucket b
      int pointBStart = (int) Math.floor((i + 0) * bucketSize) + begin + 1;
      final int pointBEnd = (int) Math.floor((i + 1) * bucketSize) + begin + 1;
      double maxArea = -1;
      for (; pointBStart < pointBEnd; pointBStart++) {
        // Calculate triangle area over three buckets
        final double area = Math.abs((pointAX - pointCX) * (getY(data, pointBStart) - pointAY) - (pointAX - getX(data, pointBStart)) * (pointCY - pointAY)) * 0.5;
        if (area > maxArea) {
          maxArea = area;
          nextA = pointBStart; // Next a is this b
        }
      }

      a = nextA; // This a is the next a (chosen b)
      System.out.println(sampledIndex++ + " : " + getX(data, a) + " : " + getY(data, a));
    }

    System.out.println(sampledIndex++ + " : " + getX(data, end) + " : " + getY(data, end));
  }

  private static final long getY(final ByteBuffer data, final int index) {
    return data.getLong(index * DATA_WIDTH + DATA_Y_OFFSET);
  }

  private static final long getX(final ByteBuffer data, final int index) {
    return data.getLong(index * DATA_WIDTH + DATA_X_OFFSET);
  }

}
