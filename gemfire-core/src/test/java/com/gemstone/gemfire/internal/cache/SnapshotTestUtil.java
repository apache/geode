package com.gemstone.gemfire.internal.cache;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static org.junit.Assert.*;
import com.examples.snapshot.MyObject;
import com.gemstone.gemfire.cache.snapshot.SnapshotIterator;
import com.gemstone.gemfire.cache.snapshot.SnapshotReader;

public class SnapshotTestUtil {

  public static <K,V> void checkSnapshotEntries(File dir, Map<K, V> expected, String diskStoreName, String regionName) 
      throws Exception {
    final Map<K, V> testData = new HashMap<K, V>(expected);
    String snapshot = "snapshot-" + diskStoreName + "-" + regionName + ".gfd";
    SnapshotIterator<Integer, MyObject> iter = SnapshotReader.read(new File(dir, snapshot));
    try {
      while (iter.hasNext()) {
        Entry<Integer, MyObject> entry = iter.next();
        Object expectedVal = testData.remove(entry.getKey());
        assertEquals(expectedVal, entry.getValue());
      }
      assertTrue(testData.isEmpty());
    } finally {
      iter.close();
    }
  }
}
