package org.xerial.snappy;

import java.io.File;

public class SnappyUtils {
  public static File findNativeLibrary() {
    return SnappyLoader.findNativeLibrary();
  }
}
