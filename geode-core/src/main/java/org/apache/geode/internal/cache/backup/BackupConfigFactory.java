package org.apache.geode.internal.cache.backup;

import static org.apache.geode.internal.cache.backup.AbstractBackupWriterConfig.TIMESTAMP;
import static org.apache.geode.internal.cache.backup.AbstractBackupWriterConfig.TYPE;
import static org.apache.geode.internal.cache.backup.FileSystemBackupWriterConfig.BASELINE_DIR;
import static org.apache.geode.internal.cache.backup.FileSystemBackupWriterConfig.TARGET_DIR;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

class BackupConfigFactory {

  private String targetDirPath;
  private String baselineDirPath;

  BackupConfigFactory() {
    // nothing
  }

  BackupConfigFactory withTargetDirPath(String targetDirPath) {
    this.targetDirPath = targetDirPath;
    return this;
  }

  BackupConfigFactory withBaselineDirPath(String baselineDirPath) {
    this.baselineDirPath = baselineDirPath;
    return this;
  }

  Properties createBackupProperties() {
    Properties properties = new Properties();
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
    properties.setProperty(TIMESTAMP, format.format(new Date()));
    properties.setProperty(TYPE, "FileSystem");
    properties.setProperty(TARGET_DIR, targetDirPath);
    if (baselineDirPath != null) {
      properties.setProperty(BASELINE_DIR, baselineDirPath);
    }
    return properties;
  }
}
