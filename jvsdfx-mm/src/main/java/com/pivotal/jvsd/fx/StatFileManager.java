package com.pivotal.jvsd.fx;

import com.pivotal.jvsd.model.stats.StatArchiveFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Basic container for all stat files we are managing.
 *
 * @author Jens Deppe
 */
public class StatFileManager {

  private List<StatArchiveFile> statFiles = new ArrayList<>();

  private static StatFileManager instance = new StatFileManager();

  private StatFileManager() {
    // We are a singleton
  }

  public static StatFileManager getInstance() {
    return instance;
  }

  public void add(String[] fileNames) throws IOException {
    for (String name : fileNames) {
      statFiles.add(new StatArchiveFile(name));
    }
  }

  public List<StatArchiveFile> getArchives() {
    return statFiles;
  }
}
