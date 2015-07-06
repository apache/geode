package com.pivotal.jvsd.model;

import com.pivotal.jvsd.model.stats.StatArchiveFile;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
* @author Jens Deppe
*/
public class ResourceWrapper {
  private int row;

  private StatArchiveFile.ResourceInst inst;

  public ResourceWrapper(StatArchiveFile.ResourceInst inst, int idx) {
    this.inst = inst;
    this.row = idx;
  }

  public int getRow() {
    return row;
  }

  public Date getStartTime() {
    return new Date(inst.getFirstTimeMillis());
  }

  public int getSamples() {
    return inst.getSampleCount();
  }

  public String getType() {
    return inst.getType().getName();
  }

  public String getName() {
    return inst.getName();
  }

  public List<String> getStatNames() {
    List<String> statNames = new ArrayList<>();

    for (StatArchiveFile.StatValue sv : inst.getStatValues()) {
      if (!(sv.getSnapshotsAverage() == 0 && sv.getSnapshotsMaximum() == 0 && sv.
          getSnapshotsMinimum() == 0)) {
        statNames.add(sv.getDescriptor().getName());
      }
    }
    return statNames;
  }

  public StatArchiveFile.StatValue getStatValue(String name) {
    return inst.getStatValue(name);
  }
}
