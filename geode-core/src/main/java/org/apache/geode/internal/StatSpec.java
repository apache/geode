package org.apache.geode.internal;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.geode.internal.statistics.StatArchiveReader;

public class StatSpec implements StatArchiveReader.StatSpec {
  public final String cmdLineSpec;
  public final String typeId;
  public final String instanceId;
  public final String statId;
  private final Pattern tp;
  private final Pattern sp;
  private final Pattern ip;
  private final int combineType;

  public StatSpec(String cmdLineSpec) {
    this.cmdLineSpec = cmdLineSpec;
    if (cmdLineSpec.charAt(0) == '+') {
      cmdLineSpec = cmdLineSpec.substring(1);
      if (cmdLineSpec.charAt(0) == '+') {
        cmdLineSpec = cmdLineSpec.substring(1);
        this.combineType = GLOBAL;
      } else {
        this.combineType = FILE;
      }
    } else {
      this.combineType = NONE;
    }
    int dotIdx = cmdLineSpec.lastIndexOf('.');
    String typeId = null;
    String instanceId = null;
    String statId = null;
    if (dotIdx != -1) {
      statId = cmdLineSpec.substring(dotIdx + 1);
      cmdLineSpec = cmdLineSpec.substring(0, dotIdx);
    }
    int commaIdx = cmdLineSpec.indexOf(':');
    if (commaIdx != -1) {
      instanceId = cmdLineSpec.substring(0, commaIdx);
      typeId = cmdLineSpec.substring(commaIdx + 1);
    } else {
      instanceId = cmdLineSpec;
    }

    if (statId == null || statId.length() == 0) {
      this.statId = "";
      this.sp = null;
    } else {
      this.statId = statId;
      this.sp = Pattern.compile(statId, Pattern.CASE_INSENSITIVE);
    }
    if (typeId == null || typeId.length() == 0) {
      this.typeId = "";
      this.tp = null;
    } else {
      this.typeId = typeId;
      this.tp = Pattern.compile(".*" + typeId, Pattern.CASE_INSENSITIVE);
    }
    if (instanceId == null || instanceId.length() == 0) {
      this.instanceId = "";
      this.ip = null;
    } else {
      this.instanceId = instanceId;
      this.ip = Pattern.compile(instanceId, Pattern.CASE_INSENSITIVE);
    }
  }

  @Override
  public String toString() {
    return "StatSpec instanceId=" + this.instanceId + " typeId=" + this.typeId + " statId="
        + this.statId;
  }

  @Override
  public int getCombineType() {
    return this.combineType;
  }

  @Override
  public boolean archiveMatches(File archive) {
    return true;
  }

  @Override
  public boolean statMatches(String statName) {
    if (this.sp == null) {
      return true;
    } else {
      Matcher m = this.sp.matcher(statName);
      return m.matches();
    }
  }

  @Override
  public boolean typeMatches(String typeName) {
    if (this.tp == null) {
      return true;
    } else {
      Matcher m = this.tp.matcher(typeName);
      return m.matches();
    }
  }

  @Override
  public boolean instanceMatches(String textId, long numericId) {
    if (this.ip == null) {
      return true;
    } else {
      Matcher m = this.ip.matcher(textId);
      if (m.matches()) {
        return true;
      }
      m = this.ip.matcher(String.valueOf(numericId));
      return m.matches();
    }
  }
}
