package com.gemstone.org.jgroups.util;

import java.io.StreamCorruptedException;

public interface GFLogWriter {

  boolean fineEnabled();

  boolean infoEnabled();

  boolean warningEnabled();
  
  boolean severeEnabled();

  
  void fine(String string);

  void fine(String string, Throwable e);


  void info(StringId str);
  
  void info(StringId str, Throwable e);

  void info(StringId str, Object arg);

  void info(StringId str, Object[] objects);

  void info(StringId str, Object arg, Throwable e);

  void warning(StringId str);

  void warning(StringId str, Object string, Throwable thr);

  void warning(StringId str,
      Object arg);
  
  void warning(StringId str, Throwable e);

  void warning(
      StringId str,
      Object[] objects);

  void severe(StringId str);
  
  void severe(StringId str, Throwable e);

  void severe(
      StringId str,
      Object arg);

  void severe(StringId str, Object arg, Throwable exception);

  
}
