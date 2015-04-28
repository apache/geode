package com.gemstone.gemfire.internal.process;

/**
 * Enumeration of GemFire {@link ControllableProcess} types and the file names
 * associated with controlling its lifecycle.
 * 
 * @author Kirk Lund
 * @since 8.0
 */
public enum ProcessType {
  LOCATOR ("LOCATOR", "vf.gf.locator"),
  SERVER ("SERVER", "vf.gf.server");

  public static final String TEST_PREFIX_PROPERTY = "gemfire.test.ProcessType.TEST_PREFIX";

  private static final String SUFFIX_PID = "pid";
  private static final String SUFFIX_STOP_REQUEST = "stop.cmd";
  private static final String SUFFIX_STATUS_REQUEST = "status.cmd";
  private static final String SUFFIX_STATUS = "status";
  
  private final String name;
  private final String fileName;
  
  private ProcessType(final String name, final String fileName) {
    this.name = name;
    this.fileName = fileName;
  }
  
  public String getPidFileName() {
    return new StringBuilder(System.getProperty(TEST_PREFIX_PROPERTY, ""))
        .append(this.fileName).append(".").append(SUFFIX_PID).toString();
  }
  
  public String getStopRequestFileName() {
    return new StringBuilder(System.getProperty(TEST_PREFIX_PROPERTY, ""))
        .append(this.fileName).append(".").append(SUFFIX_STOP_REQUEST).toString();
  }
  
  public String getStatusRequestFileName() {
    return new StringBuilder(System.getProperty(TEST_PREFIX_PROPERTY, ""))
        .append(this.fileName).append(".").append(SUFFIX_STATUS_REQUEST).toString();
  }
  
  public String getStatusFileName() {
    return new StringBuilder(System.getProperty(TEST_PREFIX_PROPERTY, ""))
        .append(this.fileName).append(".").append(SUFFIX_STATUS).toString();
  }
  
  @Override
  public String toString() {
    return this.name;
  }
}
