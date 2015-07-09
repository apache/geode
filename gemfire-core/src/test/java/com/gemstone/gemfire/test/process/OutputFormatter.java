package com.gemstone.gemfire.test.process;

import java.util.List;

public class OutputFormatter {

  private final List<String> outputLines;
  
  public OutputFormatter(List<String> outputLines) {
    this.outputLines = outputLines;
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (String line : this.outputLines) {
      sb.append("\n").append(line);
    }
    sb.append("\n");
    return sb.toString();
  }
}
