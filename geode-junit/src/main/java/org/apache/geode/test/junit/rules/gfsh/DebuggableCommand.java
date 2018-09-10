package org.apache.geode.test.junit.rules.gfsh;

public class DebuggableCommand {
  final int debugPort;
  final String command;

  public DebuggableCommand(String command) {
    this.command = command;
    this.debugPort = -1;
  }

  public DebuggableCommand(String command, int debugPort) {
    this.command = command;
    this.debugPort = debugPort;
  }
}
