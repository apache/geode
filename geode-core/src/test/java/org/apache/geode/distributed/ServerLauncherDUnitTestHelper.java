package org.apache.geode.distributed;

public class ServerLauncherDUnitTestHelper {
  public static void main(String[] args) {

    int locatorPort = Integer.parseInt(args[0]);

    System.setProperty("gemfire.disableShutdownHook", "true");

    final ServerLauncher serverLauncher =
        new ServerLauncher.Builder().setCommand(ServerLauncher.Command.START)
            .setMemberName("server1").set("locators", "localhost[" + locatorPort + "]")
            .set("log-level", "config").set("log-file", "").setDebug(true).build();

    serverLauncher.start();

    Thread t = new Thread(() -> serverLauncher.stop());

    t.setDaemon(true);
    t.start();
  }
}
