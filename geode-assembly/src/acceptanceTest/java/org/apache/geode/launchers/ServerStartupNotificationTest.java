package org.apache.geode.launchers;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.test.junit.rules.gfsh.GfshRule;

public class ServerStartupNotificationTest {

  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  private File serverFolder;
  private String serverName;

  /**
   * Test ideas
   * - redundancy recovery
   * - persistent value recovery
   * - ServerLauncher.start() finished
   * - Log statement that indicates server is online
   * - Meter that reflects server online state
   */

  @Before
  public void setup() {
    serverFolder = temporaryFolder.getRoot();
    serverName = testName.getMethodName();
  }

  @After
  public void stopServer() {
    String stopServerCommand = "stop server --dir=" + serverFolder.getAbsolutePath();
    gfshRule.execute(stopServerCommand);
  }

  @Test
  public void startupWithNoAsyncTasks() {
    String startServerCommand = String.join(" ",
        "start server",
        "--name=" + serverName,
        "--dir=" + serverFolder.getAbsolutePath(),
        "--disable-default-server");

    gfshRule.execute(startServerCommand);

    Pattern logLinePattern = Pattern.compile("^\\[info .*].*Server is online.*");
    Path logFile = serverFolder.toPath().resolve(serverName + ".log");
    await()
        .untilAsserted(() ->
            assertThat(Files.lines(logFile))
                .as("Log file " + logFile + " includes line matching " + logLinePattern)
                .anyMatch(logLinePattern.asPredicate())
        );
  }
}
