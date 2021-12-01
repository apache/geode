package org.apache.geode.gradle.testing.isolation;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Arrays;

import org.junit.Test;

public class WorkingDirectoryIsolatorTest {

  @Test
  public void updatesRelativeUnixPaths() {
    ProcessBuilder processBuilder = new ProcessBuilder();
    processBuilder.directory(new File(""));
    processBuilder.command("/bin/java", "-javaagent:../some/jacocoagent.jar=destfile=../jacoco/integrationTest.exec", "GradleWorkerMain");
    new WorkingDirectoryIsolator().accept(processBuilder);

    assertEquals(Arrays.asList("/bin/java", "-javaagent:../../some/jacocoagent.jar=destfile=../../jacoco/integrationTest.exec", "GradleWorkerMain"), processBuilder.command());
  }

  @Test
  public void updatesRelativeWindowsPaths() {
    ProcessBuilder processBuilder = new ProcessBuilder();
    processBuilder.directory(new File(""));
    processBuilder.command("/bin/java", "-javaagent:..\\some\\jacocoagent.jar=destfile=..\\jacoco\\integrationTest.exec", "GradleWorkerMain");
    new WorkingDirectoryIsolator().accept(processBuilder);

    assertEquals(Arrays.asList("/bin/java", "-javaagent:..\\..\\some\\jacocoagent.jar=destfile=..\\..\\jacoco\\integrationTest.exec", "GradleWorkerMain"), processBuilder.command());
  }

}
