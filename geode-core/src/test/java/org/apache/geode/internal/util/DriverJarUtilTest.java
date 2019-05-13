package org.apache.geode.internal.util;

import static org.assertj.core.api.Assertions.assertThat;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.internal.DeployedJar;

public class DriverJarUtilTest {

  private DriverJarUtil util;

  @Before
  public void setup() {
    util = spy(new DriverJarUtil());
  }

  @Test
  public void getJDBCDriverNameWithStringIsSuccessful() throws IOException {
    DeployedJar driverJar = mock(DeployedJar.class);
    doReturn(driverJar).when(util).createDeployedJar(any());

    File jarFile = mock(File.class);
    doReturn(jarFile).when(driverJar).getFile();

    FileInputStream fileInputStream = mock(FileInputStream.class);
    BufferedInputStream bufferedInputStream = mock(BufferedInputStream.class);
    ZipInputStream zipInputStream = mock(ZipInputStream.class);

    doReturn(fileInputStream).when(util).createFileInputStream(any());
    doReturn(bufferedInputStream).when(util).createBufferedInputStream(any());
    doReturn(zipInputStream).when(util).createZipInputStream(any());

    ZipEntry zipEntry = mock(ZipEntry.class);

    long stringLength = 1;

    when(zipInputStream.getNextEntry()).thenReturn(zipEntry);
    when(zipEntry.getName()).thenReturn("META-INF/services/java.sql.Driver");
    when(zipEntry.getSize()).thenReturn(stringLength);
    when(zipInputStream.read(any(byte[].class), any(int.class), any(int.class))).thenReturn(1);

    assertThat(util.getJdbcDriverName("jarName").length()).isEqualTo(stringLength);
  }

  @Test
  public void getJDBCDriverNameThrowsIOExceptionWhenZipEntrySizeIsInvalid() throws IOException {
    DeployedJar driverJar = mock(DeployedJar.class);
    doReturn(driverJar).when(util).createDeployedJar(any());

    File jarFile = mock(File.class);
    doReturn(jarFile).when(driverJar).getFile();

    FileInputStream fileInputStream = mock(FileInputStream.class);
    BufferedInputStream bufferedInputStream = mock(BufferedInputStream.class);
    ZipInputStream zipInputStream = mock(ZipInputStream.class);

    doReturn(fileInputStream).when(util).createFileInputStream(any());
    doReturn(bufferedInputStream).when(util).createBufferedInputStream(any());
    doReturn(zipInputStream).when(util).createZipInputStream(any());

    ZipEntry zipEntry = mock(ZipEntry.class);

    when(zipInputStream.getNextEntry()).thenReturn(zipEntry);
    when(zipEntry.getName()).thenReturn("META-INF/services/java.sql.Driver");
    when(zipEntry.getSize()).thenReturn(-1L);

    try {
      util.getJdbcDriverName("jarName");
      Assert.fail("Expected IOException not thrown.");
    } catch (IOException ex) {
      assertThat(ex.getMessage()).isEqualTo("Invalid zip entry found for META-INF/services/java.sql.Driver " +
          "within jar. Ensure that the jar containing the driver has been deployed and that " +
          "the driver is at least JDBC 4.0");
    }
  }

  @Test
  public void getJDBCDriverNameThrowsIOExceptionWhenZipInputStreamIsEmpty() throws IOException {
    DeployedJar driverJar = mock(DeployedJar.class);
    doReturn(driverJar).when(util).createDeployedJar(any());

    File jarFile = mock(File.class);
    doReturn(jarFile).when(driverJar).getFile();

    String jarName = "jarName";
    doReturn(jarName).when(driverJar).getJarName();

    FileInputStream fileInputStream = mock(FileInputStream.class);
    BufferedInputStream bufferedInputStream = mock(BufferedInputStream.class);
    ZipInputStream zipInputStream = mock(ZipInputStream.class);

    doReturn(fileInputStream).when(util).createFileInputStream(any());
    doReturn(bufferedInputStream).when(util).createBufferedInputStream(any());
    doReturn(zipInputStream).when(util).createZipInputStream(any());

    when(zipInputStream.getNextEntry()).thenReturn(null);

    try {
      util.getJdbcDriverName(jarName);
      Assert.fail("Expected IOException not thrown.");
    } catch (IOException ex) {
      assertThat(ex.getMessage()).isEqualTo("Could not find JDBC Driver class name in jar file '"
          + driverJar.getJarName() + "'");
    }

  }

  @Test
  public void getJDBCDriverNameThrowsIOExceptionWhenNoSuitableZipEntryIsFound() throws IOException {
    DeployedJar driverJar = mock(DeployedJar.class);
    doReturn(driverJar).when(util).createDeployedJar(any());

    File jarFile = mock(File.class);
    doReturn(jarFile).when(driverJar).getFile();

    String jarName = "jarName";
    doReturn(jarName).when(driverJar).getJarName();

    FileInputStream fileInputStream = mock(FileInputStream.class);
    BufferedInputStream bufferedInputStream = mock(BufferedInputStream.class);
    ZipInputStream zipInputStream = mock(ZipInputStream.class);

    doReturn(fileInputStream).when(util).createFileInputStream(any());
    doReturn(bufferedInputStream).when(util).createBufferedInputStream(any());
    doReturn(zipInputStream).when(util).createZipInputStream(any());

    ZipEntry zipEntry = mock(ZipEntry.class);

    when(zipInputStream.getNextEntry()).thenReturn(zipEntry).thenReturn(null);
    when(zipEntry.getName()).thenReturn("bad-driver-name");

    try {
      util.getJdbcDriverName(jarName);
      Assert.fail("Expected IOException not thrown.");
    } catch (IOException ex) {
      assertThat(ex.getMessage()).isEqualTo("Could not find JDBC Driver class name in jar file '"
          + driverJar.getJarName() + "'");
    }

  }
}
