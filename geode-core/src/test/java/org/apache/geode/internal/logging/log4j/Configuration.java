package org.apache.geode.internal.logging.log4j;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.commons.io.IOUtils;

public class Configuration {

  private URL resource;
  private String configFileName;

  public Configuration(final URL resource, final String configFileName) {
    this.resource = resource;
    this.configFileName = configFileName;
  }

  public File createConfigFileIn(final File targetFolder) throws IOException, URISyntaxException {
    File targetFile = new File(targetFolder, this.configFileName);
    IOUtils.copy(this.resource.openStream(), new FileOutputStream(targetFile));
    assertThat(targetFile).hasSameContentAs(new File(this.resource.toURI()));
    return targetFile;
  }
}
