package org.apache.geode.client.sni;

import java.util.function.Supplier;

import com.palantir.docker.compose.DockerComposeRule;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Assume;
import org.junit.rules.ExternalResource;

public class NotOnWindowsDockerRule extends ExternalResource {

  private final Supplier<DockerComposeRule> dockerRuleSupplier;
  private DockerComposeRule docker;

  public NotOnWindowsDockerRule(Supplier<DockerComposeRule> dockerRuleSupplier) {
    this.dockerRuleSupplier = dockerRuleSupplier;
  }

  @Override
  protected void before() throws Throwable {
    Assume.assumeFalse(SystemUtils.IS_OS_WINDOWS);
    this.docker = dockerRuleSupplier.get();
    docker.before();
  }

  @Override
  protected void after() {
    if (docker != null) {
      docker.after();
    }
  }

  public DockerComposeRule get() {
    return docker;
  }
}
