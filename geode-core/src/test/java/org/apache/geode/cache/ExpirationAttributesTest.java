package org.apache.geode.cache;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class ExpirationAttributesTest {
  @Test
  public void constructor() throws Exception {
    ExpirationAttributes attributes = new ExpirationAttributes();
    assertThat(attributes.getTimeout()).isEqualTo(0);
    assertThat(attributes.getAction()).isEqualTo(ExpirationAction.INVALIDATE);

    attributes = new ExpirationAttributes(-10, null);
    assertThat(attributes.getTimeout()).isEqualTo(0);
    assertThat(attributes.getAction()).isEqualTo(ExpirationAction.INVALIDATE);

    attributes = new ExpirationAttributes(10);
    assertThat(attributes.getTimeout()).isEqualTo(10);
    assertThat(attributes.getAction()).isEqualTo(ExpirationAction.INVALIDATE);

    attributes = new ExpirationAttributes(10, null);
    assertThat(attributes.getTimeout()).isEqualTo(10);
    assertThat(attributes.getAction()).isEqualTo(ExpirationAction.INVALIDATE);

    attributes = new ExpirationAttributes(20, ExpirationAction.DESTROY);
    assertThat(attributes.getTimeout()).isEqualTo(20);
    assertThat(attributes.getAction()).isEqualTo(ExpirationAction.DESTROY);
  }
}
