package org.apache.geode.management.internal.cli.functions;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.management.internal.cli.functions.RegionFunctionArgs.ExpirationAttrs;
import org.junit.Test;

public class ExpirationAttrsTest {

  private static ExpirationAttrs.ExpirationFor expirationFor =
      ExpirationAttrs.ExpirationFor.ENTRY_IDLE;

  @Test
  public void constructor() throws Exception {
    ExpirationAttrs attrs = new ExpirationAttrs(expirationFor, null, null);
    assertThat(attrs.getTime()).isEqualTo(0);
    assertThat(attrs.getAction()).isEqualTo(ExpirationAction.INVALIDATE);

    attrs = new ExpirationAttrs(expirationFor, -1, null);
    assertThat(attrs.getTime()).isEqualTo(0);
    assertThat(attrs.getAction()).isEqualTo(ExpirationAction.INVALIDATE);

    attrs = new ExpirationAttrs(expirationFor, 0, null);
    assertThat(attrs.getTime()).isEqualTo(0);
    assertThat(attrs.getAction()).isEqualTo(ExpirationAction.INVALIDATE);

    attrs = new ExpirationAttrs(expirationFor, 2, "destroy");
    assertThat(attrs.getTime()).isEqualTo(2);
    assertThat(attrs.getAction()).isEqualTo(ExpirationAction.DESTROY);
  }
}
