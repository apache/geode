package org.apache.geode.modules.session.catalina;

import static org.mockito.Mockito.mock;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.client.ClientCache;

public abstract class AbstractSessionCacheJUnitTest {

  protected SessionManager sessionManager = mock(SessionManager.class);

  protected AbstractSessionCache sessionCache;

  @Before
  public void setup() {

  }
}
