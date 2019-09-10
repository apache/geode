package org.apache.geode.modules.session.catalina;

import static org.mockito.Mockito.spy;

import org.junit.Before;

public class Tomcat7DeltaSessionManagerJUnitTest extends DeltaSessionManagerJUnitTest {

  @Before
  public void setup() {
    manager = spy(new Tomcat7DeltaSessionManager());
    initTest();
  }

}
