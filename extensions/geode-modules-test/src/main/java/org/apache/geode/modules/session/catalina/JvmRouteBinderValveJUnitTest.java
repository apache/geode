package org.apache.geode.modules.session.catalina;


import java.io.IOException;

import javax.servlet.ServletException;

import org.junit.Test;

public class JvmRouteBinderValveJUnitTest {

  @Test
  public void invokeWithNoPossibleFailover()
      throws IOException, ServletException {
    // JvmRouteBinderValve routeBinderValue = spy(new JvmRouteBinderValve());
    // JvmRouteBinderValve nextRouteBinderValue = mock(JvmRouteBinderValve.class);
    // when(routeBinderValue.getNext()).thenReturn(nextRouteBinderValue);
    //
    // //Not sure how to unit test with these classes - NoClassDefFound errors on constructor call
    // and Mockito can't mock them
    // Request request = mock(Request.class);
    // Response response = mock(Response.class);
    // Context context = mock(Context.class);
    // Manager manager = mock(DeltaSessionManager.class);
    //
    // doReturn(context).when(request).getContext();
    // when(context.getManager()).thenReturn(manager);
    // //when(((DeltaSessionManager) manager).getJvmRoute()).thenReturn(null);
    //
    // verify(nextRouteBinderValue).invoke(request, response);

  }
}
