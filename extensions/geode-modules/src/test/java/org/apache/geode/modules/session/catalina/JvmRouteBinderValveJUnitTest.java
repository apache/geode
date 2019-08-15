package org.apache.geode.modules.session.catalina;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import javax.servlet.ServletException;

import org.apache.catalina.Context;
import org.apache.catalina.Manager;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.junit.Test;

public class JvmRouteBinderValveJUnitTest {

  @Test
  public void InvokeWithNoPossibleFailover()
      throws IOException, ServletException {
    JvmRouteBinderValve routeBinderValue = spy(new JvmRouteBinderValve());
    JvmRouteBinderValve nextRouteBinderValue = mock(JvmRouteBinderValve.class);
    when(routeBinderValue.getNext()).thenReturn(nextRouteBinderValue);

    //Not sure how to unit test with these classes - NoClassDefFound errors on constructor call and Mockito can't mock them
    Request request = spy(new Request());
    Response response = spy(new Response());
    Context context = mock(Context.class);
    Manager manager = mock(DeltaSessionManager.class);

    doReturn(context).when(request).getContext();
    when(context.getManager()).thenReturn(manager);
    //when(((DeltaSessionManager) manager).getJvmRoute()).thenReturn(null);

    verify(nextRouteBinderValue).invoke(request, response);

  }
}
