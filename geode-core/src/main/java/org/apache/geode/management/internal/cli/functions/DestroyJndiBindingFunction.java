package org.apache.geode.management.internal.cli.functions;

import javax.naming.NamingException;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.management.internal.cli.i18n.CliStrings;

public class DestroyJndiBindingFunction implements InternalFunction {

  static final String RESULT_MESSAGE = "Jndi binding \"{0}\" destroyed on \"{1}\"";
  static final String EXCEPTION_RESULT_MESSAGE = "Jndi binding \"{0}\" not found on \"{1}\"";

  @Override
  public void execute(FunctionContext context) {
    ResultSender<Object> resultSender = context.getResultSender();
    String jndiName = (String) context.getArguments();

    try {
      JNDIInvoker.unMapDatasource(jndiName);
      resultSender.lastResult(new CliFunctionResult(context.getMemberName(), true,
          CliStrings.format(RESULT_MESSAGE, jndiName, context.getMemberName())));
    } catch (NamingException e) {
      resultSender.lastResult(new CliFunctionResult(context.getMemberName(), true,
          CliStrings.format(EXCEPTION_RESULT_MESSAGE, jndiName, context.getMemberName())));
    }
  }
}
