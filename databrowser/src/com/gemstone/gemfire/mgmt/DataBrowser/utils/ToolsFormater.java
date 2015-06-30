package com.gemstone.gemfire.mgmt.DataBrowser.utils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class ToolsFormater extends Formatter {
  private final static String           format        = " [ {2} {0} {1} ]";
  private final static String           TIME_FORMAT   = "yyyy/MM/dd HH:mm:ss.SSS z";
  private final static String           lineseparator = System.getProperty(
                                                          "line.separator",
                                                          "\n");
  private static final SimpleDateFormat timeFormatter = new SimpleDateFormat(
                                                          TIME_FORMAT);

  Date                                  dat           = new Date();
  private MessageFormat                 formatter;
  private Object                        args[]        = new Object[3];

  @Override
  public String format(LogRecord arg0) {
    StringBuffer buffer = new StringBuffer();
    dat.setTime(arg0.getMillis());
    args[0] = timeFormatter.format(dat);
    args[1] = Thread.currentThread().getName();
    args[2] = arg0.getLevel().getLocalizedName();

    StringBuffer text = new StringBuffer();
    if (formatter == null) {
      formatter = new MessageFormat(format);
    }
    formatter.format(args, text, null);
    buffer.append(text);
    buffer.append(" ");

    String message = formatMessage(arg0);
    buffer.append(message);
    buffer.append(" ");

    if (arg0.getThrown() != null) {
      try {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        // TODO: MGH - find a better way to handle this instead of printing the stack
        arg0.getThrown().printStackTrace(pw);
        pw.close();
        buffer.append(sw.toString());
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
    buffer.append(lineseparator);

    return buffer.toString();
  }

}
