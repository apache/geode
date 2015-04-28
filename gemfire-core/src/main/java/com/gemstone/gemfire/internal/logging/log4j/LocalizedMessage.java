package com.gemstone.gemfire.internal.logging.log4j;

import org.apache.logging.log4j.message.Message;

import com.gemstone.org.jgroups.util.StringId;

/**
 * An implementation of a Log4j {@link Message} that wraps a {@link StringId}.
 * 
 * @author David Hoots
 */
public final class LocalizedMessage implements Message {
  private static final long serialVersionUID = -8893339995741536401L;

  private final StringId stringId;
  private final Object[] params;
  private final Throwable throwable;
  
  private LocalizedMessage(final StringId stringId, final Object[] params, final Throwable throwable) {
    this.stringId = stringId;
    this.params = params;
    this.throwable = throwable;
  }

  public static LocalizedMessage create(final StringId stringId) {
    return new LocalizedMessage(stringId, null, null);
  }
  
  public static final LocalizedMessage create(final StringId stringId, final Object[] params) {
    return new LocalizedMessage(stringId, params, null);
  }

  public static final LocalizedMessage create(final StringId stringId, final Throwable throwable) {
    return new LocalizedMessage(stringId, null, throwable);
  }
  
  public static final LocalizedMessage create(final StringId stringId, final Object object) {
    return new LocalizedMessage(stringId, new Object[] { object }, null);
  }
  
  public static final LocalizedMessage create(final StringId stringId, final Object[] params, final Throwable throwable) {
    return new LocalizedMessage(stringId, params, throwable);
  }
  
  @Override
  public String getFormattedMessage() {
    return this.stringId.toLocalizedString(params);
  }

  @Override
  public String getFormat() {
    return this.stringId.getRawText();
  }

  @Override
  public Object[] getParameters() {
    return this.params;
  }

  @Override
  public Throwable getThrowable() {
    return this.throwable;
  }
}
