package com.gemstone.gemfire.modules.session.catalina.internal;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.modules.session.catalina.DeltaSession;

public interface DeltaSessionAttributeEvent extends DataSerializable {

  public void apply(DeltaSession session);
}
