/*
 * =========================================================================
 *  Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.vmware.gemfire.tools.pulse.internal.service;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 * Class PulseServiceFactory
 * 
 * @author azambare
 * @since version 7.5
 */
@Component
@Scope("singleton")
public class PulseServiceFactory implements ApplicationContextAware {

  static final long serialVersionUID = 02L;
  ApplicationContext applicationContext = null;

  public PulseService getPulseServiceInstance(final String servicename) {

    if (applicationContext != null
        && applicationContext.containsBean(servicename)) {
      return (PulseService) applicationContext.getBean(servicename);
    }
    return null;
  }

  @Override
  public void setApplicationContext(final ApplicationContext applicationContext)
      throws BeansException {

    this.applicationContext = applicationContext;
  }
}
