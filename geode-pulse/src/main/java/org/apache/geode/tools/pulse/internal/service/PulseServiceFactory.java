/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.geode.tools.pulse.internal.service;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 * Class PulseServiceFactory
 * 
 * @since GemFire version 7.5
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
