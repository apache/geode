/*
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

package org.apache.geode.security.templates;

import com.gemstone.gemfire.rest.internal.web.security.template.SampleRESTSecurityConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.support.AbstractAnnotationConfigDispatcherServletInitializer;

public class SampleSpringWebMvcInitializer extends AbstractAnnotationConfigDispatcherServletInitializer {

  @Override
  protected Class<?>[] getRootConfigClasses() {
    return new Class[] { SampleRESTSecurityConfiguration.class };
  }

  /**
   * Specify {@link Configuration @Configuration}
   * and/or {@link Component @Component} classes to be
   * provided to the {@linkplain #createServletApplicationContext() dispatcher servlet
   * application context}.
   * @return the configuration classes for the dispatcher servlet application context or
   * {@code null} if all configuration is specified through root config classes.
   */
  @Override
  protected Class<?>[] getServletConfigClasses() {
    return new Class<?>[0];
  }

  /**
   * Specify the servlet mapping(s) for the {@code DispatcherServlet} &mdash;
   * for example {@code "/"}, {@code "/app"}, etc.
   * @see #registerDispatcherServlet(ServletContext)
   */
  @Override
  protected String[] getServletMappings() {
    return new String[0];
  }
}

