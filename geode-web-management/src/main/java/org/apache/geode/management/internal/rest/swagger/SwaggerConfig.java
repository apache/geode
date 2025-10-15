/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.management.internal.rest.swagger;

import java.util.HashMap;
import java.util.Map;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRegistration;
import org.springdoc.core.models.GroupedOpenApi;
import org.springdoc.webmvc.ui.SwaggerUiHome;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.PropertySource;
import org.springframework.web.WebApplicationInitializer;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import org.apache.geode.management.internal.rest.security.GeodeAuthenticationProvider;


@PropertySource({"classpath:swagger-management.properties"})
@EnableWebMvc
@Configuration("swaggerConfigManagement")
@ComponentScan(basePackages = {"org.springdoc"},
    excludeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE,
        classes = SwaggerUiHome.class))
@SuppressWarnings("unused")
public class SwaggerConfig implements WebApplicationInitializer {

  /**
   * Initializes the Swagger web application context on startup.
   *
   * <p>
   * Jakarta Servlet spec returns null when servlet already exists. The "geode" servlet is
   * defined in web.xml. Jakarta Servlet 6.0 (and Jetty 12) returns null from addServlet() to
   * indicate servlet name conflict, preventing NullPointerException during DispatcherServlet
   * initialization. Previous javax.servlet implementations had inconsistent behavior.
   * See Jakarta Servlet spec 4.4.
   */
  @Override
  public void onStartup(ServletContext servletContext) throws ServletException {
    WebApplicationContext context = getContext();
    servletContext.addListener(new ContextLoaderListener(context));

    ServletRegistration.Dynamic dispatcher = servletContext.addServlet("geode",
        new DispatcherServlet(context));

    // Only configure if this is a new servlet registration (dispatcher != null)
    if (dispatcher != null) {
      dispatcher.setLoadOnStartup(1);
      dispatcher.addMapping("/*");
    }
  }

  private AnnotationConfigWebApplicationContext getContext() {
    AnnotationConfigWebApplicationContext context = new AnnotationConfigWebApplicationContext();
    context.scan("org.apache.geode.management.internal.rest");
    context.register(this.getClass(), org.springdoc.webmvc.ui.SwaggerConfig.class,
        org.springdoc.core.properties.SwaggerUiConfigProperties.class,
        org.springdoc.core.properties.SwaggerUiOAuthProperties.class,
        org.springdoc.core.configuration.SpringDocConfiguration.class,
        org.springdoc.core.properties.SpringDocConfigProperties.class,
        org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration.class);

    return context;
  }

  @Bean
  public GroupedOpenApi api() {
    return GroupedOpenApi.builder()
        .group("management-api")
        .pathsToMatch("/**")
        .build();
  }

  @Autowired
  private GeodeAuthenticationProvider authProvider;

  /**
   * API Info as it appears on the Swagger-UI page
   */
  @Bean
  public OpenAPI apiInfo() {
    Map<String, Object> extensions = new HashMap<>();
    extensions.put("authTokenEnabled",
        Boolean.toString(authProvider.isAuthTokenEnabled()));
    return new OpenAPI()
        .info(new Info().title("Apache Geode Management REST API")
            .description(
                "REST API to manage Geode. This is experimental. All request/response formats are subject to change.")
            .version("v1")
            .extensions(extensions)
            .termsOfService("http://www.apache.org/licenses/")
            .license(new License().name("Apache License, version 2.0")
                .url("http://www.apache.org/licenses/"))
            .contact(new Contact().name("the Apache Geode Community").url("http://geode.apache.org")
                .email("user@geode.apache.org")));
  }
}
