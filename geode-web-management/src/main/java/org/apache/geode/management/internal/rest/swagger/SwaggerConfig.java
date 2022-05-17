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

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRegistration;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import org.springdoc.core.GroupedOpenApi;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
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
@ComponentScan(basePackages = {"org.springdoc"})
@SuppressWarnings("unused")
public class SwaggerConfig implements WebApplicationInitializer {

  @Override
  public void onStartup(ServletContext servletContext) throws ServletException {
    WebApplicationContext context = getContext();
    servletContext.addListener(new ContextLoaderListener(context));
    ServletRegistration.Dynamic dispatcher = servletContext.addServlet("geode",
        new DispatcherServlet(context));
    dispatcher.setLoadOnStartup(1);
    dispatcher.addMapping("/*");
  }

  private AnnotationConfigWebApplicationContext getContext() {
    AnnotationConfigWebApplicationContext context = new AnnotationConfigWebApplicationContext();
    context.scan("org.apache.geode.management.internal.rest");
    context.register(this.getClass(), org.springdoc.webmvc.ui.SwaggerConfig.class,
        org.springdoc.core.SwaggerUiConfigProperties.class,
        org.springdoc.core.SwaggerUiOAuthProperties.class,
        org.springdoc.webmvc.core.SpringDocWebMvcConfiguration.class,
        org.springdoc.webmvc.core.MultipleOpenApiSupportConfiguration.class,
        org.springdoc.core.SpringDocConfiguration.class,
        org.springdoc.core.SpringDocConfigProperties.class,
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
    extensions.put("openapi", "3.0.1");
    return new OpenAPI()
        .info(new Info().title("Apache Geode Management REST API")
            .description(
                "REST API to manage Geode. This is experimental. All request/response formats are subject to change.")
            .version("v3")
            .extensions(extensions)
            .termsOfService("http://www.apache.org/licenses/")
            .license(new License().name("Apache License, version 2.0")
                .url("http://www.apache.org/licenses/"))
            .contact(new Contact().name("the Apache Geode Community").url("http://geode.apache.org")
                .email("user@geode.apache.org")));
  }
}
