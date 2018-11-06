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
package org.apache.geode.rest.internal.web.swagger.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.Contact;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;


@PropertySource({"classpath:swagger.properties"})
@Configuration
@EnableSwagger2
@SuppressWarnings("unused")
public class SwaggerConfig {

  @Bean
  public Docket api() {
    return new Docket(DocumentationType.SWAGGER_2).select().apis(RequestHandlerSelectors.any())
        .paths(PathSelectors.any()).build().apiInfo(apiInfo());
  }

  /**
   * API Info as it appears on the Swagger-UI page
   */
  private ApiInfo apiInfo() {
    return new ApiInfoBuilder()
        .title("Apache Geode Developer REST API")
        .description(
            "Developer REST API and interface to Geode's distributed, in-memory data grid and cache.")
        .version("1.0")
        .termsOfServiceUrl("http://www.apache.org/licenses/")
        .license("Apache License, version 2.0")
        .licenseUrl("http://www.apache.org/licenses/")
        .contact(new Contact("the Apache Geode Community",
            "http://geode.apache.org",
            "dev@geode.apache.org"))
        .build();
  }
}
