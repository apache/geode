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
 */
package org.apache.geode.rest.internal.web.swagger.config;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.mangofactory.swagger.configuration.SpringSwaggerConfig;
import com.mangofactory.swagger.configuration.SwaggerGlobalSettings;
import com.mangofactory.swagger.core.SwaggerApiResourceListing;
import com.mangofactory.swagger.models.dto.ApiInfo;
import com.mangofactory.swagger.models.dto.AuthorizationType;
import com.mangofactory.swagger.plugin.SwaggerSpringMvcPlugin;
import com.mangofactory.swagger.scanners.ApiListingReferenceScanner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import org.apache.geode.internal.i18n.LocalizedStrings;

@Configuration
@ComponentScan(basePackages = "com.mangofactory.swagger")
@SuppressWarnings("unused")
public class SwaggerConfig {

  protected static final List<String> DEFAULT_INCLUDE_PATTERNS = Arrays
      .asList("/.*");

  protected static final String SWAGGER_GROUP = "apacheGeode";

  @Autowired
  private SpringSwaggerConfig springSwaggerConfig;

  @Value("${app.docs}")
  private String docsLocation;

  @SuppressWarnings("SpringJavaAutowiringInspection")
  @Autowired
  public void setSpringSwaggerConfig(SpringSwaggerConfig springSwaggerConfig) {
    this.springSwaggerConfig = springSwaggerConfig;
  }

  @Bean
  public SwaggerSpringMvcPlugin customImplementation() {

    return new SwaggerSpringMvcPlugin(this.springSwaggerConfig).apiInfo(apiInfo());
  }

  /**
   * API Info as it appears on the Swagger-UI page
   */
  private ApiInfo apiInfo() {
    return new ApiInfo(
        LocalizedStrings.SwaggerConfig_VENDOR_PRODUCT_LINE.toLocalizedString(),
        LocalizedStrings.SwaggerConfig_DESCRIPTOR.toLocalizedString(),
        LocalizedStrings.SwaggerConfig_EULA_LINK.toLocalizedString(),
        LocalizedStrings.SwaggerConfig_SUPPORT_LINK.toLocalizedString(),
        LocalizedStrings.SwaggerConfig_DOC_TITLE.toLocalizedString(),
        LocalizedStrings.SwaggerConfig_DOC_LINK.toLocalizedString());
  }

  /**
   * Configure a SwaggerApiResourceListing for each Swagger instance within your
   * app. e.g. 1. private 2. external APIs 3. ..., required to be a Spring bean
   * as Spring will call the postConstruct method to bootstrap Swagger scanning.
   */
  @Bean
  public SwaggerApiResourceListing swaggerApiResourceListing() {
    // The group name is important and should match the group set on
    // ApiListingReferenceScanner
    // Note that swaggerCache() is by DefaultSwaggerController to serve the
    // Swagger JSON
    SwaggerApiResourceListing swaggerApiResourceListing = new SwaggerApiResourceListing(springSwaggerConfig.swaggerCache(), SWAGGER_GROUP);

    // set required Swagger settings
    swaggerApiResourceListing.setSwaggerGlobalSettings(swaggerGlobalSettings());

    // use a custom path provider or
    // springSwaggerConfig.defaultSwaggerPathProvider()
    swaggerApiResourceListing.setSwaggerPathProvider(apiPathProvider());

    // supply the API Info as it should appear on Swagger-UI web page
    swaggerApiResourceListing.setApiInfo(apiInfo());

    // every SwaggerApiResourceListing needs an ApiListingReferenceScanner to
    // scan the Spring RequestMappings
    swaggerApiResourceListing.setApiListingReferenceScanner(apiListingReferenceScanner());

    // global authorization - see the Swagger documentation
    swaggerApiResourceListing.setAuthorizationTypes(Collections.<AuthorizationType>emptyList());

    return swaggerApiResourceListing;
  }

  /**
   * Global Swagger configuration settings
   */
  @Bean
  public SwaggerGlobalSettings swaggerGlobalSettings() {
    SwaggerGlobalSettings swaggerGlobalSettings = new SwaggerGlobalSettings();
    swaggerGlobalSettings.setGlobalResponseMessages(springSwaggerConfig
        .defaultResponseMessages());
    swaggerGlobalSettings.setIgnorableParameterTypes(springSwaggerConfig
        .defaultIgnorableParameterTypes());
    return swaggerGlobalSettings;
  }

  /**
   * The ApiListingReferenceScanner does most of the work. It scans the
   * appropriate Spring RequestMappingHandlerMappings, applies the correct
   * absolute paths to the generated Swagger resources, and so on.
   */
  @Bean
  public ApiListingReferenceScanner apiListingReferenceScanner() {
    ApiListingReferenceScanner apiListingReferenceScanner = new ApiListingReferenceScanner();

    // Picks up all of the registered Spring RequestMappingHandlerMappings
    // during scanning...
    apiListingReferenceScanner
        .setRequestMappingHandlerMapping(springSwaggerConfig
            .swaggerRequestMappingHandlerMappings());

    // Excludes any Controllers with the supplied Annotations...
    apiListingReferenceScanner.setExcludeAnnotations(springSwaggerConfig
        .defaultExcludeAnnotations());

    // Only include paths that match the supplied Regular Expressions...
    apiListingReferenceScanner.setIncludePatterns(DEFAULT_INCLUDE_PATTERNS);

    //
    apiListingReferenceScanner.setResourceGroupingStrategy(springSwaggerConfig
        .defaultResourceGroupingStrategy());

    // PathProvider used to generate the appropriate uri's
    apiListingReferenceScanner.setSwaggerPathProvider(apiPathProvider());

    // Must match the Swagger group set on the SwaggerApiResourceListing...
    apiListingReferenceScanner.setSwaggerGroup(SWAGGER_GROUP);

    return apiListingReferenceScanner;
  }

  /**
   * Example of a custom path provider
   */
  @Bean
  public RestApiPathProvider apiPathProvider() {
    RestApiPathProvider apiPathProvider = new RestApiPathProvider(docsLocation);
    apiPathProvider.setDefaultPathProvider(springSwaggerConfig
        .defaultSwaggerPathProvider());
    return apiPathProvider;
  }

}
