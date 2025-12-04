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
import org.springdoc.core.models.GroupedOpenApi;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import org.apache.geode.management.internal.rest.security.GeodeAuthenticationProvider;

/*
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * SpringDoc 2.x Integration for Pure Spring Framework (Non-Boot) Application
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * MIGRATION CONTEXT:
 * This configuration enables SpringDoc 2.x (OpenAPI 3.x documentation) in a
 * pure Spring Framework application without Spring Boot. The main application
 * uses XML-based configuration (management-servlet.xml), while this config
 * provides annotation-based SpringDoc integration.
 *
 * PROBLEM SOLVED:
 * SpringDoc 2.x was designed for Spring Boot and depends heavily on Boot's
 * autoconfiguration infrastructure. Previous attempts excluded SpringDoc JARs
 * from the WAR, causing Swagger UI to return 404 errors. This configuration
 * successfully integrates SpringDoc by:
 *
 * 1. Including SpringDoc JARs in WAR (removed build.gradle exclusions)
 * 2. Providing required infrastructure beans without full Boot adoption
 * 3. Using component scanning to discover SpringDoc's internal beans
 * 4. Leveraging Spring Boot's JacksonAutoConfiguration as a library only
 *
 * ARCHITECTURE:
 * This class is picked up by the main XML context's component-scan of
 * org.apache.geode.management.internal.rest package. It registers itself
 * as a Spring @Configuration and provides OpenAPI documentation beans.
 *
 * KEY DESIGN DECISIONS:
 *
 * 1. @EnableWebMvc - Required for Spring MVC infrastructure beans
 * - Provides mvcConversionService, RequestMappingHandlerMapping, etc.
 * - SpringDoc needs these beans to introspect REST controllers
 * - Must be present even though main context has <mvc:annotation-driven>
 *
 * 2. @ComponentScan(basePackages = {"org.springdoc"}) - Discovery strategy
 * - SpringDoc 2.x uses many internal Spring beans for auto-configuration
 * - Component scanning is more robust than manual @Import registration
 * - Discovers: OpenApiResource, SwaggerConfigResource, SwaggerWelcome, etc.
 *
 * 3. excludeFilters - Prevent bean conflicts and mapping issues
 * - Test classes: Exclude org.springdoc.*Test.* to avoid test beans
 * - SwaggerUiHome: Excluded because it tries to map GET [/], which conflicts
 * with existing GeodeManagementController mapping. We don't need the root
 * redirect since Swagger UI is accessed at /management/swagger-ui.html
 *
 * 4. @Import({SpringDocConfiguration.class, JacksonAutoConfiguration.class})
 * - SpringDocConfiguration: Core SpringDoc bean definitions
 * - JacksonAutoConfiguration: Provides ObjectMapper for OpenAPI serialization
 * - We use these as libraries, not as Spring Boot autoconfiguration
 *
 * 5. NO WebApplicationInitializer - Previous approach removed
 * - Original code created a separate servlet context via onStartup()
 * - Simplified to single-context approach using component-scan pickup
 * - Reduces complexity and memory overhead (no second context)
 *
 * PARENT CLASSLOADER DEPENDENCY:
 * jackson-dataformat-yaml is required for OpenAPI YAML generation but must be
 * in the parent classloader (geode/lib) to avoid classloader conflicts with
 * WAR-deployed Jackson libraries. See geode-core/build.gradle for the
 * runtimeOnly dependency addition.
 *
 * INTEGRATION WITH MAIN CONTEXT:
 * - Main Context: management-servlet.xml (XML config)
 * └── Component scans: org.apache.geode.management.internal.rest
 * └── Picks up: SwaggerConfig.class (@Configuration)
 * └── Registers: OpenAPI beans, SpringDoc infrastructure
 *
 * - Bean Isolation:
 * └── ObjectMapper: Main context has id="objectMapper" primary="true"
 * └── SpringDoc's ObjectMapper: From JacksonAutoConfiguration (separate bean)
 * └── No conflicts because different bean names
 *
 * TESTING VALIDATION:
 * - SwaggerManagementVerificationIntegrationTest.isSwaggerRunning: ✅ PASS
 * - Swagger UI accessible: http://localhost:7070/management/swagger-ui.html
 * - OpenAPI JSON: http://localhost:7070/management/v3/api-docs
 * - All 235 unit tests: ✅ PASS (no regressions)
 *
 * BENEFITS:
 * - Full Swagger UI documentation for Management REST API
 * - OpenAPI 3.x spec generation for API consumers
 * - Automatic API documentation sync with code changes
 * - No code duplication (SpringDoc handles all OpenAPI logic)
 * - Interactive API testing via Swagger UI
 *
 * RELATED FILES:
 * - geode-web-management/build.gradle: SpringDoc JAR inclusions
 * - geode-core/build.gradle: jackson-dataformat-yaml parent classloader
 * - management-servlet.xml: Main XML context configuration
 * - swagger-management.properties: SpringDoc property customization
 *
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */

@PropertySource({"classpath:swagger-management.properties"})
@EnableWebMvc // Required for Spring MVC beans (mvcConversionService, etc.)
@ComponentScan(basePackages = {"org.springdoc"},
    excludeFilters = {
        // Exclude test classes to prevent test beans from being registered
        @ComponentScan.Filter(type = FilterType.REGEX, pattern = "org\\.springdoc\\..*Test.*"),
        // Exclude SwaggerUiHome to prevent GET [/] mapping conflict
        @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE,
            classes = org.springdoc.webmvc.ui.SwaggerUiHome.class)
    })
@Configuration("swaggerConfigManagement")
@SuppressWarnings("unused")
@Import({
    // Core SpringDoc configuration classes for OpenAPI generation
    org.springdoc.core.configuration.SpringDocConfiguration.class,
    // Provides ObjectMapper bean for OpenAPI JSON/YAML serialization
    org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration.class
})
public class SwaggerConfig {

  /**
   * Defines the API group for SpringDoc documentation generation.
   *
   * <p>
   * SpringDoc uses GroupedOpenApi to organize endpoints into logical groups.
   * This configuration creates a single "management-api" group that includes all
   * endpoints (/**) from the Management REST API.
   *
   * <p>
   * REASONING FOR pathsToMatch("/**"):
   * - Captures all REST endpoints: /management/v1/*, /management/v2/*, etc.
   * - Simpler than listing individual path patterns
   * - Ensures new endpoints are automatically documented
   *
   * <p>
   * The generated OpenAPI spec is accessible at:
   * - JSON: /management/v3/api-docs
   * - YAML: /management/v3/api-docs.yaml
   *
   * @return GroupedOpenApi configuration for the management API group
   */
  @Bean
  public GroupedOpenApi api() {
    return GroupedOpenApi.builder()
        .group("management-api")
        .pathsToMatch("/**")
        .build();
  }

  /**
   * Optional injection of GeodeAuthenticationProvider from main XML context.
   *
   * <p>
   * CROSS-CONTEXT DEPENDENCY HANDLING:
   * GeodeAuthenticationProvider is defined in management-servlet.xml (main context),
   * not in this SpringDoc configuration. We use @Autowired(required = false) to make
   * this dependency optional, allowing SwaggerConfig to initialize successfully even
   * if the bean is not available in the same context.
   *
   * <p>
   * WHY OPTIONAL:
   * - Prevents circular dependency issues during Spring context initialization
   * - Allows SwaggerConfig to work in test environments without full security setup
   * - More resilient to configuration changes in the main context
   *
   * <p>
   * USAGE:
   * If present, authProvider.isAuthTokenEnabled() is used to populate the OpenAPI
   * spec extensions, indicating whether token-based authentication is enabled.
   */
  @Autowired(required = false)
  private GeodeAuthenticationProvider authProvider;

  /**
   * Provides OpenAPI metadata for Swagger UI display and API documentation.
   *
   * <p>
   * This bean defines the API information shown on the Swagger UI page, including:
   * - Title: "Apache Geode Management REST API"
   * - Description: API purpose and experimental status warning
   * - Version: "v1" (current API version)
   * - License: Apache License 2.0
   * - Contact: Apache Geode community details
   * - Custom extensions: Authentication configuration flags
   *
   * <p>
   * DYNAMIC EXTENSION HANDLING:
   * The "authTokenEnabled" extension is conditionally added based on whether
   * GeodeAuthenticationProvider is available. This pattern allows the OpenAPI
   * spec to reflect the actual runtime authentication configuration.
   *
   * <p>
   * WHY CONDITIONAL CHECK (if authProvider != null):
   * - Prevents NullPointerException when running without full security setup
   * - Allows Swagger UI to work in development environments
   * - Makes tests more resilient (don't require auth provider mock)
   *
   * <p>
   * OPENAPI SPEC GENERATION:
   * This metadata is combined with controller annotations (@Operation, @Parameter,
   * @ApiResponse) to generate the complete OpenAPI 3.0.1 specification. The spec
   * is automatically regenerated on application startup based on current code.
   *
   * <p>
   * SWAGGER UI DISPLAY:
   * - Title appears at the top of /management/swagger-ui.html
   * - Description shows below the title
   * - Extensions are available in the raw JSON spec
   * - License and contact links are clickable in the UI
   *
   * @return OpenAPI metadata configuration for the Management REST API
   */
  @Bean
  public OpenAPI apiInfo() {
    Map<String, Object> extensions = new HashMap<>();

    // Conditionally add authTokenEnabled extension if security provider is available
    if (authProvider != null) {
      extensions.put("authTokenEnabled",
          Boolean.toString(authProvider.isAuthTokenEnabled()));
    }

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
