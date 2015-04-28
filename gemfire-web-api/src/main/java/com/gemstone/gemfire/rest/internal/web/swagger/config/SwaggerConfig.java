package com.gemstone.gemfire.rest.internal.web.swagger.config;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.mangofactory.swagger.configuration.JacksonScalaSupport;
import com.mangofactory.swagger.configuration.SpringSwaggerConfig;
import com.mangofactory.swagger.configuration.SpringSwaggerModelConfig;
import com.mangofactory.swagger.configuration.SwaggerGlobalSettings;
import com.mangofactory.swagger.core.SwaggerApiResourceListing;
import com.mangofactory.swagger.scanners.ApiListingReferenceScanner;
import com.wordnik.swagger.model.ApiInfo;
import com.wordnik.swagger.model.AuthorizationType;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = "com.mangofactory.swagger")
@SuppressWarnings("unused")
public class SwaggerConfig {

  protected static final List<String> DEFAULT_INCLUDE_PATTERNS = Arrays
      .asList("/.*");

  protected static final String SWAGGER_GROUP = "gemfireApi";

  @Autowired
  private SpringSwaggerConfig springSwaggerConfig;

  @Autowired
  private SpringSwaggerModelConfig springSwaggerModelConfig;

  @Value("${app.docs}")
  private String docsLocation;

  /**
   * API Info as it appears on the Swagger-UI page
   */
  private ApiInfo apiInfo() {
    return new ApiInfo(
        "Pivotal" + "\u2122" + " GemFire" + "\u00AE" + " Developer REST API",
        "Developer REST API and interface to GemFire's distributed, in-memory data grid and cache.",
        "https://network.pivotal.io/pivotal_software_eula",
        "support@pivotal.io",
        "Pivotal GemFire Documentation",
        "http://gemfire.docs.pivotal.io/index.html");
  }

  /**
   * Adds the Jackson Scala module to the MappingJackson2HttpMessageConverter
   * registered with Spring. Swagger core models are Scala so we need to be able
   * to convert to JSON. Also registers some custom serializers needed to
   * transform Swagger models to Swagger-UI required JSON format.
   */
  @Bean
  public JacksonScalaSupport jacksonScalaSupport() {
    JacksonScalaSupport jacksonScalaSupport = new JacksonScalaSupport();
    // set to false to disable
    jacksonScalaSupport.setRegisterScalaModule(true);
    return jacksonScalaSupport;
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
    SwaggerApiResourceListing swaggerApiResourceListing = new SwaggerApiResourceListing(
        springSwaggerConfig.swaggerCache(), SWAGGER_GROUP);

    // set required Swagger settings
    swaggerApiResourceListing.setSwaggerGlobalSettings(swaggerGlobalSettings());

    // use a custom path provider or
    // springSwaggerConfig.defaultSwaggerPathProvider()
    swaggerApiResourceListing.setSwaggerPathProvider(apiPathProvider());

    // supply the API Info as it should appear on Swagger-UI web page
    swaggerApiResourceListing.setApiInfo(apiInfo());

    // every SwaggerApiResourceListing needs an ApiListingReferenceScanner to
    // scan the Spring RequestMappings
    swaggerApiResourceListing
        .setApiListingReferenceScanner(apiListingReferenceScanner());

    // global authorization - see the Swagger documentation
    swaggerApiResourceListing.setAuthorizationTypes(Collections
        .<AuthorizationType> emptyList());

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
    swaggerGlobalSettings.setParameterDataTypes(springSwaggerModelConfig
        .defaultParameterDataTypes());
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
