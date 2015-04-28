package com.gemstone.gemfire.rest.internal.web.swagger.config;

import javax.servlet.ServletContext;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.mangofactory.swagger.core.SwaggerPathProvider;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;
import org.springframework.web.util.UriComponentsBuilder;

@SuppressWarnings("unused")
public class RestApiPathProvider implements SwaggerPathProvider {

  @Autowired
  private ServletContext servletContext;

  private final String docsLocation;

  private SwaggerPathProvider defaultPathProvider;

  public RestApiPathProvider(final String docsLocation) {
    Assert.isTrue(!StringUtils.isBlank(docsLocation),
        "The docs location must be specified!");

    this.docsLocation = "http://"
        + InternalDistributedSystem.getAnyInstance().getConfig()
            .getHttpServiceBindAddress()
        + ":"
        + InternalDistributedSystem.getAnyInstance().getConfig()
            .getHttpServicePort();
  }

  @Override
  public String getApiResourcePrefix() {
    return defaultPathProvider.getApiResourcePrefix();
  }

  @Override
  public String getAppBasePath() {
    return UriComponentsBuilder.fromHttpUrl(docsLocation)
        .path(servletContext.getContextPath()).build().toString();
  }

  @Override
  public String getSwaggerDocumentationBasePath() {
    return UriComponentsBuilder.fromHttpUrl(getAppBasePath())
        .pathSegment("api-docs/").build().toString();
  }

  @Override
  public String getRequestMappingEndpoint(String requestMappingPattern) {
    return defaultPathProvider.getRequestMappingEndpoint(requestMappingPattern);
  }

  public void setDefaultPathProvider(
      final SwaggerPathProvider defaultSwaggerPathProvider) {
    this.defaultPathProvider = defaultSwaggerPathProvider;
  }

}
