/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.web.controllers;

import java.io.IOException;
import java.util.Set;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.lang.ObjectUtils;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.internal.util.IOUtils;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.web.domain.Link;
import com.gemstone.gemfire.management.internal.web.domain.LinkIndex;
import com.gemstone.gemfire.management.internal.web.domain.QueryParameterSource;
import com.gemstone.gemfire.management.internal.web.http.HttpMethod;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * The ShellCommandsController class implements GemFire REST API calls for Gfsh Shell Commands.
 * 
 * @author John Blum
 * @see com.gemstone.gemfire.management.internal.cli.commands.ShellCommands
 * @see com.gemstone.gemfire.management.internal.web.controllers.AbstractCommandsController
 * @see org.springframework.stereotype.Controller
 * @see org.springframework.web.bind.annotation.RequestMapping
 * @see org.springframework.web.bind.annotation.RequestMethod
 * @see org.springframework.web.bind.annotation.RequestParam
 * @see org.springframework.web.bind.annotation.ResponseBody
 * @since 8.0
 */
@Controller("shellController")
@RequestMapping(AbstractCommandsController.REST_API_VERSION)
@SuppressWarnings("unused")
public class ShellCommandsController extends AbstractCommandsController {

  protected static final String MBEAN_ATTRIBUTE_LINK_RELATION = "mbean-attribute";
  protected static final String MBEAN_OPERATION_LINK_RELATION = "mbean-operation";
  protected static final String MBEAN_QUERY_LINK_RELATION = "mbean-query";
  protected static final String PING_LINK_RELATION = "ping";

  @RequestMapping(method = RequestMethod.POST, value = "/management/commands", params = "cmd")
  @ResponseBody
  public String command(@RequestParam("cmd") final String command) {
    return processCommand(decode(command));
  }

  // TODO research the use of Jolokia instead
  @RequestMapping(method = RequestMethod.GET, value = "/mbean/attribute")
  public ResponseEntity<?> getAttribute(@RequestParam("resourceName") final String resourceName,
                                        @RequestParam("attributeName") final String attributeName)
  {
    try {
      final Object attributeValue = getMBeanServer().getAttribute(ObjectName.getInstance(decode(resourceName)),
        decode(attributeName));

      return new ResponseEntity<byte[]>(IOUtils.serializeObject(attributeValue), HttpStatus.OK);
    }
    catch (AttributeNotFoundException e) {
      return new ResponseEntity<String>(printStackTrace(e), HttpStatus.BAD_REQUEST);
    }
    catch (InstanceNotFoundException e) {
      return new ResponseEntity<String>(printStackTrace(e), HttpStatus.NOT_FOUND);
    }
    catch (MalformedObjectNameException e) {
      return new ResponseEntity<String>(printStackTrace(e), HttpStatus.BAD_REQUEST);
    }
    catch (Exception e) {
      return new ResponseEntity<String>(printStackTrace(e), HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  // TODO research the use of Jolokia instead
  @RequestMapping(method = RequestMethod.POST, value = "/mbean/operation")
  public ResponseEntity<?> invoke(@RequestParam("resourceName") final String resourceName,
                                  @RequestParam("operationName") final String operationName,
                                  @RequestParam(value = "signature", required = false) String[] signature,
                                  @RequestParam(value = "parameters", required = false) Object[] parameters)
  {
    signature = (signature != null ? signature : StringUtils.EMPTY_STRING_ARRAY);
    parameters = (parameters != null ? parameters : ObjectUtils.EMPTY_OBJECT_ARRAY);

    try {
      final Object result = getMBeanServer().invoke(ObjectName.getInstance(decode(resourceName)), decode(operationName),
        parameters, signature);

      return new ResponseEntity<byte[]>(IOUtils.serializeObject(result), HttpStatus.OK);
    }
    catch (InstanceNotFoundException e) {
      return new ResponseEntity<String>(printStackTrace(e), HttpStatus.NOT_FOUND);
    }
    catch (MalformedObjectNameException e) {
      return new ResponseEntity<String>(printStackTrace(e), HttpStatus.BAD_REQUEST);
    }
    catch (Exception e) {
      return new ResponseEntity<String>(printStackTrace(e), HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @RequestMapping(method = RequestMethod.POST, value = "/mbean/query")
  public ResponseEntity<?> queryNames(@RequestBody final QueryParameterSource query) {
    try {
      final Set<ObjectName> objectNames = getMBeanServer().queryNames(query.getObjectName(), query.getQueryExpression());

      return new ResponseEntity<byte[]>(IOUtils.serializeObject(objectNames), HttpStatus.OK);
    }
    catch (IOException e) {
      return new ResponseEntity<String>(printStackTrace(e), HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Gets a link index for the web service endpoints and REST API calls in GemFire for management and monitoring
   * using GemFire shell (Gfsh).
   * 
   * @return a LinkIndex containing Links for all web service endpoints, REST API calls in GemFire.
   * @see com.gemstone.gemfire.management.internal.cli.i18n.CliStrings
   * @see com.gemstone.gemfire.management.internal.web.controllers.AbstractCommandsController#toUri(String)
   * @see com.gemstone.gemfire.management.internal.web.domain.Link
   * @see com.gemstone.gemfire.management.internal.web.domain.LinkIndex
   * @see com.gemstone.gemfire.management.internal.web.http.HttpMethod
   */
  // TODO figure out a better way to maintain this link index, such as using an automated way to introspect
  // the Spring Web MVC Controller RequestMapping Annotations.
  @RequestMapping(method = RequestMethod.GET, value = "/index", produces = MediaType.APPLICATION_XML_VALUE)
  @ResponseBody
  public LinkIndex index() {
    //logger.warning(String.format("Returning Link Index for Context Path (%1$s).",
    //  ServletUriComponentsBuilder.fromCurrentContextPath().build().toString()));
    return new LinkIndex()
      // Cluster Commands
      .add(new Link(CliStrings.STATUS_SYSTEM, toUri("/cluster")))
      // Member Commands
      .add(new Link(CliStrings.LIST_MEMBER, toUri("/members")))
      .add(new Link(CliStrings.DESCRIBE_MEMBER, toUri("/members/{name}")))
      // Region Commands
      .add(new Link(CliStrings.LIST_REGION, toUri("/regions")))
      .add(new Link(CliStrings.DESCRIBE_REGION, toUri("/regions/{name}")))
      .add(new Link(CliStrings.ALTER_REGION, toUri("/regions/{name}"), HttpMethod.PUT))
      .add(new Link(CliStrings.CREATE_REGION, toUri("/regions"), HttpMethod.POST))
      .add(new Link(CliStrings.DESTROY_REGION, toUri("/regions/{name}"), HttpMethod.DELETE))
      // Index Commands
      .add(new Link(CliStrings.LIST_INDEX, toUri("/indexes")))
      .add(new Link(CliStrings.CLEAR_DEFINED_INDEXES, toUri("/indexes?op=clear-defined"), HttpMethod.DELETE))
      .add(new Link(CliStrings.CREATE_INDEX, toUri("/indexes"), HttpMethod.POST))
      .add(new Link(CliStrings.CREATE_DEFINED_INDEXES, toUri("/indexes?op=create-defined"), HttpMethod.POST))
      .add(new Link(CliStrings.DEFINE_INDEX, toUri("/indexes?op=define"), HttpMethod.POST))
      .add(new Link(CliStrings.DESTROY_INDEX, toUri("/indexes"), HttpMethod.DELETE))
      .add(new Link(CliStrings.DESTROY_INDEX, toUri("/indexes/{name}"), HttpMethod.DELETE))
        // Data Commands
      .add(new Link(CliStrings.GET, toUri("/regions/{region}/data"), HttpMethod.GET))
      .add(new Link(CliStrings.PUT, toUri("/regions/{region}/data"), HttpMethod.PUT))
      .add(new Link(CliStrings.REMOVE, toUri("/regions/{region}/data"), HttpMethod.DELETE))
      .add(new Link(CliStrings.EXPORT_DATA, toUri("/members/{member}/regions/{region}/data"), HttpMethod.GET))
      .add(new Link(CliStrings.IMPORT_DATA, toUri("/members/{member}/regions/{region}/data"), HttpMethod.POST))
      .add(new Link(CliStrings.LOCATE_ENTRY, toUri("/regions/{region}/data/location"), HttpMethod.GET))
      .add(new Link(CliStrings.QUERY, toUri("/regions/data/query"), HttpMethod.GET))
      .add(new Link(CliStrings.REBALANCE, toUri("/regions/data?op=rebalance"), HttpMethod.POST))
      // Function Commands
      .add(new Link(CliStrings.LIST_FUNCTION, toUri("/functions")))
      .add(new Link(CliStrings.DESTROY_FUNCTION, toUri("/functions/{id}"), HttpMethod.DELETE))
      .add(new Link(CliStrings.EXECUTE_FUNCTION, toUri("/functions/{id}"), HttpMethod.POST))
      // Client Commands
      .add(new Link(CliStrings.LIST_CLIENTS, toUri("/clients")))
      .add(new Link(CliStrings.DESCRIBE_CLIENT, toUri("/clients/{clientID}")))
      // Config Commands
      .add(new Link(CliStrings.ALTER_RUNTIME_CONFIG, toUri("/config"), HttpMethod.POST))
      .add(new Link(CliStrings.DESCRIBE_CONFIG, toUri("/members/{member}/config")))
      .add(new Link(CliStrings.EXPORT_CONFIG, toUri("/config")))
      .add(new Link(CliStrings.EXPORT_SHARED_CONFIG, toUri("/config/cluster")))
      .add(new Link(CliStrings.IMPORT_SHARED_CONFIG, toUri("/config/cluster"), HttpMethod.POST))
      .add(new Link(CliStrings.STATUS_SHARED_CONFIG, toUri("/services/cluster-config")))
      // Deploy Commands
      .add(new Link(CliStrings.LIST_DEPLOYED, toUri("/deployed")))
      .add(new Link(CliStrings.DEPLOY, toUri("/deployed"), HttpMethod.POST))
      .add(new Link(CliStrings.UNDEPLOY, toUri("/deployed"), HttpMethod.DELETE))
      // Disk Store Commands
      .add(new Link(CliStrings.LIST_DISK_STORE, toUri("/diskstores")))
      .add(new Link(CliStrings.BACKUP_DISK_STORE, toUri("/diskstores?op=backup"), HttpMethod.POST))
      .add(new Link(CliStrings.COMPACT_DISK_STORE, toUri("/diskstores/{name}?op=compact"), HttpMethod.POST))
      .add(new Link(CliStrings.CREATE_DISK_STORE, toUri("/diskstores"), HttpMethod.POST))
      .add(new Link(CliStrings.DESCRIBE_DISK_STORE, toUri("/diskstores/{name}")))
      .add(new Link(CliStrings.DESTROY_DISK_STORE, toUri("/diskstores/{name}"), HttpMethod.DELETE))
      .add(new Link(CliStrings.REVOKE_MISSING_DISK_STORE, toUri("/diskstores/{id}?op=revoke"), HttpMethod.POST))
      .add(new Link(CliStrings.SHOW_MISSING_DISK_STORE, toUri("/diskstores/missing")))
      // Durable Client Commands
      .add(new Link(CliStrings.LIST_DURABLE_CQS, toUri("/durable-clients/{durable-client-id}/cqs")))
      .add(new Link(CliStrings.COUNT_DURABLE_CQ_EVENTS, toUri("/durable-clients/{durable-client-id}/cqs/events")))
      .add(new Link(CliStrings.COUNT_DURABLE_CQ_EVENTS, toUri("/durable-clients/{durable-client-id}/cqs/{durable-cq-name}/events")))
      .add(new Link(CliStrings.CLOSE_DURABLE_CLIENTS, toUri("/durable-clients/{durable-client-id}?op=close"), HttpMethod.POST))
      .add(new Link(CliStrings.CLOSE_DURABLE_CQS, toUri("/durable-clients/{durable-client-id}/cqs/{durable-cq-name}?op=close"), HttpMethod.POST))
      // Launcher Lifecycle Commands
      .add(new Link(CliStrings.STATUS_LOCATOR, toUri("/members/{name}/locator")))
      .add(new Link(CliStrings.STATUS_SERVER, toUri("/members/{name}/server")))
      // Miscellaneous Commands
      .add(new Link(CliStrings.CHANGE_LOGLEVEL, toUri("/groups/{groups}/loglevel"), HttpMethod.POST))
      .add(new Link(CliStrings.CHANGE_LOGLEVEL, toUri("/members/{members}/loglevel"), HttpMethod.POST))
      .add(new Link(CliStrings.CHANGE_LOGLEVEL, toUri("/members/{members}/groups/{groups}/loglevel"), HttpMethod.POST))
      .add(new Link(CliStrings.EXPORT_LOGS, toUri("/logs")))
      .add(new Link(CliStrings.EXPORT_STACKTRACE, toUri("/stacktraces")))
      .add(new Link(CliStrings.GC, toUri("/gc"), HttpMethod.POST))
      .add(new Link(CliStrings.GC, toUri("/members/{member}/gc"), HttpMethod.POST))
      .add(new Link(CliStrings.NETSTAT, toUri("/netstat")))
      .add(new Link(CliStrings.SHOW_DEADLOCK, toUri("/deadlocks")))
      .add(new Link(CliStrings.SHOW_LOG, toUri("/members/{member}/log")))
      .add(new Link(CliStrings.SHOW_METRICS, toUri("/metrics")))
      .add(new Link(CliStrings.SHUTDOWN, toUri("/shutdown"), HttpMethod.POST)) // verb!
      // Queue Commands
      .add(new Link(CliStrings.CREATE_ASYNC_EVENT_QUEUE, toUri("/async-event-queues"), HttpMethod.POST))
      .add(new Link(CliStrings.LIST_ASYNC_EVENT_QUEUES, toUri("/async-event-queues")))
      // PDX Commands
      .add(new Link(CliStrings.CONFIGURE_PDX, toUri("/pdx"), HttpMethod.POST))
      //.add(new Link(CliStrings.PDX_DELETE_FIELD, toUri("/pdx/type/field"), HttpMethod.DELETE))
      .add(new Link(CliStrings.PDX_RENAME, toUri("/pdx/type"), HttpMethod.POST))
      // Shell Commands
      .add(new Link(MBEAN_ATTRIBUTE_LINK_RELATION, toUri("/mbean/attribute")))
      .add(new Link(MBEAN_OPERATION_LINK_RELATION, toUri("/mbean/operation"), HttpMethod.POST))
      .add(new Link(MBEAN_QUERY_LINK_RELATION, toUri("/mbean/query"), HttpMethod.POST))
      .add(new Link(PING_LINK_RELATION, toUri("/ping"), HttpMethod.GET))
      .add(new Link(CliStrings.VERSION, toUri("/version")))
      // WAN Gateway Commands
      .add(new Link(CliStrings.LIST_GATEWAY, toUri("/gateways")))
      .add(new Link(CliStrings.CREATE_GATEWAYRECEIVER, toUri("/gateways/receivers"), HttpMethod.POST))
      .add(new Link(CliStrings.CREATE_GATEWAYSENDER, toUri("/gateways/senders"), HttpMethod.POST))
      .add(new Link(CliStrings.LOAD_BALANCE_GATEWAYSENDER, toUri("/gateways/senders/{id}?op=load-balance"), HttpMethod.POST))
      .add(new Link(CliStrings.PAUSE_GATEWAYSENDER, toUri("/gateways/senders/{id}?op=pause"), HttpMethod.POST))
      .add(new Link(CliStrings.RESUME_GATEWAYSENDER, toUri("/gateways/senders/{id}?op=resume"), HttpMethod.POST))
      .add(new Link(CliStrings.START_GATEWAYRECEIVER, toUri("/gateways/receivers?op=start"), HttpMethod.POST))
      .add(new Link(CliStrings.START_GATEWAYSENDER, toUri("/gateways/senders?op=start"), HttpMethod.POST))
      .add(new Link(CliStrings.STATUS_GATEWAYRECEIVER, toUri("/gateways/receivers")))
      .add(new Link(CliStrings.STATUS_GATEWAYSENDER, toUri("/gateways/senders/{id}")))
      .add(new Link(CliStrings.STOP_GATEWAYRECEIVER, toUri("/gateways/receivers?op=stop"), HttpMethod.POST))
      .add(new Link(CliStrings.STOP_GATEWAYSENDER, toUri("/gateways/senders/{id}?op=stop"), HttpMethod.POST));
  }

  @RequestMapping(method = { RequestMethod.GET, RequestMethod.HEAD }, value = "/ping")
  public ResponseEntity<String> ping() {
    return new ResponseEntity<String>("<html><body><h1>Mischief Managed!</h1></body></html>", HttpStatus.OK);
  }

  @RequestMapping(method = RequestMethod.GET, value = "/version")
  @ResponseBody
  public String version() {
    return GemFireVersion.getProductName().concat("/").concat(GemFireVersion.getGemFireVersion());
  }

  @RequestMapping(method = RequestMethod.GET, value = "/version/full")
  @ResponseBody
  public String versionSimple() {
    return GemFireVersion.asString();
  }

}
