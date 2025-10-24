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

package org.apache.geode.gradle.plugins

import org.gradle.api.Project

class DependencyConstraints {
/** By necessity, the version of those plugins used in the build-scripts are defined in the
 * buildscript {} configuration in the root project's build.gradle. */
  static Map<String,String> disparateDependencies = initExternalDependencies()

  static String get(String name) {
    return disparateDependencies.get(name)
  }

  static private Map<String, String> initExternalDependencies() {
    Map<String,String> deps = new HashMap<>()
    // These versions are consumed beyond the scope of source set dependencies.

    // These version numbers are consumed by :geode-modules-assembly:distAppServer filtering
    // Some of these are referenced below as well
    deps.put("antlr.version", "2.7.7")
    deps.put("commons-io.version", "2.18.0")
    deps.put("commons-lang3.version", "3.12.0")
    deps.put("commons-validator.version", "1.7")
    deps.put("fastutil.version", "8.5.8")
    deps.put("jakarta.activation.version", "2.1.3")
    deps.put("jakarta.transaction.version", "2.0.1")
    deps.put("jakarta.xml.bind.version", "4.0.2")
    deps.put("jakarta.servlet.version", "6.0.0")
    deps.put("jakarta.resource.version", "2.1.0")
    deps.put("jakarta.mail.version", "2.1.2")
    deps.put("jakarta.annotation.version", "2.1.1")
    deps.put("jakarta.ejb.version", "4.0.1")
    deps.put("jgroups.version", "3.6.20.Final")
    deps.put("log4j.version", "2.17.2")
    deps.put("log4j-slf4j2-impl.version", "2.23.1")
    deps.put("micrometer.version", "1.14.0")
    deps.put("shiro.version", "1.13.0")
    deps.put("slf4j-api.version", "2.0.17")
    deps.put("jakarta.transaction-api.version", "2.0.1")
    deps.put("jboss-modules.version", "1.11.0.Final")
    deps.put("jackson.version", "2.17.0")
    deps.put("jackson.databind.version", "2.17.0")
    // Spring Framework 6.x Migration
    deps.put("springshell.version", "3.3.3")
    deps.put("springframework.version", "6.1.14")
    deps.put("springboot.version", "3.3.5")
    deps.put("springsecurity.version", "6.3.4")
    deps.put("springhateoas.version", "2.3.3")
    deps.put("springldap.version", "3.2.7")
    deps.put("springdoc.version", "2.6.0")

    // These version numbers are used in testing various versions of tomcat and are consumed explicitly
    // in will be called explicitly in the relevant extensions module, and respective configurations
    // in geode-assembly.gradle.  Moreover, dependencyManagement does not seem to play nicely when
    // specifying @zip in a dependency, the manner in which we consume them in custom configurations.
    // This would possibly be corrected if they were proper source sets.
    // Note: Tomcat 6/7/8/9 versions kept for upgradeTest (downloads old Geode releases)
    deps.put("tomcat6.version", "6.0.37")
    deps.put("tomcat7.version", "7.0.109")
    deps.put("tomcat8.version", "8.5.66")
    deps.put("tomcat9.version", "9.0.62")
    // Jakarta EE - Tomcat 10.1+ and 11.x support
    deps.put("tomcat10.version", "10.1.33")
    deps.put("tomcat11.version", "11.0.11")

    // The jetty version is also hard-coded in geode-assembly:test
    // at o.a.g.sessions.tests.GenericAppServerInstall.java
    // Jetty 12.0.x for Jakarta EE 10 (Servlet 6.0) compatibility
    // Jetty 12 reorganized modules under ee10, ee9, ee8 packages
    deps.put("jetty.version", "12.0.27")

    // These versions are referenced in test.gradle, which is aggressively injected into all projects.
    deps.put("junit.version", "4.13.2")
    deps.put("junit-jupiter.version", "5.8.2")
    deps.put("cglib.version", "3.3.0")

    // This old version is for geode-assembly:acceptanceTest for gradle-in-gradle tests. As noted there, do not let
    // this version be the same as the geode build itself.
    deps.put("gradle-tooling-api.version", "5.1.1")
    return deps
  }

  void apply(Project project) {
    def dependencySet = { Map<String, String> group_and_version, Closure closure ->
      DependencySetHandler delegate =
          new DependencySetHandler(group_and_version.get("group"), group_and_version.get("version"), project)
      closure.setDelegate(delegate)
      closure.call(delegate)
    }

    project.dependencies {
      constraints {
        // informal, inter-group dependencySet
        api(group: 'antlr', name: 'antlr', version: get('antlr.version'))
        api(group: 'cglib', name: 'cglib', version: get('cglib.version'))
        // GEODE-10466: Requires native library support for both x86_64 and ARM Mac
        api(group: 'com.arakelian', name: 'java-jq', version: '2.0.0')
        api(group: 'com.carrotsearch.randomizedtesting', name: 'randomizedtesting-runner', version: '2.7.9')
        api(group: 'com.github.davidmoten', name: 'geo', version: '0.8.0')
        api(group: 'com.github.stefanbirkner', name: 'system-rules', version: '1.19.0')
        api(group: 'com.github.stephenc.findbugs', name: 'findbugs-annotations', version: '1.3.9-1')
        api(group: 'com.google.code.findbugs', name: 'jsr305', version: '3.0.2')
        api(group: 'com.google.guava', name: 'guava', version: '31.1-jre')
        api(group: 'com.healthmarketscience.rmiio', name: 'rmiio', version: '2.1.2')
        api(group: 'com.mockrunner', name: 'mockrunner-servlet', version: '2.0.6')
        api(group: 'com.nimbusds', name:'nimbus-jose-jwt', version:'8.11')
        // Pinning transitive dependency from spring-security-oauth2 to clean up our licenses.
        api(group: 'com.nimbusds', name: 'oauth2-oidc-sdk', version: '8.9')
        api(group: 'jakarta.activation', name: 'jakarta.activation-api', version: get('jakarta.activation.version'))
        api(group: 'com.sun.istack', name: 'istack-commons-runtime', version: '4.0.1')
        api(group: 'jakarta.mail', name: 'jakarta.mail-api', version: get('jakarta.mail.version'))
        api(group: 'jakarta.xml.bind', name: 'jakarta.xml.bind-api', version: get('jakarta.xml.bind.version'))
        api(group: 'org.glassfish.jaxb', name: 'jaxb-runtime', version: '4.0.2')
        api(group: 'org.glassfish.jaxb', name: 'jaxb-core', version: '4.0.2')
        api(group: 'com.tngtech.archunit', name:'archunit-junit4', version: '0.15.0')
        api(group: 'com.zaxxer', name: 'HikariCP', version: '4.0.3')
        api(group: 'commons-beanutils', name: 'commons-beanutils', version: '1.11.0')
        api(group: 'commons-codec', name: 'commons-codec', version: '1.15')
        api(group: 'commons-collections', name: 'commons-collections', version: '3.2.2')
        api(group: 'commons-configuration', name: 'commons-configuration', version: '1.10')
        api(group: 'commons-digester', name: 'commons-digester', version: '2.1')
        api(group: 'commons-fileupload', name: 'commons-fileupload', version: '1.4')
        api(group: 'commons-io', name: 'commons-io', version: get('commons-io.version'))
        api(group: 'commons-logging', name: 'commons-logging', version: '1.3.5')
        api(group: 'commons-modeler', name: 'commons-modeler', version: '2.0.1')
        api(group: 'commons-validator', name: 'commons-validator', version: get('commons-validator.version'))
        // Careful when upgrading this dependency: see GEODE-7370 and GEODE-8150.
        api(group: 'io.github.classgraph', name: 'classgraph', version: '4.8.147')
        api(group: 'io.github.resilience4j', name: 'resilience4j-retry', version: '1.7.1')
        api(group: 'io.lettuce', name: 'lettuce-core', version: '6.1.8.RELEASE')
        api(group: 'io.micrometer', name: 'micrometer-core', version: get('micrometer.version'))
        api(group: 'io.swagger.core.v3', name: 'swagger-annotations', version: '2.2.22')
        api(group: 'it.unimi.dsi', name: 'fastutil', version: get('fastutil.version'))
        api(group: 'jakarta.annotation', name: 'jakarta.annotation-api', version: get('jakarta.annotation.version'))
        api(group: 'jakarta.annotation', name: 'jsr250-api', version: '1.0')
        api(group: 'jakarta.ejb', name: 'jakarta.ejb-api', version: get('jakarta.ejb.version'))
        api(group: 'jakarta.mail', name: 'jakarta.mail-api', version: get('jakarta.mail.version'))
        api(group: 'jakarta.resource', name: 'jakarta.resource-api', version: get('jakarta.resource.version'))
        api(group: 'jakarta.servlet', name: 'jakarta.servlet-api', version: get('jakarta.servlet.version'))
        api(group: 'jakarta.transaction', name: 'jakarta.transaction-api', version: get('jakarta.transaction.version'))
        api(group: 'jakarta.xml.bind', name: 'jakarta.xml.bind-api', version: get('jakarta.xml.bind.version'))
        api(group: 'joda-time', name: 'joda-time', version: '2.12.7')
        api(group: 'junit', name: 'junit', version: get('junit.version'))
        api(group: 'mx4j', name: 'mx4j-tools', version: '3.0.1')
        api(group: 'mysql', name: 'mysql-connector-java', version: '5.1.46')
        api(group: 'net.java.dev.jna', name: 'jna', version: '5.11.0')
        api(group: 'net.java.dev.jna', name: 'jna-platform', version: '5.11.0')
        api(group: 'net.minidev', name: 'json-smart', version: '2.4.7')
        api(group: 'net.sf.jopt-simple', name: 'jopt-simple', version: '5.0.4')
        api(group: 'net.sourceforge.pmd', name: 'pmd-java', version: '6.42.0')
        api(group: 'net.sourceforge.pmd', name: 'pmd-test', version: '6.42.0')
        api(group: 'net.spy', name: 'spymemcached', version: '2.12.3')
        api(group: 'org.apache.bcel', name: 'bcel', version: '6.5.0')
        api(group: 'org.apache.commons', name: 'commons-lang3', version: get('commons-lang3.version'))
        api(group: 'org.apache.commons', name: 'commons-text', version: 1.9)
        api(group: 'org.apache.derby', name: 'derby', version: '10.14.2.0')
        // Apache HttpComponents 5.x - Modern HTTP client with HTTP/2 support
        api(group: 'org.apache.httpcomponents.client5', name: 'httpclient5', version: '5.4.4')
        api(group: 'org.apache.httpcomponents.core5', name: 'httpcore5', version: '5.3.4')
        api(group: 'org.apache.httpcomponents.core5', name: 'httpcore5-h2', version: '5.3.4')
        // Legacy HttpComponents 4.x (keep temporarily during migration, remove after complete)
        api(group: 'org.apache.httpcomponents', name: 'httpclient', version: '4.5.13')
        api(group: 'org.apache.httpcomponents', name: 'httpcore', version: '4.4.15')
        api(group: 'org.apache.shiro', name: 'shiro-core', version: get('shiro.version'))
        api(group: 'org.assertj', name: 'assertj-core', version: '3.22.0')
        api(group: 'org.awaitility', name: 'awaitility', version: '4.2.0')
        api(group: 'org.buildobjects', name: 'jproc', version: '2.8.0')
        api(group: 'org.codehaus.cargo', name: 'cargo-core-uberjar', version: '1.10.24')
        // Jetty 12: Core server module stays in org.eclipse.jetty
        api(group: 'org.eclipse.jetty', name: 'jetty-server', version: get('jetty.version'))
        // Jetty 12: Servlet and webapp modules moved to ee10 package for Jakarta EE 10
        api(group: 'org.eclipse.jetty.ee10', name: 'jetty-ee10-servlet', version: get('jetty.version'))
        api(group: 'org.eclipse.jetty.ee10', name: 'jetty-ee10-webapp', version: get('jetty.version'))
        // Jetty 12: Annotations module for ServletContainerInitializer discovery
        api(group: 'org.eclipse.jetty.ee10', name: 'jetty-ee10-annotations', version: get('jetty.version'))
        api(group: 'org.eclipse.persistence', name: 'javax.persistence', version: '2.2.1')
        api(group: 'org.httpunit', name: 'httpunit', version: '1.7.3')
        api(group: 'org.iq80.snappy', name: 'snappy', version: '0.5')
        api(group: 'org.jboss.modules', name: 'jboss-modules', version: get('jboss-modules.version'))
        api(group: 'org.jctools', name: 'jctools-core', version: '3.3.0')
        api(group: 'org.jgroups', name: 'jgroups', version: get('jgroups.version'))
        api(group: 'org.mortbay.jetty', name: 'servlet-api', version: '3.0.20100224')
        api(group: 'org.openjdk.jmh', name: 'jmh-core', version: '1.32')
        api(group: 'org.postgresql', name: 'postgresql', version: '42.2.8')
        api(group: 'org.skyscreamer', name: 'jsonassert', version: '1.5.0')
        api(group: 'org.slf4j', name: 'slf4j-api', version: get('slf4j-api.version'))
        api(group: 'org.apache.logging.log4j', name: 'log4j-slf4j2-impl', version: get('log4j-slf4j2-impl.version'))
        api(group: 'jakarta.transaction', name: 'jakarta.transaction-api', version: get('jakarta.transaction-api.version'))
        api(group: 'org.springframework.hateoas', name: 'spring-hateoas', version: get('springhateoas.version'))
        api(group: 'org.springframework.ldap', name: 'spring-ldap-core', version: get('springldap.version'))
        api(group: 'org.springframework.shell', name: 'spring-shell-starter', version: get('springshell.version'))
        api(group: 'org.testcontainers', name: 'testcontainers', version: '1.21.3')
        api(group: 'pl.pragmatists', name: 'JUnitParams', version: '1.1.0')
        api(group: 'xerces', name: 'xercesImpl', version: '2.12.0')
        api(group: 'xml-apis', name: 'xml-apis', version: '1.4.01')
        api(group: 'org.junit-pioneer', name: 'junit-pioneer', version: '1.7.1')
      }
    }

    dependencySet(group: 'org.mockito', version: '4.6.1') {
      entry('mockito-core')
      entry('mockito-junit-jupiter')
    }

    dependencySet(group: 'com.fasterxml.jackson.core', version: get('jackson.version')) {
      entry('jackson-annotations')
      entry('jackson-core')
    }

    dependencySet(group: 'com.fasterxml.jackson.core', version: get('jackson.databind.version')) {
      entry('jackson-databind')
    }

    dependencySet(group: 'com.fasterxml.jackson.datatype', version: get('jackson.version')) {
      entry('jackson-datatype-joda')
      entry('jackson-datatype-jsr310')
    }

    dependencySet(group: 'com.jayway.jsonpath', version: '2.7.0') {
      entry('json-path-assert')
      entry('json-path')
    }

    dependencySet(group: 'com.pholser', version: '1.0') {
      entry('junit-quickcheck-core')
      entry('junit-quickcheck-generators')
    }

    dependencySet(group: 'org.springdoc', version: get('springdoc.version')) {
      entry('springdoc-openapi-starter-webmvc-ui')
    }

    dependencySet(group: 'mx4j', version: '3.0.2') {
      entry('mx4j-remote')
      entry('mx4j')
    }

    dependencySet(group: 'org.apache.logging.log4j', version: get('log4j.version')) {
      entry('log4j-api')
      entry('log4j-core')
      entry('log4j-jcl')
      entry('log4j-jul')
      entry('log4j-slf4j-impl')
    }

    // Apache Lucene 9.12.3 - Upgraded for Jakarta EE 10 compatibility
    // Previous: 6.6.6 (2017) - Incompatible with modern Jakarta EE stack
    // Lucene 9.x requires Java 11+ and is designed for Jakarta EE compatibility
    // NOTE: Artifact names changed in Lucene 9.x: analyzers-* → analysis-*
    dependencySet(group: 'org.apache.lucene', version: '9.12.3') {
      entry('lucene-analysis-common')     // was: lucene-analyzers-common
      entry('lucene-analysis-phonetic')   // was: lucene-analyzers-phonetic
      entry('lucene-core')
      entry('lucene-queryparser')
      entry('lucene-test-framework')
    }

    dependencySet(group: 'org.hamcrest', version: '2.2') {
      entry('hamcrest')
    }

    dependencySet(group: 'org.junit.jupiter', version: get('junit-jupiter.version')) {
      entry('junit-jupiter-api')
      entry('junit-jupiter-params')
      entry('junit-jupiter-engine')
    }

    dependencySet(group: 'org.junit.vintage', version: get('junit-jupiter.version')) {
      entry('junit-vintage-engine')
    }

    dependencySet(group: 'org.seleniumhq.selenium', version: '3.141.59') {
      entry('selenium-api')
      entry('selenium-chrome-driver')
      entry('selenium-remote-driver')
      entry('selenium-support')
    }

    dependencySet(group: 'org.springframework.security', version: get('springsecurity.version')) {
      entry('spring-security-config')
      entry('spring-security-core')
      entry('spring-security-ldap')
      entry('spring-security-test')
      entry('spring-security-web')
      entry('spring-security-oauth2-core')
      entry('spring-security-oauth2-client')
      entry('spring-security-oauth2-jose')
    }

    dependencySet(group: 'org.springframework', version: get('springframework.version')) {
      // Spring 6.x requires explicit spring-aop dependency declaration
      // Previously implicit via transitive dependencies, now must be declared explicitly.
      // Missing this causes ClassNotFoundException: org.springframework.aop.TargetSource
      // during Spring context initialization, preventing HTTP service and WAN gateway startup.
      entry('spring-aop')
      entry('spring-aspects')
      entry('spring-beans')
      entry('spring-context')
      entry('spring-core')
      entry('spring-expression')
      entry('spring-messaging')
      entry('spring-oxm')
      entry('spring-test')
      entry('spring-tx')
      entry('spring-web')
      entry('spring-webmvc')
    }

    dependencySet(group: 'org.springframework.boot', version: get('springboot.version')) {
      entry('spring-boot-starter')
      entry('spring-boot-starter-jetty')
      entry('spring-boot-starter-validation')
      entry('spring-boot-starter-web')
      entry('spring-boot-autoconfigure')
    }

    dependencySet(group: 'org.jetbrains', version: '23.0.0') {
      entry('annotations')
    }

  }
}
