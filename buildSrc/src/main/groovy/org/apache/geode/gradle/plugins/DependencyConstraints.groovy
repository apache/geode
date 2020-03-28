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

import org.gradle.api.Plugin
import org.gradle.api.Project

class DependencyConstraints implements Plugin<Project> {
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
    deps.put("commons-io.version", "2.6")
    deps.put("commons-lang3.version", "3.10")
    deps.put("commons-validator.version", "1.6")
    deps.put("fastutil.version", "8.3.1")
    deps.put("javax.transaction-api.version", "1.3")
    deps.put("jgroups.version", "3.6.14.Final")
    deps.put("log4j.version", "2.13.1")
    deps.put("micrometer.version", "1.4.1")
    deps.put("shiro.version", "1.5.2")
    deps.put("slf4j-api.version", "1.7.30")

    // These version numbers are used in testing various versions of tomcat and are consumed explicitly
    // in will be called explicitly in the relevant extensions module, and respective configurations
    // in geode-assembly.gradle.  Moreover, dependencyManagement does not seem to play nicely when
    // specifying @zip in a dependency, the manner in which we consume them in custom configurations.
    // This would possibly be corrected if they were proper source sets.
    deps.put("tomcat6.version", "6.0.37")
    deps.put("tomcat7.version", "7.0.99")
    deps.put("tomcat8.version", "8.5.50")
    deps.put("tomcat9.version", "9.0.12")

    // The jetty version is also hard-coded in geode-assembly:test
    // at o.a.g.sessions.tests.GenericAppServerInstall.java
    deps.put("jetty.version", "9.4.21.v20190926")

    // These version numbers are consumed by protobuf configurations that are plugin-specific and not
    // part of the typical Gradle dependency configurations.
    deps.put("protoc.version", "3.11.4")
    deps.put("protobuf-gradle-plugin.version", "0.8.10")
    deps.put("protobuf-java.version", "3.11.4")

    // These versions are referenced in test.gradle, which is aggressively injected into all projects.
    deps.put("junit.version", "4.12")
    deps.put("cglib.version", "3.3.0")
    return deps
  }

  @Override
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
        api(group: 'com.arakelian', name: 'java-jq', version: '0.10.1')
        api(group: 'com.carrotsearch.randomizedtesting', name: 'randomizedtesting-runner', version: '2.7.7')
        api(group: 'com.fasterxml.jackson.datatype', name: 'jackson-datatype-joda', version: '2.9.8')
        api(group: 'com.fasterxml.jackson.module', name: 'jackson-module-scala_2.10', version: '2.10.0')
        api(group: 'com.github.davidmoten', name: 'geo', version: '0.7.7')
        api(group: 'com.github.stefanbirkner', name: 'system-rules', version: '1.19.0')
        api(group: 'com.github.stephenc.findbugs', name: 'findbugs-annotations', version: '1.3.9-1')
        api(group: 'com.google.code.findbugs', name: 'jsr305', version: '3.0.2')
        api(group: 'com.google.guava', name: 'guava', version: '28.2-jre')
        api(group: 'com.google.protobuf', name: 'protobuf-gradle-plugin', version: get('protobuf-gradle-plugin.version'))
        api(group: 'com.google.protobuf', name: 'protobuf-java', version: get('protobuf-java.version'))
        api(group: 'com.healthmarketscience.rmiio', name: 'rmiio', version: '2.1.2')
        api(group: 'com.mockrunner', name: 'mockrunner-servlet', version: '2.0.4')
        api(group: 'com.nimbusds', name:'nimbus-jose-jwt', version:'8.11')
        api(group: 'com.sun.activation', name: 'javax.activation', version: '1.2.0')
        api(group: 'com.sun.istack', name: 'istack-commons-runtime', version: '3.0.11')
        api(group: 'com.sun.xml.bind', name: 'jaxb-impl', version: '2.3.2')
        api(group: 'com.tngtech.archunit', name:'archunit-junit4', version: '0.12.0')
        api(group: 'com.zaxxer', name: 'HikariCP', version: '3.4.2')
        api(group: 'commons-beanutils', name: 'commons-beanutils', version: '1.9.4')
        api(group: 'commons-codec', name: 'commons-codec', version: '1.14')
        api(group: 'commons-collections', name: 'commons-collections', version: '3.2.2')
        api(group: 'commons-configuration', name: 'commons-configuration', version: '1.10')
        api(group: 'commons-digester', name: 'commons-digester', version: '2.1')
        api(group: 'commons-fileupload', name: 'commons-fileupload', version: '1.4')
        api(group: 'commons-io', name: 'commons-io', version: get('commons-io.version'))
        api(group: 'commons-logging', name: 'commons-logging', version: '1.2')
        api(group: 'commons-modeler', name: 'commons-modeler', version: '2.0.1')
        api(group: 'commons-validator', name: 'commons-validator', version: get('commons-validator.version'))
        api(group: 'io.github.classgraph', name: 'classgraph', version: '4.8.68')
        api(group: 'io.micrometer', name: 'micrometer-core', version: get('micrometer.version'))
        api(group: 'io.netty', name: 'netty-all', version: '4.1.48.Final')
        api(group: 'io.swagger', name: 'swagger-annotations', version: '1.5.23')
        api(group: 'it.unimi.dsi', name: 'fastutil', version: get('fastutil.version'))
        api(group: 'javax.annotation', name: 'javax.annotation-api', version: '1.3.2')
        api(group: 'javax.annotation', name: 'jsr250-api', version: '1.0')
        api(group: 'javax.ejb', name: 'ejb-api', version: '3.0')
        api(group: 'javax.mail', name: 'javax.mail-api', version: '1.6.2')
        api(group: 'javax.resource', name: 'javax.resource-api', version: '1.7.1')
        api(group: 'javax.servlet', name: 'javax.servlet-api', version: '3.1.0')
        api(group: 'javax.xml.bind', name: 'jaxb-api', version: '2.3.1')
        api(group: 'joda-time', name: 'joda-time', version: '2.9.8')
        api(group: 'junit', name: 'junit', version: get('junit.version'))
        api(group: 'mx4j', name: 'mx4j-tools', version: '3.0.1')
        api(group: 'mysql', name: 'mysql-connector-java', version: '5.1.46')
        api(group: 'net.java.dev.jna', name: 'jna', version: '5.5.0')
        api(group: 'net.java.dev.jna', name: 'jna-platform', version: '5.5.0')
        api(group: 'net.openhft', name: 'compiler', version: '2.3.5')
        api(group: 'net.sf.jopt-simple', name: 'jopt-simple', version: '5.0.4')
        api(group: 'net.sourceforge.pmd', name: 'pmd-java', version: '6.22.0')
        api(group: 'net.sourceforge.pmd', name: 'pmd-test', version: '6.22.0')
        api(group: 'net.spy', name: 'spymemcached', version: '2.12.3')
        api(group: 'org.apache.bcel', name: 'bcel', version: '6.4.1')
        api(group: 'org.apache.commons', name: 'commons-lang3', version: get('commons-lang3.version'))
        api(group: 'org.apache.commons', name: 'commons-text', version: 1.8)
        api(group: 'org.apache.derby', name: 'derby', version: '10.14.2.0')
        api(group: 'org.apache.httpcomponents', name: 'httpclient', version: '4.5.12')
        api(group: 'org.apache.httpcomponents', name: 'httpcore', version: '4.4.13')
        api(group: 'org.apache.shiro', name: 'shiro-core', version: get('shiro.version'))
        api(group: 'org.assertj', name: 'assertj-core', version: '3.15.0')
        api(group: 'org.awaitility', name: 'awaitility', version: '4.0.2')
        api(group: 'org.bouncycastle', name: 'bcpkix-jdk15on', version: '1.64')
        api(group: 'org.codehaus.cargo', name: 'cargo-core-uberjar', version: '1.7.11')
        api(group: 'org.eclipse.jetty', name: 'jetty-server', version: get('jetty.version'))
        api(group: 'org.eclipse.jetty', name: 'jetty-webapp', version: get('jetty.version'))
        api(group: 'org.eclipse.persistence', name: 'javax.persistence', version: '2.2.1')
        api(group: 'org.httpunit', name: 'httpunit', version: '1.7.3')
        api(group: 'org.iq80.snappy', name: 'snappy', version: '0.4')
        api(group: 'org.jgroups', name: 'jgroups', version: get('jgroups.version'))
        api(group: 'org.mockito', name: 'mockito-core', version: '2.23.0')
        api(group: 'org.mortbay.jetty', name: 'servlet-api', version: '3.0.20100224')
        api(group: 'org.openjdk.jmh', name: 'jmh-core', version: '1.23')
        api(group: 'org.postgresql', name: 'postgresql', version: '42.2.8')
        api(group: 'org.skyscreamer', name: 'jsonassert', version: '1.5.0')
        api(group: 'org.slf4j', name: 'slf4j-api', version: get('slf4j-api.version'))
        api(group: 'org.springframework.hateoas', name: 'spring-hateoas', version: '1.0.1.RELEASE')
        api(group: 'org.springframework.ldap', name: 'spring-ldap-core', version: '2.3.2.RELEASE')
        api(group: 'org.springframework.shell', name: 'spring-shell', version: '1.2.0.RELEASE')
        api(group: 'org.testcontainers', name: 'testcontainers', version: '1.13.0')
        api(group: 'pl.pragmatists', name: 'JUnitParams', version: '1.1.0')
        api(group: 'redis.clients', name: 'jedis', version: '3.2.0')
        api(group: 'xerces', name: 'xercesImpl', version: '2.12.0')
      }
    }

    dependencySet(group: 'com.fasterxml.jackson.core', version: '2.10.1') {
      entry('jackson-annotations')
      entry('jackson-core')
      entry('jackson-databind')
    }

    dependencySet(group: 'com.jayway.jsonpath', version: '2.4.0') {
      entry('json-path-assert')
      entry('json-path')
    }

    dependencySet(group: 'com.palantir.docker.compose', version: '0.31.1') {
      entry('docker-compose-rule-core')
      entry('docker-compose-rule-junit4')
    }

    dependencySet(group: 'com.pholser', version: '0.9.1') {
      entry('junit-quickcheck-core')
      entry('junit-quickcheck-generators')
    }

    dependencySet(group: 'io.springfox', version: '2.9.2') {
      entry('springfox-swagger-ui')
      entry('springfox-swagger2')
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

    dependencySet(group: 'org.apache.lucene', version: '6.6.6') {
      entry('lucene-analyzers-common')
      entry('lucene-analyzers-phonetic')
      entry('lucene-core')
      entry('lucene-queryparser')
      entry('lucene-test-framework')
    }

    dependencySet(group: 'org.hamcrest', version: '1.3') {
      entry('hamcrest-all')
      entry('hamcrest-core')
      entry('hamcrest-library')
    }

    dependencySet(group: 'org.powermock', version: '2.0.2') {
      entry('powermock-api-mockito2')
      entry('powermock-core')
      entry('powermock-module-junit4')
    }

    dependencySet(group: 'org.seleniumhq.selenium', version: '3.141.59') {
      entry('selenium-api')
      entry('selenium-chrome-driver')
      entry('selenium-remote-driver')
      entry('selenium-support')
    }

    dependencySet(group: 'org.springframework.security', version: '5.3.1.RELEASE') {
      entry('spring-security-config')
      entry('spring-security-core')
      entry('spring-security-ldap')
      entry('spring-security-test')
      entry('spring-security-web')
      entry('spring-security-oauth2-core')
      entry('spring-security-oauth2-client')
      entry('spring-security-oauth2-jose')
    }

    dependencySet(group: 'org.springframework', version: '5.2.5.RELEASE') {
      entry('spring-aspects')
      entry('spring-beans')
      entry('spring-context')
      entry('spring-core')
      entry('spring-expression')
      entry('spring-oxm')
      entry('spring-test')
      entry('spring-tx')
      entry('spring-web')
      entry('spring-webmvc')
    }
  }
}
