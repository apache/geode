# Project Geode End-User Documentation

Apache Geode provides the full source for end-user documentation in markdown format. The latest check-ins to `incubator-geode/geode-docs` are regularly built and published to http://geode.incubator.apache.org/docs/. Users can build the markdown into an HTML user guide using [Bookbinder](https://github.com/pivotal-cf/bookbinder) and the instructions below.

Bookbinder is a gem that binds  a unified documentation web application from markdown, html, and/or DITA source material. The source material for bookbinder must be stored either in local directories or in GitHub repositories. Bookbinder runs [middleman](http://middlemanapp.com/) to produce a Rackup app that can be deployed locally or as a Web application.

This document contains instructions for building and viewing the Geode documentation locally.

- [Prerequisites](#prereq)
- [Bookbinder Usage](#usage)
- [Building the Documentation](#building)

## Prerequisites

Bookbinder requires Ruby version 2.0.0-p195 or higher.

Follow the instructions below to install Bookbinder:

1. Add gem "bookbindery" to your Gemfile.
2. Run `bundle install` to install the dependencies specified in your Gemfile.

## Bookbinder Usage

Bookbinder is meant to be used from within a project called a **book**. The book includes a configuration file that describes which documentation repositories to use as source materials. Bookbinder provides a set of scripts to aggregate those repositories and publish them to various locations.

For Geode, a preconfigured **book** is provided in the directory `geode-book`, which gathers content from the directory `geode-docs`. You can use this configuration to build HTML for Geode on your local system.

The installed `config.yml` file configures the Geode book for building locally. The file configures the local directory for the markdown source files. It uses the following configuration:

  ```
book_repo: apache/incubator-geode/geode-book
public_host: localhost

sections:
- repository:
    name: geode-docs
  directory: docs
  subnav_template: geode-subnav

template_variables:
  support_url: http://geode.incubator.apache.org/community
  product_url: http://geode.incubator.apache.org/
  book_title: Apache Geode (Incubating) Documentation
  support_link: <a href="http://geode.incubator.apache.org/community" target="_blank">Community</a>
  support_call_to_action: <a href="http://geode.incubator.apache.org/community" target="_blank">Need Help?</a>
  product_link: <div class="header-item"><a href="http://geode.incubator.apache.org/">Back to Geode Page</a></div>
  domain_name: apache.org
  book_title_short: Geode Docs

broken_link_exclusions: iefix|using_custom_classes|arrowhead|cppdocs|DotNetDocs
  ```

## Building the Documentation

1. The GemFile in the `geode-book` directory already defines the `gem "bookbindery"` dependency. Make sure you are in the `geode-book` directory and enter:

  ```
  $ bundle install
  ```

2. To build the files locally using the installed `config.yml` file, enter:

  ```
  $ bundle exec bookbinder bind local
  ```

  Bookbinder converts the markdown source into HTML, which it puts in the `final_app` directory.

5. To view the local documentation, do the following:

  ```
  $ cd final_app
  $ rackup
  ```

  You can now view the local documentation at <http://localhost:9292>.
