# Project Geode End-User Documentation

The latest check-ins to `project-geode/docs` are automatically built and published to <http://geode-docs.cfapps.io>.

Project Geode provides the full source for end-user documentation in markdown format. The markdown can be built into HTML using [Bookbinder](https://github.com/pivotal-cf/bookbinder).

Bookbinder is a gem that binds together a unified documentation web application from markdown, html, and/or DITA source material. The source material for bookbinder must be stored either in local directories or in GitHub repositories. Bookbinder runs [middleman](http://middlemanapp.com/) to produce a Rackup app that can be deployed locally or as a Web application.

This document contains instructions for building the local Geode documentation and publishing it to Cloud Foundry or Pivotal Web Services. It contains the sections:

- [Bookbinder Usage](#usage)
- [Prerequisites](#prereq)
- [Building the Documentation](#building)
- [Publishing the Documentation](#publishing)
- [Getting More Information](#moreinfo)

[]()

## Bookbinder Usage

Bookbinder is meant to be used from within a project called a **book**. The book includes a configuration file that describes which documentation repositories to use as source materials. Bookbinder provides a set of scripts to aggregate those repositories and publish them to various locations.

For Geode, a preconfigured **book** is provided in the directory `/geode-book`. You can use this configuration to build HTML for project Geode on your local system.

[]()

## Prerequisites

- Bookbinder requires Ruby version 2.0.0-p195 or higher.

## Building the Documentation

1. Begin by moving or copying the `/geode-book directory` to a directory that is parallel to `project-geode/docs`. For example:

  ```
  $ cd /repos/project-geode/docs
  $ cp -r geode-book ..
  $ cd ../geode-book
  ```

2. The GemFile in the book directory already defines the `gem "bookbindery"` dependency. Make sure you are in the relocated book directory and enter:

  ```
  $ bundle install
  ```

3. The installed `config.yml` file configures the Project Geode book for building locally. The installed file configures the local directory for the markdown source files. It uses the configuration:

  ```
  book_repo: project-geode/geode-book
  public_host: localhost

  sections:
  - repository:
     name: gemfire/docs-geode-md
   directory: docs-geode
   subnav_template: geode-nav

  template_variables:
   product_url: http://geode.incubator.apache.org
   book_title: Project Geode Documentation
  ```

4. Build the files locally using the installed `config.yml` file. Execute the command:

  ```
  $ bundle exec bookbinder bind local
  ```

  Bookbinder converts the markdown source into HTML, putting the final output in the `final_app` directory.

5. To view the local documentation:

  ```
  $ cd final_app
  $ rackup
  ```

  You can now view the local documentation at <http://localhost:9292>

[]()

## Publishing the Documentation

Because the `final_app` directory contains the full output of the HTML conversion process, you can easily publish this directory as a hosted Web application.

[]()

## Getting More Information

Bookbinder provides additional functionality to construct books from multiple Github repos, to perform variable substitution, and also to automatically build documentation in a continuous integration pipeline. For more information, see <https://github.com/pivotal-cf/bookbinder>.

The latest check-ins to `gemfire/docs-geode-md` are automatically built and published to <http://geode-docs.cfapps.io>.
