# Apache Geode User Guide

This document contains instructions for building and viewing the Apache Geode User Guide locally.

- [About](#about)
- [Prerequisites](#prerequisites)
- [Bookbinder Usage](#bookbinder-usage)
- [Building the Documentation](#building-the-documentation)
- [Embedding the User Guide in the Geode Website](#embedding-the-user-guide-in-the-geode-website)

## About

Apache Geode provides the full source for the Apache Geode User Guide in markdown format (see `{geode-project-dir}/geode-docs/CONTRIBUTE.md`). The latest check-ins to `{geode-project-dir}/geode-docs` on the `develop` branch are regularly built and published to http://geode.apache.org/docs/. Users can build the markdown into an HTML user guide using [Bookbinder](https://github.com/pivotal-cf/bookbinder) and the instructions below.

Bookbinder is a Ruby gem that binds  a unified documentation web application from markdown, html, and/or DITA source material. The source material for bookbinder must be stored either in local directories or in GitHub repositories. Bookbinder runs [Middleman](http://middlemanapp.com/) to produce a Rackup app that can be deployed locally or as a web application.

## Prerequisites

Bookbinder requires Ruby version 2.0.0-p195 or higher.

Follow the instructions below to install Bookbinder:

1. Add gem "bookbindery" to your Gemfile.
2. Run `bundle install` to install the dependencies specified in your Gemfile.

## Bookbinder Usage

Bookbinder is meant to be used from within a project called a **book**. The book includes a configuration file that describes which documentation repositories to use as source materials. Bookbinder provides a set of scripts to aggregate those repositories and publish them to various locations.

For Geode, a preconfigured **book** is provided in the directory `{geode-project-dir}/geode-book`, which gathers content from the directory `{geode-project-dir}/geode-docs`. You can use this configuration to build an HTML version of the Apache Geode User Guide on your local system.

## Building the Documentation

1. The GemFile in the `geode-book` directory already defines the `gem "bookbindery"` dependency. Make sure you are in the `{geode-project-dir}/geode-book` directory and enter:

    ```
    $ bundle install
    ```

   Note: You will not have to run `bundle install` on subsequent builds.

2. To build the documentation locally using the installed `config.yml` file, enter:

    ```
    $ bundle exec bookbinder bind local
    ```

   Bookbinder converts the markdown source into HTML, which it puts in the `final_app` directory.

3. Navigate to `{geode-project-dir}/geode-book/final_app/` and enter:

    ```
    $ bundle install
    ```

   Note: You will not have to run `bundle install` on subsequent builds.

4. To start a local website of the Apache Geode User Guide, enter:

    ```
    $ rackup
    ```

   You can now view the local documentation at <http://localhost:9292>. 

## Embedding the User Guide in the Geode Website

Once you have reviewed your local build of the User Guide, you can embed it in the Apache Geode website by doing the following:

1. Compile the website source *before adding the User Guide files*. In the `{geode-project-dir}/geode-site/website` directory, enter:

    ```
    $ nanoc compile
    ```

2. Move the built User Guide files to the Geode website. Navigate to: `{geode-project-dir}/geode-book/final_app/public/` and enter:

    ```
    $ tar cvf ~/Desktop/new-guide-content.tar .
    $ cd ../../../geode-site/content
    $ tar xvf ~/Desktop/new-guide-content.tar
    ```

3. In the `{geode-project-dir}/geode-site/website` directory, enter:

    ```
    $ nanoc view
    ```

   You can now view the local website at http://localhost:3000.

4. Once you have reviewed your changes, follow the instructions at `{geode-project-dir}/geode-site/website/README.md` for propagating changes to the `asf-site` branch.
