# Apache Geode User Guide

This document contains instructions for building and viewing the Apache Geode User Guide locally and moving the built guide to the Apache Geode website for publication.

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

## Publishing the User Guide to the Geode Website

Once you have reviewed your local build of the User Guide, you can move it to the Apache Geode website by doing the following:

1. Navigate to: `{geode-project-dir}/geode-book/final_app/public/docs/guide/NN`, where `NN` is the product version of your documentation (e.g., `{geode-project-dir}/geode-book/final_app/public/docs/guide/11` if you are building the documentation for Apache Geode 1.1).

2. To move the directory, enter:

    ```
    $ tar cvf ~/Desktop/new-guide-content.tar .
    $ cd ../../../../../../geode-site/website/content/docs/guide/NN
    $ tar xvf ~/Desktop/new-guide-content.tar
    ```
   **Note:** If the "`NN`" directory doesn't exist in the `{geode-project-dir}/geode-site/website/content/docs/guide/` directory, you will need to create it first.

3. Follow the instructions at `{geode-project-dir}/geode-site/website/README.md` to build, review, and publish the Apache Geode website.
