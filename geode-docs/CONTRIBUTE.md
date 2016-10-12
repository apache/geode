# Contributing to the Project Geode Documentation

Project Geode welcomes your contributions to the community's documentation efforts. You can participate by writing new content, reviewing and editing existing content, or fixing bugs. This document covers the following topics:

- [How to Contribute](#contribute)
- [Document Source Files and Tools](#source_tools)
- [Writing Guidelines](#guidelines)

[]()

## How to Contribute

We use the fork-and-pull collaboration method on GitHub:

1. In your GitHub account, fork the `apache/incubator-geode` repository.
2. Create a local clone of your fork.
3. Make changes in the `geode-docs` directory and commit them in your fork.
4. In the `apache/incubator-geode` repository, create a pull request.

See [Using Pull Requests](https://help.github.com/articles/using-pull-requests/) on GitHub for more about the fork-and-pull collaboration method.

[]()

## Document Source Files and Tools

Project Geode documentation source files are written in markdown. Image files include .gif and .png graphics and editable image files in the open-source SVG format.

- [Working with Markdown Files](#markdown)
- [Working with Images and Graphics](#images)

[]()

### Working with Markdown Files

You can edit markdown files in any text editor. For more, read [Daring Fireball's Markdown Syntax page](https://daringfireball.net/projects/markdown/syntax).

[]()

### Working with Images and Graphics

Image files in .gif or .png format are in the `docs/images` directory in the Project Geode docs repo. Images in .svg format are in the `docs/images_svg` directory.

Most of the Project Geode image files have been converted to the open source SVG format. You can insert SVG images directly into an XML topic and modify images using a SVG editor.

The Wikipedia page [Comparison of Vector Graphics Editors](http://en.wikipedia.org/wiki/Comparison_of_vector_graphics_editors) provides a list and comparison of commercial and free vector graphics editors. Note, however, that not all of these programs support the SVG format.

[]()

## Writing Guidelines

The most important advice we can provide for working with the Project Geode docs is to spend some time becoming familiar with the existing source files and the structure of the project directory. In particular, note the following conventions and tips:

- Top-level subdirectories organize topics into "books": basic_config, configuring, developing, etc.

- Use lowercase characters for all file and directory names. Separate words in filenames with an underscore (`_`) character.

- Use the `.md` file extension for topic files.

- Add new topics to the existing directories by subject type. Only create a new directory if you are starting a new subject or a new book.

- To start a new topic, you can make a copy of an existing file with similar content and edit it.

- Use the appropriate document type for the content you are writing. Create multiple topics if you are writing overview, procedural, and reference content.
