# Deploying Geode to Concourse

In general, the pipelines are all deployed to CI via the `deploy_meta.sh` script. There are a
number of deployment scenarios it handles.

##### Prerequisites
You must be logged into the appropriate Google Cloud Platform project for this to work.
 
### Deployment Configuration
In this directory is a file called `meta.properties`. This file contains variables that change
how Geode is deployed. These values should be overriden by creating a file called 
`meta.properties.local` and changing value definitions in that file. Note that 
`meta.properties.local` is ignored by git because those types of changes are solely related to
a specific deployment scenario.

## Configuration Variables

### UPSTREAM_FORK
The UPSTREAM_FORK value indicates the GitHub user that hosts the master repository for Geode. Its
default is `apache`, which results in a repository URL of `https://github.com/apache/geode.git`.

### GEODE_FORK
The GEODE_FORK value indicates the github username that hosts the repository under test. For
example, if the GitHub user `molly` wishes to deploy a pipeline testing Geode from her fork, she
would set this value to `molly`. This results in the repository URL 
`https://github.com/molly/geode.git`.

### GEODE_REPO_NAME
The GEODE_REPO_NAME value indicates the name of the repository under test. This defaults to `geode`.

### CONCOURSE_HOST
The CONCOURSE_HOST value indicates the hostname of the concourse infrastructure you wish to deploy
the Geode CI pipeline to. The default value is `concourse.apachegeode-ci.info`. You might wish to 
change this value if you are hosting your own CI infrastructure for Geode.

### ARTIFACT_BUCKET
The ARTIFACT_BUCKET value indicates the Google Cloud Storage bucket name in which Concourse will 
store build artifacts and test results. The default value is `files.apachegeode-ci.info`.

### PUBLIC
The PUBLIC value indicates whether the deployed pipeline should be public or not. The default value
is `true`, though in certain situations this is overridden, such as a production deployment.

### REPOSITORY_PUBLIC
The REPOSITORY_PUBLIC value indicates whether the repository under test is public or not. The
default value is `true`. If this value is set to false, the pipeline will expect credentials to
be available in the vault deployment associated with the concourse deployment.

### GRADLE_GLOBAL_ARGS
The GRADLE_GLOBAL_ARGS value is for passing a set of arguments to gradle wherever it is invoked in
CI. The default value is an empty string.

## Deploying
Once you have made your configuration choices, simply run `deploy_meta.sh` with no arguments.
its default behavior is to deploy to the [production
environment](https://concourse.apachegeode-ci.info). It will autodetect your branch and deploy the 
appropriate pipeline. This will assume the branch is present in the
[upstream repository](https://github.com/apache/geode). The resulting meta pipeline will be named
`apache-<branch>-meta`.

If you change the configuration to use your own fork of Geode, the resulting meta pipeline will be
named `<your GitHub username>-<branch>-meta`.
