# Deploying Geode to Concourse

In general, the pipelines are all deployed to CI via the `deploy_meta.sh` script. There are a
number of deployment scenarios it handles.

##### Prerequisites
You must be logged into the appropriate Google Cloud Platform project for this to work.
 
## Deploying to Production
If you run `deploy_meta.sh` with no arguments, its default behavior is to deploy to the [production
environment](https://concourse.apachegeode-ci.info). It will autodetect your branch and deploy the 
appropriate pipeline. This will assume the branch is present in the
[upstream repository](https://github.com/apache/geode). The resulting meta pipeline will be named
`apache-<branch>-meta`.

## Deploying from your fork to Geode Concourse
If you wish to deploy the meta pipeline using your fork as the source, you must specify your github
username as the first argument: `./deploy_meta.sh <github username>`. The resulting meta pipeline
will be named `<github username>-<branch>-meta`. The default value for this argument is `apache`.

## Specifying a different repository
If your repository is named something other than `geode`, you must specify it as a second argument:
`./deploy_meta.sh apache geode-testing`. There are no optional arguments, so if you need
to provide an argument beyond the first, you must manually specify earlier arguments even if
you are using default values.

## Specifying a different upstream
If your upstream repository owner is not `apache` (you forked a fork) then you must specify it as the third argument:
`./deploy_meta.sh <github username> geode-testing not-apache`.

## Specifying a different concourse infrastructure
If you would like to deploy the pipeline to a concourse infrastructure other than
`concourse.apachegeode-ci.info`, you must provide its hostname as the fourth argument:
`./deploy_meta.sh apache geode apache concourse.mydomain.com`

## Specifying the GCS bucket for artifact and test result storage
If you would like to use a bucket other than `files.apachegeode-ci.info` for storing test results
and artifacts, you must specify it as the fifth argument:
`./deploy_meta.sh apache geode apache concourse.apachegeode-ci.info <my-gcs-bucket-name>`

## Changing whether the created pipelines are public or not
If the upstream and fork names do not match, the pipeline will not be made public. Otherwise
the pipelines will be public by default, but can be overridden with the fifth argument:
`./deploy_meta.sh apache geode apache concourse.apachegeode-ci.info files.apachegeode-ci.info false`
