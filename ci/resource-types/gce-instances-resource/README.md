# Google Cloud Compute Engine Listing Concourse Resource Type

Lists Google Cloud compute instances.
## Resource Type Configuration
```
resource_types:
- name: gce-instances
  type: docker-image
  source:
    repository: gce-instances-resource
    tag: latest
```

## Source Configuration
- `filter`: (optional) A `bash` evaluated filter applied to listing instances.

## `check`: Emits instances as versions
If `filter` is provided in source configuration then the list of instances is filtered. Should be 
combined with with a get task configured with `version: every` so that all instances found by check
will be processed. 

## `in`: Creates or destroys a instance
Produces the following files with values from an instance found by `check`.
- `id`: Contains the instance id.
- `name`: Contains the instance name.
- `selfLink`: Contains the instance selfLink URI.
- `description`: Contains the description of instance in JSON format. 

## Examples ##
Stop all instances that are running and have a `time-to-live` label value older than now.
```
resource_types:
- name: gce-instances
  type: docker-image
  source:
    repository: gce-instances-resource
    tag: latest

resources:
- name: stopable-instance
  type: gce-instances
  source:
    filter: 'labels.time-to-live:* AND labels.time-to-live<$(date +%s) AND status:RUNNING'

jobs:
- name: stop-instance
  plan:
  - get: stopable-instance
    version: every
    trigger: true
  - task: stop-instances
    config:
      platform: linux
      image_resource:
        type: docker-image
        source:
          repository: google/cloud-sdk
          tag: alpine
      inputs:
      - name: stopable-instance
      run:
        path: /bin/sh
        args:
        - -exc
        - |
          gcloud compute instances stop $(cat stopable-instance/selfLink) --quiet
```