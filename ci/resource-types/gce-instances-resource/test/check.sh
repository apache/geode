jq -n '
{
  "version": {},
  "source": {
    "filter": "labels.time-to-live:* AND labels.time-to-live<$(date +%s) AND status:RUNNING"
  }
}
' | \
    docker run -i --rm \
        --env BUILD_TEAM_NAME=Team \
        --env BUILD_PIPELINE_NAME=Pipeline \
        --env BUILD_JOB_NAME=Job \
        --env BUILD_NAME=1 \
        gce-instances-resource /opt/resource/check /tmp

jq -n '
{
  "version": {},
  "source": {
    "filter": "labels.time-to-live:* AND labels.time-to-live<$(($(date +%s) - 86400)) AND status:TERMINATED"
  }
}
' | \
    docker run -i --rm \
        --env BUILD_TEAM_NAME=Team \
        --env BUILD_PIPELINE_NAME=Pipeline \
        --env BUILD_JOB_NAME=Job \
        --env BUILD_NAME=1 \
        gce-instances-resource /opt/resource/check /tmp
