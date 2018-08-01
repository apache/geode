jq -n '
{
  "params": {}
  },
  "source": {}
}
' | \
    docker run -i --rm \
        --env BUILD_TEAM_NAME=Team \
        --env BUILD_PIPELINE_NAME=Pipeline \
        --env BUILD_JOB_NAME=Job \
        --env BUILD_NAME=1 \
        gce-instances-resource /opt/resource/out /tmp
