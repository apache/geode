jq -n '
{
  "version": {
    "id": "1929725350891113587"
  },
  "source": {
  }
}
' | \
    docker run -i --rm \
        --env BUILD_TEAM_NAME=Team \
        --env BUILD_PIPELINE_NAME=Pipeline \
        --env BUILD_JOB_NAME=Job \
        --env BUILD_NAME=1 \
        gce-reaper-resource /opt/resource/in /tmp
