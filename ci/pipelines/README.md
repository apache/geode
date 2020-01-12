# Deploying Pipelines

```bash
./deploy_meta.sh <github account name>
```

# Generating Pipeline Templates Manually
To generate a pipeline, using jinja:
* Be in the pipelines directory or a subdirectory
* With ${GEODE_BRANCH} and ${GEODE_FORK} correctly set in your environment
(for the pipeline you want to create):

```bash
./render.py <directory containing jinja template>
```

The generated file should be named `generated-pipeline.yml`.