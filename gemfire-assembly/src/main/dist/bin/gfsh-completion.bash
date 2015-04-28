#!/usr/bin/env bash
#
# Auto completion script for GemFire's gfsh script
#
# Either explicitly source it into your shell enviroment, set it up in your
# .bashrc or .bash_profile or copy it to /etc/bash_completion.d
#

_gfsh() {
    # The main verbs gfsh understands
    declare -r VERBS="compact describe encrypt help run start status stop validate version"

    # The nouns relevant to the verbs
    declare -r N_compact="offline-disk-store"
    declare -r N_describe="offline-disk-store"
    declare -r N_encrypt="password"
    declare -r N_help="$VERBS"
    declare -r N_start="jconsole jvisualvm locator pulse server vsd"
    declare -r N_status="locator server"
    declare -r N_stop="locator server"
    declare -r N_validate="offline-disk-store"

    # The options relevant to each verb-noun combination
    declare -r OPT_compact_offline_disk_store="--name --disk-dirs --max-oplog-size --J"
    declare -r OPT_describe_offline_disk_store="--name=value --disk-dirs --region"
    declare -r OPT_encrypt_password="--password"
    declare -r OPT_run="--file --quiet --continue-on-error"
    declare -r OPT_start_jconsole="--interval --notile --pluginpath --version --J"
    declare -r OPT_start_jvisualvm="--J"
    declare -r OPT_start_locator="--name --bind-address --force --group \
        --hostname-for-clients --locators --log-level --mcast-address \
        --mcast-port --port --dir --properties-file --security-properties-file \
        --initial-heap --max-heap --J --connect --enable-cluster-configuration \
        --load-cluster-configuration-from-dir"
    declare -r OPT_start_pulse="--url"
    declare -r OPT_start_server="--name --assign-buckets --bind-address \
        --cache-xml-file --classpath --disable-default-server \
        --disable-exit-when-out-of-memory --enable-time-statistics --force \
        --properties-file --security-properties-file --group \
        --locators \
        --log-level --mcast-address --mcast-port --name --memcached-port \
        --memcached-protocol --rebalance --server-bind-address --server-port \
        --spring-xml-location --statistic-archive-file --dir --initial-heap \
        --max-heap --use-cluster-configuration --J --critical-heap-percentage \
        --eviction-heap-percentage --hostname-for-clients --max-connections \
        --message-time-to-live --max-message-count --max-threads --socket-buffer-size"
    declare -r OPT_start_vsd="--file"
    declare -r OPT_status_locator="--name --host --port --pid --dir"
    declare -r OPT_status_server="--name --pid --dir"
    declare -r OPT_stop_locator="--name --pid --dir"
    declare -r OPT_stop_server="--name --pid --dir"
    declare -r OPT_validate_offline_disk_store="--name --disk-dirs"
    declare -r OPT_version="--full"

    local cur=${COMP_WORDS[COMP_CWORD]}
    local use="VERBS"

    local verb=${COMP_WORDS[1]}
    local noun=${COMP_WORDS[2]}

    # Ignore potential options
    noun=${noun##-*}

    # Because variable names can't have dashes
    noun=${noun//-/_}

    if [[ -n "$noun" ]]; then
        use="OPT_${verb}_${noun}"
        if [[ -z "${!use}" ]]; then
            use="N_$verb"
        fi
    elif [[ -n "$verb" ]]; then
        # Special handling for these as they don't have associated nouns
        if [[ "$verb" = "run" || "$verb" = "version" ]]; then
            use="OPT_$verb"
        else
            use="N_$verb"
            if [[ -z "${!use}" ]]; then
                use="VERBS"
            fi
        fi
    fi

    COMPREPLY=( $( compgen -W "${!use}" -- "$cur" ) )
}

complete -F _gfsh gfsh
    
