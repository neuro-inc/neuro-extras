# NOTE: This script is going to be executed within the Kaniko image only, where we have busybox only.
set -o pipefail

result_path="${NE_RESULT_PATH:-/kaniko/.docker/config.json}"
prefix="${NE_AUTH_PREFIX:-NE_REGISTRY_AUTH}"

ifs_old=$IFS; IFS=$'\n\r'
extra_auths="`env | grep $prefix`"
res=""
if [ -n "${extra_auths}" ]
then
    if ! command -v jq &> /dev/null; then
        wget -q https://github.com/stedolan/jq/releases/download/jq-1.6/jq-linux64 -O /busybox/jq && chmod +x /busybox/jq;
    fi
    for auth in ${extra_auths}; do
        key="${auth%%=*}"
        value="${auth#*=}"
        if [ -f "${value}" ]; then
            # ENV var points to the file
            jq < ${value} > /dev/null 2>&1 && res=`echo $res | cat - ${value} | jq -sc 'reduce .[] as $item ({}; . * $item)'`
        else
            # ENV var contain auth info itself
            echo ${value} | jq > /dev/null 2>&1 && res=`echo $res ${value} | jq -sc 'reduce .[] as $item ({}; . * $item)'`
        fi
        # if we were able to read the config - lets store it
        if [[ ! $? -eq 0 ]]; then
            echo "[WARNING] Could not read config from '$key'" >&2
        fi
    done
    echo $res | jq > $result_path
fi
IFS=$ifs_old
