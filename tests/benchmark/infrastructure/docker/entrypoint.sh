#!/bin/sh

set -x

USER_ID=${LOCAL_USER_ID:-1001}

useradd --uid $USER_ID astro-sdk
chown -R astro-sdk /tmp
exec gosu astro-sdk "$@"
