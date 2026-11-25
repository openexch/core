# syntax=docker/dockerfile:1
# SPDX-License-Identifier: Apache-2.0
#
# The core engine image: the published core-cluster.jar on a Java 21 runtime, started by deploy/bin/core-launch
# (the same launcher the systemd units run). Nothing is compiled here: the image wraps the published jar.
#
# Build context: a directory holding the published core-cluster.jar; deploy/ of this repo is a named context:
#
#   docker build -f deploy/oci/Dockerfile --build-context deploy=deploy -t openexchange/core <jar-dir>
#
# Run (read-only root, non-root, state on a volume; the arguments are core-launch's: role, driver mode):
#   docker run --read-only --tmpfs /tmp --shm-size 1g -v core-state:/var/lib/openexchange/core \
#       --env-file cluster.env --env-file member.env --env-file host.env --env-file secrets.env \
#       <image> member embedded
# External driver: run the cluster-tools image's aeronmd as the member's driver, share its network namespace
# (--network container:<driver>) and its driver directory (a shared tmpfs volume, AERON_DIR in both), same uid,
# and pass `member external`.

# Java 21 (Zulu, headless JRE), pinned by digest.
ARG JRE_IMAGE=azul/zulu-openjdk-debian:21-jre-headless-latest@sha256:3b45cab1e7092d81daede33a242871fd1110e208b760f75479c9495402feef7a
FROM ${JRE_IMAGE}
ARG OE_UID=10001
RUN groupadd --system --gid "${OE_UID}" openexchange \
    && useradd --system --uid "${OE_UID}" --gid "${OE_UID}" --no-create-home --home-dir /nonexistent \
        --shell /usr/sbin/nologin openexchange \
    && install -d -m 0755 /opt/openexchange/core/bin \
    && mkdir -p /var/lib/openexchange/core \
    && chown openexchange:openexchange /var/lib/openexchange/core
COPY --chmod=0444 core-cluster.jar /opt/openexchange/core/core-cluster.jar
COPY --from=deploy --chmod=0555 bin/core-launch /opt/openexchange/core/bin/core-launch
ENV BASE_DIR=/var/lib/openexchange/core
USER openexchange
VOLUME ["/var/lib/openexchange/core"]
# Stride 0 at the default base 9300: UDP 9301-9305 (archive control, client, member, log, transfer), TCP 9306 the
# node endpoint (/health, /ready, /metrics).
EXPOSE 9301-9305/udp 9306/tcp
# No default role or driver mode: core-launch refuses (78) a start that names neither.
ENTRYPOINT ["/opt/openexchange/core/bin/core-launch"]
