// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.cluster;

import com.openexchange.cluster.ClusterPorts;
import com.openexchange.cluster.MemberCompat;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * S4 test seam for {@link ShadowClusterRig}: each member boots at its own member version (generation 1, the level in
 * {@link #level}), set on its contexts after {@code ClusterConfig.create} the way the node does
 * ({@code ClusterConfig.memberCompat}); a refusal by any member's validator is recorded (in-process there is no exit;
 * the process tests assert the 78). {@code /ready} is read over HTTP from the member's own endpoint.
 */
final class MemberLevels {

    final Map<Integer, Integer> level = new ConcurrentHashMap<>();
    /** Every refusal, as {@code member <id>: <own> refused <other>}. */
    final List<String> refusals = new CopyOnWriteArrayList<>();
    private final ShadowClusterRig rig;

    MemberLevels(final ShadowClusterRig rig, final int initialLevel) {
        this.rig = rig;
        for (final ShadowClusterRig.Member m : rig.members) {
            level.put(m.id, initialLevel);
            m.afterConfig = mm -> mm.cfg.memberCompat(MemberCompat.of(1, level.get(mm.id)),
                    (own, other) -> refusals.add("member " + mm.id + ": " + own + " refused " + other));
        }
    }

    /** Stop {@code m}, set its level and boot it again; waits for its readiness. */
    void reboot(final ShadowClusterRig.Member m, final int newLevel) {
        rig.kill(m);
        level.put(m.id, newLevel);
        rig.restart(m);
        rig.awaitReady(m, 120_000);
    }

    int probePort(final ShadowClusterRig.Member m) {
        return ClusterPorts.port(m.id, rig.portBase, ClusterPorts.NODE_ENDPOINT_OFFSET, ClusterPorts.SHARED_HOST_STRIDE);
    }

    String ready(final ShadowClusterRig.Member m) throws IOException {
        final HttpURLConnection c = (HttpURLConnection)URI.create("http://127.0.0.1:" + probePort(m) + "/ready")
                .toURL().openConnection();
        c.setConnectTimeout(1_000);
        c.setReadTimeout(2_000);
        final var in = c.getResponseCode() < 400 ? c.getInputStream() : c.getErrorStream();
        try (in) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    static String token(final String body, final String name) {
        final int at = body.indexOf(" " + name + "=");
        if (at < 0) {
            return null;
        }
        final int from = at + name.length() + 2;
        int to = body.indexOf(' ', from);
        if (to < 0) {
            to = body.indexOf('\n', from);
        }
        return body.substring(from, to < 0 ? body.length() : to).trim();
    }

    /** Waits until {@code m}'s {@code /ready} shows {@code compat=g1.<level>}. */
    void awaitCompat(final ShadowClusterRig.Member m, final int expectedLevel) {
        final String expected = "g1." + expectedLevel;
        final String[] last = {""};
        rig.await(() -> {
            try {
                last[0] = ready(m);
                return expected.equals(token(last[0], "compat"));
            } catch (final IOException notYet) {
                return false;
            }
        }, 30_000, () -> "member " + m.id + " /ready must show compat=" + expected + ": " + last[0]);
    }
}
