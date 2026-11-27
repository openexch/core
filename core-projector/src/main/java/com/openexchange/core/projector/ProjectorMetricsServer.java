// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.projector;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * Minimal Prometheus {@code /metrics} + JSON {@code /health} endpoint for the money projector.
 * JDK-built-in HTTP server on its own daemon thread, mirroring the settlement bridge's metrics
 * server: zero dependencies, reads {@link ProjectorState}'s volatile single-writer fields.
 * {@code /health} flips to 503 the instant the projector halts on a detected gap.
 */
public final class ProjectorMetricsServer {

    private final ProjectorState state;
    private HttpServer server;

    public ProjectorMetricsServer(final ProjectorState state) {
        this.state = state;
    }

    /** Starts the server. Port 0 binds an OS-assigned ephemeral port; read it back via {@link #port()}. */
    public void start(final int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/metrics", this::handleMetrics);
        server.createContext("/health", this::handleHealth);
        server.setExecutor(runnable -> {
            final Thread t = new Thread(runnable, "projector-metrics-http");
            t.setDaemon(true);
            t.start();
        });
        server.start();
        System.out.println("[PROJECTOR] metrics /metrics + /health on port " + port());
    }

    /** The actual bound port, resolving a requested port 0 to the ephemeral port the OS picked. */
    public int port() {
        return server.getAddress().getPort();
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    private void handleMetrics(final HttpExchange exchange) throws IOException {
        final byte[] body = render().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    private void handleHealth(final HttpExchange exchange) throws IOException {
        // Single-writer volatile fields: read each once so the pair reported is self-consistent.
        final boolean halted = state.halted;
        final boolean pgConnected = state.connectedToPg;
        final byte[] body = ("{\"halted\":" + halted + ",\"pgConnected\":" + pgConnected + "}")
                .getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(halted ? 503 : 200, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    String render() {
        final StringBuilder sb = new StringBuilder(1024);
        gauge(sb, "projector_halted",
                "1 if the projector is parked (a gap on every source, a payload conflict, a refused version), else 0",
                state.halted ? 1 : 0);
        gauge(sb, "projector_pg_connected", "1 if the projector's last Postgres flush/connect succeeded, else 0",
                state.connectedToPg ? 1 : 0);
        counter(sb, "projector_epochs_total", "Projector epochs started since process start", state.epochs);
        counter(sb, "projector_errors_total", "Epoch failures since process start", state.errors);
        counter(sb, "projector_rows_projected_total",
                "Journal records durably committed to Postgres", state.rowsProjected);
        counter(sb, "projector_duplicate_records_total",
                "Journal records at or below the high-water: not inserted, each compared with its stored rows",
                state.duplicateRecords);
        counter(sb, "projector_payload_conflicts_total",
                "Duplicates that differed from their stored rows (a determinism fault) - MUST stay 0",
                state.payloadConflicts);
        counter(sb, "projector_pruned_records_total",
                "Duplicates with no stored row because age-based retention deleted it (at or below its mark): accepted",
                state.prunedRecords);
        counter(sb, "projector_source_switches_total",
                "Gaps on one source answered by reading the next source before parking",
                state.sourceSwitches);
        counter(sb, "projector_version_refusals_total",
                "Records of a money journal version this projector does not read - MUST stay 0",
                state.versionRefusals);
        counter(sb, "projector_gaps_detected_total",
                "Dense journalSeq gaps detected (a lost money movement) - MUST stay 0; alert on any increase",
                state.gapsDetected);
        gauge(sb, "projector_gap_pending",
                "1 while a gap one source showed is neither filled by another source nor parked on, else 0; "
                        + "it parks after PROJECTOR_GAP_UNFILLED_PARK_MS",
                state.gapPending ? 1 : 0);
        gauge(sb, "projector_gap_high_water",
                "Postgres high-water right below the pending gap (meaningful while projector_gap_pending is 1)",
                state.gapPendingHighWater);
        counter(sb, "projector_source_stalls_total",
                "Journal-source stalls/losses detected and recovered by epoch restart - investigate if climbing",
                state.sourceStalls);
        counter(sb, "projector_pg_flush_failures_total",
                "Postgres flush failures (batch rolled back and retried, never skipped) - investigate if climbing",
                state.pgFlushFailures);
        gauge(sb, "projector_source_backlog_bytes",
                "Bytes recorded in the followed journal recording not yet delivered to the projector; "
                        + "sustained >0 while rows_projected is flat = the projector is behind",
                state.sourceBacklogBytes);
        gauge(sb, "projector_last_journal_seq",
                "journalSeq of the last record durably committed to Postgres", state.lastJournalSeq);
        gauge(sb, "projector_epoch_high_water",
                "Postgres high-water (MAX(journal_seq)) as of the start of the current epoch",
                state.epochHighWater);
        return sb.toString();
    }

    private static void gauge(final StringBuilder sb, final String name, final String help, final long value) {
        series(sb, name, "gauge", help, value);
    }

    private static void counter(final StringBuilder sb, final String name, final String help, final long value) {
        series(sb, name, "counter", help, value);
    }

    private static void series(final StringBuilder sb, final String name, final String type, final String help,
                               final long value) {
        sb.append("# HELP ").append(name).append(' ').append(help).append('\n');
        sb.append("# TYPE ").append(name).append(' ').append(type).append('\n');
        sb.append(name).append(' ').append(value).append('\n');
    }
}
