// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure.journal;

import com.openexchange.core.infrastructure.InfrastructureConstants;
import com.openexchange.core.infrastructure.Logger;
import io.aeron.archive.client.AeronArchive;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;

import java.util.concurrent.TimeUnit;

/**
 * Owns the order journal's moving parts on one member: the SPSC ring ({@link OrderJournal}, written by the service
 * thread) and the writer agent ({@link OrderJournalWriterAgent}, its own thread) that records it on the member's own
 * archive. A copy of {@link MoneyJournalRuntime} with one difference: there is no arming command. The journal's
 * sequence is the engine's eventSeq, which advances on every member whether it records or not, so the journal is on
 * from genesis on every member that runs this jar.
 */
public final class OrderJournalRuntime implements AutoCloseable {

    private static final Logger log = Logger.getLogger(OrderJournalRuntime.class);

    private final int nodeId;
    private final OrderJournalSettings settings;
    private final OrderJournal journal;
    private AgentRunner writerRunner;

    private OrderJournalRuntime(final int nodeId, final OrderJournalSettings settings) {
        this.nodeId = nodeId;
        this.settings = settings;
        this.journal = new OrderJournal(settings);
    }

    public static OrderJournalRuntime create(final int nodeId, final OrderJournalSettings settings) {
        log.info("Order journal machinery ready: node=%d channel=%s stream=%d %s", nodeId, settings.channel(),
                InfrastructureConstants.ORDER_JOURNAL_STREAM_ID, settings);
        return new OrderJournalRuntime(nodeId, settings);
    }

    public OrderJournal journal() {
        return journal;
    }

    public OrderJournalSettings settings() {
        return settings;
    }

    /** Launch the writer thread once the member's driver and archive are up (the agent connects lazily). */
    public OrderJournalRuntime start(final AeronArchive.Context archiveClientContext, final ErrorHandler errorHandler) {
        final OrderJournalWriterAgent agent = new OrderJournalWriterAgent(journal, archiveClientContext::clone,
                InfrastructureConstants.ORDER_JOURNAL_STREAM_ID, settings);
        writerRunner = new AgentRunner(new BackoffIdleStrategy(10, 20, 1_000,
                Math.max(1_000, TimeUnit.MICROSECONDS.toNanos(settings.writerIdleMaxUs()))), errorHandler, null, agent);
        AgentRunner.startOnThread(writerRunner);
        log.info("Order journal writer started (node %d)", nodeId);
        return this;
    }

    @Override
    public void close() {
        CloseHelper.quietClose(writerRunner); // the writer drains the ring first (bounded)
        journal.close(); // a service thread still waiting on a full ring leaves
    }
}
