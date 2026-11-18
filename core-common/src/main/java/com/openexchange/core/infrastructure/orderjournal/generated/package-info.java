/* Generated SBE (Simple Binary Encoding) message codecs.*/
/**
 * core order journal schema: one record per order event, recorded per member as a replayable Aeron Archive stream (4102). journalSeq equals the core eventSeq (dense, starts at 1). The head (journalSeq at 0, epoch at 8, a log position at 16) mirrors the ME journal head so one frame peek serves both. Bytes 24..174 are bytes 16..166 of the money-schema OrderEvent body (template 102), written by the same codec; enum-typed fields carry the money-schema codes as uint8. Every value comes from the log entry that produced the event, so the record is identical on every member and on every replay.
 */
package com.openexchange.core.infrastructure.orderjournal.generated;
