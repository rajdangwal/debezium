/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection.pgoutput;

import java.time.Instant;

import io.debezium.connector.postgresql.connection.Lsn;

public class PgOutputTruncateReplicationMessage extends PgOutputReplicationMessage {

    private final boolean lastTableInTruncate;

    public PgOutputTruncateReplicationMessage(Operation op, String table, Instant commitTimestamp, long transactionId,
                                              boolean lastTableInTruncate, Lsn finalLsn) {
        super(op, table, commitTimestamp, transactionId, null, null, finalLsn);
        this.lastTableInTruncate = lastTableInTruncate;
    }

    @Override
    public boolean isLastEventForLsn() {
        return lastTableInTruncate;
    }

}
