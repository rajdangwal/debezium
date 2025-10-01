/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection;

import java.util.Objects;

/**
 * Represents a position in the PostgreSQL Write-Ahead Log (WAL).
 * 
 * The position is defined by two LSNs:
 * - finalLsn: The LSN of the COMMIT for the transaction (from the BEGIN message)
 * - lsn: The LSN of the specific event
 * 
 * This allows for proper ordering of events both within and across transactions.
 * 
 * @author Debezium Authors
 */
public class WalPosition {
    
    private final Lsn finalLsn;
    private final Lsn lsn;
    
    public WalPosition(Lsn finalLsn, Lsn lsn) {
        this.finalLsn = finalLsn;
        this.lsn = Objects.requireNonNull(lsn, "LSN cannot be null");
    }
    
    /**
     * Creates a WalPosition from offset data.
     */
    public static WalPosition fromOffset(Lsn finalLsn, Lsn lsn) {
        return new WalPosition(finalLsn, lsn);
    }
    
    /**
     * Checks if this position is before (smaller than) another position.
     * 
     * The comparison logic:
     * 1. If finalLsn is null for either position, fall back to comparing only lsn
     * 2. If finalLsn differs, use that for comparison
     * 3. If finalLsn is the same, compare the event lsn
     * 
     * @param other The other WalPosition to compare to
     * @return true if this position is before the other position
     */
    public boolean isBefore(WalPosition other) {
        if (other == null) {
            return false;
        }
        
        // If either finalLsn is null, fall back to LSN-only comparison
        if (this.finalLsn == null || other.finalLsn == null) {
            return this.lsn.compareTo(other.lsn) < 0;
        }
        
        // Compare finalLsn first
        int finalLsnComparison = this.finalLsn.compareTo(other.finalLsn);
        if (finalLsnComparison < 0) {
            return true;
        } else if (finalLsnComparison > 0) {
            return false;
        } else {
            // Same final LSN, compare event LSN
            return this.lsn.compareTo(other.lsn) < 0;
        }
    }
    
    /**
     * Checks if this position is before or equal to another position.
     */
    public boolean isBeforeOrEqual(WalPosition other) {
        if (other == null) {
            return false;
        }
        
        // If either finalLsn is null, fall back to LSN-only comparison
        if (this.finalLsn == null || other.finalLsn == null) {
            return this.lsn.compareTo(other.lsn) <= 0;
        }
        
        // Compare finalLsn first
        int finalLsnComparison = this.finalLsn.compareTo(other.finalLsn);
        if (finalLsnComparison < 0) {
            return true;
        } else if (finalLsnComparison > 0) {
            return false;
        } else {
            // Same final LSN, compare event LSN
            return this.lsn.compareTo(other.lsn) <= 0;
        }
    }
    
    public Lsn getFinalLsn() {
        return finalLsn;
    }
    
    public Lsn getLsn() {
        return lsn;
    }
    
    @Override
    public String toString() {
        return "WalPosition{" +
                "finalLsn=" + finalLsn +
                ", lsn=" + lsn +
                '}';
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WalPosition that = (WalPosition) o;
        return Objects.equals(finalLsn, that.finalLsn) && 
               Objects.equals(lsn, that.lsn);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(finalLsn, lsn);
    }
}
