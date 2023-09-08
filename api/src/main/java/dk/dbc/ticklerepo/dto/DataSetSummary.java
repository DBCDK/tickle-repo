/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPLv3
 * See license text in LICENSE.txt
 */

package dk.dbc.ticklerepo.dto;

import java.sql.Timestamp;
import java.util.Date;

@SuppressWarnings("unused")
public class DataSetSummary {

    private String name;
    private Long sum;
    private Long active;
    private Long deleted;
    private Long reset;
    private Timestamp timeOfLastModification;
    private int batchId;

    public DataSetSummary() {
    }

    public DataSetSummary(String name, Long sum, Long active, Long deleted, Long reset, Timestamp timeOfLastModification, int batchId) {
        this.name = name;
        this.sum = sum;
        this.active = active;
        this.deleted = deleted;
        this.reset = reset;
        this.timeOfLastModification = timeOfLastModification;
        this.batchId = batchId;
    }

    public String getName() {
        return name;
    }

    public DataSetSummary withName(String name) {
        this.name = name;
        return this;
    }

    public Long getSum() {
        return sum;
    }

    public DataSetSummary withSum(Long sum) {
        this.sum = sum;
        return this;
    }

    public Long getActive() {
        return active;
    }

    public DataSetSummary withActive(Long active) {
        this.active = active;
        return this;
    }

    public Long getDeleted() {
        return deleted;
    }

    public DataSetSummary withDeleted(Long deleted) {
        this.deleted = deleted;
        return this;
    }

    public Long getReset() {
        return reset;
    }

    public DataSetSummary withReset(Long reset) {
        this.reset = reset;
        return this;
    }

    public Date getTimeOfLastModification() {
        return timeOfLastModification;
    }

    public DataSetSummary withTimeOfLastModification(Date modified) {
        if (timeOfLastModification != null) {
            this.timeOfLastModification = new Timestamp(timeOfLastModification.getTime());
        }
        return this;
    }

    public int getBatchId() {
        return batchId;
    }

    public DataSetSummary withBatchId(int batchId) {
        this.batchId = batchId;
        return this;
    }

    @Override
    public String toString() {
        return "DataSetSummary{" +
                "name='" + name + '\'' +
                ", sum=" + sum +
                ", active=" + active +
                ", deleted=" + deleted +
                ", reset=" + reset +
                ", timeOfLastModification=" + timeOfLastModification +
                ", batchId=" + batchId +
                '}';
    }
}
