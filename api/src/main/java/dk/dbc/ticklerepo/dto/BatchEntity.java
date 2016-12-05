/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU 3
 * See license text in LICENSE.txt
 */

package dk.dbc.ticklerepo.dto;

import javax.persistence.Column;
import javax.persistence.Convert;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Timestamp;

@Entity
@Table(name = "batch")
public class BatchEntity {

    public enum Type {
        ACTIVE,
        DELETED,
        RESET
    }

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(updatable = false)
    private int id;

    @Column(nullable = false)
    private int dataset;

    @Column(nullable = false)
    private int batchKey;

    @Convert(converter = BatchEntityTypeConverter.class)
    private Type type;

    @Column(insertable = false, updatable = false)
    private Timestamp timeOfCreation;

    private Timestamp timeOfCompletion;

    public int getId() {
        return id;
    }

    public BatchEntity withId(int id) {
        this.id = id;
        return this;
    }

    public int getDataset() {
        return dataset;
    }

    public BatchEntity withDataset(int dataset) {
        this.dataset = dataset;
        return this;
    }

    public int getBatchKey() {
        return batchKey;
    }

    public BatchEntity withBatchKey(int batchKey) {
        this.batchKey = batchKey;
        return this;
    }

    public Type getType() {
        return type;
    }

    public BatchEntity withType(Type type) {
        this.type = type;
        return this;
    }

    public Timestamp getTimeOfCreation() {
        return timeOfCreation;
    }

    public BatchEntity withTimeOfCreation(Timestamp timeOfCreation) {
        this.timeOfCreation = timeOfCreation;
        return this;
    }

    public Timestamp getTimeOfCompletion() {
        return timeOfCompletion;
    }

    public BatchEntity withTimeOfCompletion(Timestamp timeOfCompletion) {
        this.timeOfCompletion = timeOfCompletion;
        return this;
    }
}
