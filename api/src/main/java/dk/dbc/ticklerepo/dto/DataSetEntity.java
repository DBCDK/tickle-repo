/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU 3
 * See license text in LICENSE.txt
 */

package dk.dbc.ticklerepo.dto;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

@Entity
@Table(name = "dataset")
public class DataSetEntity {

    @Id
    @SequenceGenerator(
            name = "entry_id_seq",
            sequenceName = "entry_id_seq",
            allocationSize = 1)
    @GeneratedValue(
            strategy = GenerationType.SEQUENCE,
            generator = "entry_id_seq")
    @Column(updatable = false)
    private int id;

    @Column(nullable = false)
    private String name;

    private String displayName;

    public int getId() {
        return id;
    }

    public DataSetEntity withId(int id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public DataSetEntity withName(String name) {
        this.name = name;
        return this;
    }

    public String getDisplayName() {
        return displayName;
    }

    public DataSetEntity withDisplayName(String displayName) {
        this.displayName = displayName;
        return this;
    }
}
