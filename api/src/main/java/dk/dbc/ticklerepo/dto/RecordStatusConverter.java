/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPLv3
 * See license text in LICENSE.txt
 */

package dk.dbc.ticklerepo.dto;

import org.postgresql.util.PGobject;

import javax.persistence.AttributeConverter;
import javax.persistence.Converter;
import java.sql.SQLException;

@Converter
public class RecordStatusConverter implements AttributeConverter<Record.Status, Object> {
    @Override
    public Object convertToDatabaseColumn(Record.Status status) {
        String statusValue = null;
        if (status != null) {
            statusValue = status.name();
        }

        final PGobject pgObject = new PGobject();
        pgObject.setType("record_status");
        try {
            pgObject.setValue(statusValue);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        return pgObject;
    }

    @Override
    public Record.Status convertToEntityAttribute(Object dbValue) {
        if (dbValue == null) {
            throw new IllegalArgumentException("dbValue can not be null");
        }
        switch ((String) dbValue) {
            case "ACTIVE":
                return Record.Status.ACTIVE;
            case "DELETED":
                return Record.Status.DELETED;
            case "RESET":
                return Record.Status.RESET;
            default:
                return null;
        }
    }
}
