package nl.b3p.brmo.verschil.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;

public class ResultSetSerializer extends JsonSerializer<ResultSet> {
    private static final Log LOG = LogFactory.getLog(ResultSetSerializer.class);
    private long count = -1;

    /**
     * Geeft, na serialisatie, het aantal verwerkte records.
     *
     * @return aatntal verwerkte records, {@code -1} in geval van een fout.
     */
    public long getCount() {
        return this.count;
    }

    @Override
    public Class<ResultSet> handledType() {
        return ResultSet.class;
    }

    @Override
    public void serialize(ResultSet resultSet, JsonGenerator gen, SerializerProvider serializers) throws ResultSetSerializerException {
        long counted = 0;
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int numCols = metaData.getColumnCount();
            String[] colNames = new String[numCols];
            int[] colTypes = new int[numCols];
            for (int i = 0; i < numCols; i++) {
                colNames[i] = metaData.getColumnName(i + 1);
                colTypes[i] = metaData.getColumnType(i + 1);
            }
            gen.writeStartArray();

            while (resultSet.next()) {
                boolean b;
                long l;
                double d;
                BigDecimal bd;
                gen.writeStartObject();

                for (int i = 0; i < colNames.length; i++) {
                    LOG.trace("veld naar json: " + colNames[i] + " (" + colTypes[i] + "): " + resultSet.getObject(i + 1));
                    gen.writeFieldName(colNames[i]);

                    switch (colTypes[i]) {
                        case Types.INTEGER:
                        case Types.BIGINT:
                            l = resultSet.getLong(i + 1);
                            if (resultSet.wasNull()) {
                                gen.writeNull();
                            } else {
                                gen.writeNumber(l);
                            }
                            break;
                        case Types.SMALLINT:
                        case Types.TINYINT:
                            l = resultSet.getShort(i + 1);
                            if (resultSet.wasNull()) {
                                gen.writeNull();
                            } else {
                                gen.writeNumber(l);
                            }
                            break;
                        case Types.DECIMAL:
                        case Types.NUMERIC:
                            bd = resultSet.getBigDecimal(i + 1);
                            if (resultSet.wasNull()) {
                                gen.writeNull();
                            } else {
                                gen.writeNumber(bd);
                            }
                            break;
                        case Types.FLOAT:
                        case Types.DOUBLE:
                        case Types.REAL:
                            d = resultSet.getDouble(i + 1);
                            if (resultSet.wasNull()) {
                                gen.writeNull();
                            } else {
                                gen.writeNumber(d);
                            }
                            break;
                        case Types.NVARCHAR:
                        case Types.VARCHAR:
                        case Types.LONGNVARCHAR:
                        case Types.LONGVARCHAR:
                            gen.writeString(resultSet.getString(i + 1));
                            break;
                        case Types.BOOLEAN:
                        case Types.BIT:
                            b = resultSet.getBoolean(i + 1);
                            if (resultSet.wasNull()) {
                                gen.writeNull();
                            } else {
                                gen.writeBoolean(b);
                            }
                            break;
                        case Types.DATE:
                            serializers.defaultSerializeDateValue(resultSet.getDate(i + 1), gen);
                            break;
                        case Types.TIMESTAMP:
                            serializers.defaultSerializeDateValue(resultSet.getTime(i + 1), gen);
                            break;
                        case Types.BINARY:
                        case Types.VARBINARY:
                        case Types.LONGVARBINARY:
                            gen.writeBinary(resultSet.getBytes(i + 1));
                            break;
                        case Types.BLOB:
                            Blob blob = resultSet.getBlob(i + 1);
                            serializers.defaultSerializeValue(blob.getBinaryStream(), gen);
                            blob.free();
                            break;
                        case Types.CLOB:
                            Clob clob = resultSet.getClob(i);
                            serializers.defaultSerializeValue(clob.getCharacterStream(), gen);
                            clob.free();
                            break;
                        // TODO er missen nog wat types
                        case Types.ARRAY:
                        case Types.STRUCT:
                        case Types.DISTINCT:
                        case Types.REF:
                            throw new NotImplementedException("ResultSetSerializer (nog) niet geimplementeerd voor SQL type: " + colTypes[i]);
                        case Types.JAVA_OBJECT:
                        default:
                            serializers.defaultSerializeValue(resultSet.getObject(i + 1), gen);
                    }
                }
                counted++;
                gen.writeEndObject();
            }
            gen.writeEndArray();
            this.count = counted;
        } catch (SQLException | IOException | RuntimeException e) {
            LOG.error(e);
            throw new ResultSetSerializerException(e);
        }
    }
}