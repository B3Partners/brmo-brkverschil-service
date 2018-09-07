package nl.b3p.brmo.verschil.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import static net.javacrumbs.jsonunit.JsonAssert.assertJsonEquals;

public class ResultSetJSONSerializerTest {

    @Test
    public void record() throws SQLException, JsonProcessingException {
        ResultSetMetaData metaData = Mockito.mock(ResultSetMetaData.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);

        Mockito.when(metaData.getColumnCount()).thenReturn(1);
        Mockito.when(metaData.getColumnName(1)).thenReturn("test");
        Mockito.when(metaData.getColumnType(1)).thenReturn(Types.INTEGER);
        Mockito.when(resultSet.getLong(1)).thenReturn(1L);
        // in geval van trace logging is er ook nog een getObject call
        Mockito.when(resultSet.getObject(1)).thenReturn(1);

        Mockito.when(resultSet.getMetaData()).thenReturn(metaData);
        Mockito.when(resultSet.next()).thenReturn(true).thenReturn(false);

        SimpleModule module = new SimpleModule();
        ResultSetJSONSerializer serializer = new ResultSetJSONSerializer();
        ObjectMapper mapper = new ObjectMapper();
        module.addSerializer(serializer);
        mapper.registerModule(module);
        ObjectNode objectNode = mapper.createObjectNode();
        objectNode.putPOJO("result", resultSet);

        String result = mapper.writeValueAsString(objectNode);

        assertEquals(1, serializer.getCount());
        assertJsonEquals("{\"result\":[{\"test\":1}]}", result);
    }

    @Test
    public void recordTwoCols() throws SQLException, JsonProcessingException {
        ResultSetMetaData metaData = Mockito.mock(ResultSetMetaData.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(resultSet.getMetaData()).thenReturn(metaData);
        // 2 kolommen
        Mockito.when(metaData.getColumnCount()).thenReturn(2);
        // kolom 1
        Mockito.when(metaData.getColumnName(1)).thenReturn("kolom1");
        Mockito.when(metaData.getColumnType(1)).thenReturn(Types.INTEGER);
        Mockito.when(resultSet.getLong(1)).thenReturn(1L);
        // kolom 2
        Mockito.when(metaData.getColumnName(2)).thenReturn("kolom2");
        Mockito.when(metaData.getColumnType(2)).thenReturn(Types.VARCHAR);
        Mockito.when(resultSet.getString(2)).thenReturn("waarde 2");
        // 1 rij
        Mockito.when(resultSet.next()).thenReturn(true).thenReturn(false);

        SimpleModule module = new SimpleModule();
        ResultSetJSONSerializer serializer = new ResultSetJSONSerializer();
        ObjectMapper mapper = new ObjectMapper();
        module.addSerializer(serializer);
        mapper.registerModule(module);
        ObjectNode objectNode = mapper.createObjectNode();
        objectNode.putPOJO("json", resultSet);

        String result = mapper.writeValueAsString(objectNode);

        assertEquals(1, serializer.getCount());
        assertJsonEquals("{\"json\":[{\"kolom1\":1,\"kolom2\":\"waarde 2\"}]}", result);
    }

    @Test
    public void recordTwoColsTwoRows() throws SQLException, JsonProcessingException {
        ResultSetMetaData metaData = Mockito.mock(ResultSetMetaData.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(resultSet.getMetaData()).thenReturn(metaData);
        // 2 kolommen
        Mockito.when(metaData.getColumnCount()).thenReturn(2);
        // kolom 1
        Mockito.when(metaData.getColumnName(1)).thenReturn("kolom1");
        Mockito.when(metaData.getColumnType(1)).thenReturn(Types.INTEGER);
        Mockito.when(resultSet.getLong(1)).thenReturn(1L);
        // kolom 2
        Mockito.when(metaData.getColumnName(2)).thenReturn("kolom2");
        Mockito.when(metaData.getColumnType(2)).thenReturn(Types.VARCHAR);
        Mockito.when(resultSet.getString(2)).thenReturn("waarde 2");
        // 2x .thenReturn(true) voor emulatie van 2 rijen met dezelfde inhoud
        Mockito.when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);

        SimpleModule module = new SimpleModule();
        ResultSetJSONSerializer serializer = new ResultSetJSONSerializer();
        ObjectMapper mapper = new ObjectMapper();
        module.addSerializer(serializer);
        mapper.registerModule(module);
        ObjectNode objectNode = mapper.createObjectNode();
        objectNode.putPOJO("json", resultSet);

        String result = mapper.writeValueAsString(objectNode);

        assertEquals(2, serializer.getCount());
        assertJsonEquals("{\"json\":[{\"kolom1\":1,\"kolom2\":\"waarde 2\"},{\"kolom1\":1,\"kolom2\":\"waarde 2\"}]}", result);
    }
}
