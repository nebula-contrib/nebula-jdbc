package com.vesoft.nebula.jdbc;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

class NebulaResultSetMetaDataTest {

	@Test
	void testGetColumnName() throws SQLException {
		NebulaResultSet resultSet = mock(NebulaResultSet.class);
		List<String> columnNames = Stream.of("col1", "col2").collect(Collectors.toList());
		doReturn(columnNames).when(resultSet).getColumnNames();

		NebulaResultSetMetaData metadata = NebulaResultSetMetaData.getInstance(resultSet);
		assertEquals("col1", metadata.getColumnName(1));
		assertEquals("col2", metadata.getColumnName(2));
		assertThrows(SQLException.class, () -> metadata.getColumnName(3));
	}

	@Test
	void testGetColumnLabel() throws SQLException {
		NebulaResultSet resultSet = mock(NebulaResultSet.class);
		List<String> columnNames = Stream.of("col1", "col2").collect(Collectors.toList());
		doReturn(columnNames).when(resultSet).getColumnNames();

		NebulaResultSetMetaData metadata = NebulaResultSetMetaData.getInstance(resultSet);
		assertEquals("col1", metadata.getColumnName(1));
		assertEquals("col2", metadata.getColumnName(2));
		assertThrows(SQLException.class, () -> metadata.getColumnName(3));
	}

	@Test
	void testGetInstanceDiffResultSets() {
		NebulaResultSet resultSet1 = mock(NebulaResultSet.class);
		NebulaResultSet resultSet2 = mock(NebulaResultSet.class);
		assertNotEquals(resultSet1, resultSet2);

		NebulaResultSetMetaData metaData1 = NebulaResultSetMetaData.getInstance(resultSet1);
		NebulaResultSetMetaData metaData2 = NebulaResultSetMetaData.getInstance(resultSet2);
		assertNotEquals(metaData1, metaData2);
	}

}