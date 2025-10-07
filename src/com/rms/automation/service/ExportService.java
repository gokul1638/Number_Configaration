package com.rms.automation.service;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.rms.automation.util.DBConnection;

public class ExportService {

	
	
	  private static final String[] CIM_TABLES = {
	            "cim_acnt_subscrptn_dtl",
	            "customer_attribute"
	    };
	
	
	 // ---------------- RMS TABLE EXPORT ----------------
    public static void exportRMSTable(String tableName, String[] msisdns, String rmsStatus, String cimStatus) {
        String fileName = tableName + "_export.sql";

        try (Connection rmsConn = DBConnection.getRMSConnection();
             Connection cimConn = DBConnection.getCIMConnection();
             FileWriter writer = new FileWriter(fileName)) {

            String query = buildRMSQuery(tableName, msisdns, rmsStatus, cimStatus, rmsConn, cimConn);

            if (query == null) {
                System.out.println("⚠️ Skipping " + tableName);
                return;
            }

            try (Statement stmt = rmsConn.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {

                writeResultSetToFile(rs, writer, tableName);
                System.out.println("✅ Exported table " + tableName + " to " + fileName);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String buildRMSQuery(String tableName, String[] msisdns, String rmsStatus, String cimStatus,
                                        Connection rmsConn, Connection cimConn) throws SQLException {
        StringBuilder sb = new StringBuilder();

        switch (tableName) {
            case "PAIRED_INFO_TABLE":
                sb.append("SELECT * FROM PAIRED_INFO_TABLE WHERE PAIRED_DEVICE_ID IN (")
                        .append(joinMsisdns(msisdns)).append(") AND status = ").append(rmsStatus);
                break;

            case "customer_attribute":
                sb.append("SELECT * FROM customer_attribute WHERE MSISDN IN (")
                        .append(joinMsisdns(msisdns)).append(") AND status = ").append(rmsStatus);
                break;

            case "device_msisdn_table":
                List<String> deviceIds = new ArrayList<>();
                try (PreparedStatement ps = rmsConn.prepareStatement(
                        "SELECT paired_device_id FROM PAIRED_INFO_TABLE " +
                                "WHERE paired_device_id IN (" + joinMsisdns(msisdns) + ") AND status = ?")) {
                    ps.setString(1, rmsStatus);
                    ResultSet rs = ps.executeQuery();
                    while (rs.next()) deviceIds.add(rs.getString("paired_device_id"));
                }
                if (deviceIds.isEmpty()) return null;
                sb.append("SELECT * FROM device_msisdn_table WHERE device_id IN (")
                        .append(joinStrings(deviceIds)).append(")");
                break;

            case "device_sim_table":
                List<String> simNos = new ArrayList<>();
                try (PreparedStatement ps = cimConn.prepareStatement(
                        "SELECT cim_sim_no FROM cim_admin_prodtest.cim_sgmnt_catalog " +
                                "WHERE cim_subs_msisdn = ? AND cim_account_status = ?")) {
                    for (String msisdn : msisdns) {
                        ps.setString(1, msisdn);
                        ps.setString(2, cimStatus);
                        ResultSet rs = ps.executeQuery();
                        while (rs.next()) simNos.add(rs.getString("cim_sim_no"));
                    }
                }
                if (simNos.isEmpty()) return null;
                sb.append("SELECT * FROM device_sim_table WHERE device_id IN (")
                        .append(joinStrings(simNos)).append(")");
                break;
        }
        return sb.toString();
    }

    // ---------------- NEW CIM TABLE EXPORT ----------------
    public static void exportCIMTables(String[] msisdns, String cimStatus) {
        try (Connection cimConn = DBConnection.getCIMConnection()) {

            // Step 1: Find schema for each MSISDN from master table
            Map<String, String> msisdnToSchema = new HashMap<>();
            try (PreparedStatement ps = cimConn.prepareStatement(
                    "SELECT cim_subs_msisdn, cim_segment_ref_id " +
                            "FROM cim_admin_prodtest.cim_sgmnt_catalog " +
                            "WHERE cim_subs_msisdn IN (" + joinMsisdns(msisdns) + ") AND cim_account_status = ?")) {
                ps.setString(1, cimStatus);
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    String msisdn = rs.getString("cim_subs_msisdn");
                    int refId = rs.getInt("cim_segment_ref_id");
                    String schema = mapRefIdToSchema(refId);
                    if (schema != null) msisdnToSchema.put(msisdn, schema);
                }
            }

            // Step 2: Fetch from correct schema tables
            for (Map.Entry<String, String> entry : msisdnToSchema.entrySet()) {
                String msisdn = entry.getKey();
                String schema = entry.getValue();

                for (String table : CIM_TABLES) {
                    String fileName = schema + "_" + table + "_export.sql";
                    String query = "SELECT * FROM " + schema + "." + table +
                            " WHERE msisdn = '" + msisdn + "' AND status = '" + cimStatus + "'";

                    try (Statement stmt = cimConn.createStatement();
                         ResultSet rs = stmt.executeQuery(query);
                         FileWriter writer = new FileWriter(fileName, true)) {

                        writeResultSetToFile(rs, writer, schema + "." + table);
                        System.out.println("✅ Exported from " + schema + "." + table + " to " + fileName);
                    } catch (SQLException sqlEx) {
                        System.out.println("⚠️ Query failed for " + schema + "." + table + ": " + sqlEx.getMessage());
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ---------------- EXPORT CATALOG MASTER TABLE ----------------
    public static void exportCIMCatalog(String[] msisdns, String cimStatus) {
        String fileName = "cim_admin_prodtest_cim_sgmnt_catalog_export.sql";

        try (Connection cimConn = DBConnection.getCIMConnection();
             FileWriter writer = new FileWriter(fileName)) {

            String query = "SELECT * FROM cim_admin_prodtest.cim_sgmnt_catalog " +
                           "WHERE cim_subs_msisdn IN (" + joinMsisdns(msisdns) + 
                           ") AND cim_account_status = '" + cimStatus + "'";

            try (Statement stmt = cimConn.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {

                writeResultSetToFile(rs, writer, "cim_admin_prodtest.cim_sgmnt_catalog");
                System.out.println("✅ Exported CIM catalog data to " + fileName);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ---------------- HELPER METHODS ----------------
    
    private static String mapRefIdToSchema(int refId) {
        switch (refId) {
            case 125: return "cimtest_user1";
            case 126: return "cimtest_user2";
            case 127: return "cimtest_user3";
            case 128: return "cimtest_user4";
            default: return null;
        }
    }

    private static void writeResultSetToFile(ResultSet rs, FileWriter writer, String tableName)
            throws SQLException, IOException {
        ResultSetMetaData meta = rs.getMetaData();
        int colCount = meta.getColumnCount();

        while (rs.next()) {
            StringBuilder sql = new StringBuilder("INSERT INTO " + tableName + " VALUES(");
            for (int i = 1; i <= colCount; i++) {
                Object val = rs.getObject(i);
                if (val == null) sql.append("NULL");
                else if (val instanceof String || val instanceof Timestamp)
                    sql.append("'").append(val.toString().replace("'", "''")).append("'");
                else sql.append(val);
                if (i < colCount) sql.append(", ");
            }
            sql.append(");\n");
            writer.write(sql.toString());
        }
    }

    private static String joinMsisdns(String[] msisdns) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < msisdns.length; i++) {
            sb.append("'").append(msisdns[i]).append("'");
            if (i < msisdns.length - 1) sb.append(", ");
        }
        return sb.toString();
    }

    private static String joinStrings(List<String> list) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < list.size(); i++) {
            sb.append("'").append(list.get(i)).append("'");
            if (i < list.size() - 1) sb.append(", ");
        }
        return sb.toString();
    }
}

	
	
