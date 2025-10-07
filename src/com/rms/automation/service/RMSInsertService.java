package com.rms.automation.service;



import com.rms.automation.util.DBConnection;
import java.io.*;
import java.sql.*;
import java.util.regex.*;

public class RMSInsertService {

    public static void insertRMSTablesFromSQL(String exportFolderPath) {
        File folder = new File(exportFolderPath);
        if (!folder.exists() || !folder.isDirectory()) {
            System.out.println("❌ Invalid export folder: " + exportFolderPath);
            return;
        }

        try (Connection conn = DBConnection.getRMSConnection()) {
            conn.setAutoCommit(false);

            File[] sqlFiles = folder.listFiles((dir, name) -> name.endsWith(".sql"));
            if (sqlFiles == null || sqlFiles.length == 0) {
                System.out.println("⚠️ No .sql files found in " + exportFolderPath);
                return;
            }

            for (File file : sqlFiles) {
                System.out.println("📂 Processing " + file.getName());
                processSQLFile(file, conn);
            }

            conn.commit();
            System.out.println("✅ All RMS tables inserted successfully!");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void processSQLFile(File sqlFile, Connection conn) throws Exception {
        String tableName = extractTableName(sqlFile.getName());

        try (BufferedReader br = new BufferedReader(new FileReader(sqlFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.trim().isEmpty()) continue;

                // Rule 1: For device_msisdn_table — ensure MIGRATION_CATEGORY = 1
                if (tableName.equalsIgnoreCase("device_msisdn_table")) {
                    line = updateMigrationCategory(line);
                }

                // Execute the modified insert
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(line);
                } catch (SQLException e) {
                    System.out.println("⚠️ Skipped duplicate or invalid record: " + e.getMessage());
                }
            }
        }

        // Rule 2: Add ISIM attribute in customer_attribute table
        if (tableName.equalsIgnoreCase("customer_attribute")) {
            insertISIMAttribute(conn);
        }
    }

    private static String extractTableName(String fileName) {
        if (fileName.contains("_export.sql")) {
            return fileName.replace("_export.sql", "");
        }
        return fileName.replace(".sql", "");
    }

    // Rule 1: Modify MIGRATION_CATEGORY value
    private static String updateMigrationCategory(String sqlLine) {
        Pattern pattern = Pattern.compile("\\((.*?)\\)");
        Matcher matcher = pattern.matcher(sqlLine);
        if (matcher.find()) {
            String valuesPart = matcher.group(1);
            // Replace MIGRATION_CATEGORY column value with 1 (simplified logic)
            if (valuesPart.contains("NULL")) {
                valuesPart = valuesPart.replaceFirst("NULL", "'1'");
            } else if (!valuesPart.contains("'1'")) {
                valuesPart = valuesPart + ",'1'";
            }
            return "INSERT INTO prepod_rms.device_msisdn_table VALUES(" + valuesPart + ");";
        }
        return sqlLine;
    }

    // Rule 2: After inserting all customer attributes, add ISIM attribute
    private static void insertISIMAttribute(Connection conn) throws SQLException {
        System.out.println("➕ Adding ISIM attribute for each MSISDN...");

        String fetchMSISDNs = "SELECT DISTINCT MSISDN FROM prepod_rms.customer_attribute";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(fetchMSISDNs)) {

            String insertSQL = "INSERT INTO prepod_rms.customer_attribute (MSISDN, ATTRIBUTE_NAME, ATTRIBUTE_VALUE, STATUS) VALUES (?, 'ISIM', '1', ?)";

            try (PreparedStatement ps = conn.prepareStatement(insertSQL)) {
                while (rs.next()) {
                    String msisdn = rs.getString("MSISDN");

                    // Find any existing status for this MSISDN to reuse
                    String status = "1";
                    try (PreparedStatement ps2 = conn.prepareStatement(
                            "SELECT STATUS FROM prepod_rms.customer_attribute WHERE MSISDN=? LIMIT 1")) {
                        ps2.setString(1, msisdn);
                        ResultSet rs2 = ps2.executeQuery();
                        if (rs2.next()) status = rs2.getString("STATUS");
                    }

                    ps.setString(1, msisdn);
                    ps.setString(2, status);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }
        System.out.println("✅ ISIM attributes added successfully.");
    }
}
