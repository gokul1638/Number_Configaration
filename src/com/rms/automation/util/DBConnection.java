package com.rms.automation.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnection {

    static {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver"); // Correct driver class for MySQL 8+
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("MySQL JDBC Driver not found", e);
        }
    }

    // RMS DB details
    private static final String RMS_URL = "jdbc:mysql://localhost:3306/rmsdb";
    private static final String RMS_USER = "root";
    private static final String RMS_PASSWORD = "kishore@123";

    // CIM DB details
    private static final String CIM_URL = "jdbc:mysql://localhost:3306/cim_admin_prodtest";
    private static final String CIM_USER = "root";
    private static final String CIM_PASSWORD = "kishore@123";

    /** Get connection to RMS database */
    public static Connection getRMSConnection() throws SQLException {
        return DriverManager.getConnection(RMS_URL, RMS_USER, RMS_PASSWORD);
    }

    /** Get connection to CIM database */
    public static Connection getCIMConnection() throws SQLException {
        return DriverManager.getConnection(CIM_URL, CIM_USER, CIM_PASSWORD);
    }
}