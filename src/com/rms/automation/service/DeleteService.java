package com.rms.automation.service;

import com.rms.automation.util.DBConnection;
import java.sql.*;
import java.util.*;

public class DeleteService {

    // Delete from prepod RMS & CIM schemas
    public static void deletePrepodData(String[] msisdns) {
        System.out.println("\n Starting cleanup in PREPOD for given MSISDNs...\n");
        deleteFromRMS(msisdns);
        deleteFromCIM(msisdns);
        System.out.println(" Cleanup completed successfully!\n");
    }

    // ---------------- RMS DELETE ----------------
    private static void deleteFromRMS(String[] msisdns) {
        String[] rmsTables = {
            "prepod_rms.paired_info_table",
            "prepod_rms.device_sim_table",
            "prepod_rms.customer_attribute",
            "prepod_rms.device_msisdn_table"
        };

        try (Connection conn = DBConnection.getRMSConnection()) {
            for (String table : rmsTables) {
                for (String msisdn : msisdns) {
                    String column = switch (table) {
                        case "prepod_rms.paired_info_table" -> "PAIRED_DEVICE_ID";
                        case "prepod_rms.device_sim_table" -> "device_id";
                        case "prepod_rms.customer_attribute" -> "MSISDN";
                        case "prepod_rms.device_msisdn_table" -> "device_id";
                        default -> null;
                    };

                    if (column == null) continue;
                    String sql = "DELETE FROM " + table + " WHERE " + column + " = ?";
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setString(1, msisdn);
                        int count = ps.executeUpdate();
                        if (count > 0)
                            System.out.println(" Deleted " + count + " rows from " + table + " for " + msisdn);
                    } catch (SQLException e) {
                        System.err.println("Error deleting from " + table + ": " + e.getMessage());
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // ---------------- CIM DELETE PREPODS NOTED  ----------------
    
    private static void deleteFromCIM(String[] msisdns) {
        try (Connection conn = DBConnection.getCIMConnection()) {
            // Step 1: Find cim_sim_no and ref_id from catalog
            Map<String, Integer> msisdnToRefId = new HashMap<>();
            Map<String, String> msisdnToSim = new HashMap<>();

            String catalogQuery = """
                SELECT cim_subs_msisdn, cim_sim_no, cim_segment_ref_id
                FROM prepodCIM_admin_prodtest.cim_sgmnt_catalog
                WHERE cim_subs_msisdn IN (%s)
            """.formatted(joinMsisdns(msisdns));

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(catalogQuery)) {
                while (rs.next()) {
                    msisdnToRefId.put(rs.getString("cim_subs_msisdn"), rs.getInt("cim_segment_ref_id"));
                    msisdnToSim.put(rs.getString("cim_subs_msisdn"), rs.getString("cim_sim_no"));
                }
            }

            // Step 2: Delete from cim_sgmnt_catalog
            
            for (String msisdn : msisdns) {
                String sim = msisdnToSim.get(msisdn);
                String sql = "DELETE FROM prepodCIM_admin_prodtest.cim_sgmnt_catalog WHERE cim_subs_msisdn = ? OR cim_sim_no = ?";
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setString(1, msisdn);
                    ps.setString(2, sim);
                    int count = ps.executeUpdate();
                    if (count > 0)
                        System.out.println(" Deleted " + count + " from cim_sgmnt_catalog for " + msisdn);
                }
            }

            // Step 3: Delete from user schemas
            
            String[] schemas = {"prepod_user1", "prepod_user2", "prepod_user3", "prepod_user4"};
            for (String msisdn : msisdns) {
                int refId = msisdnToRefId.getOrDefault(msisdn, -1);
                List<String> targetSchemas = new ArrayList<>();
                if (refId >= 125 && refId <= 128) {
                    targetSchemas.add(mapRefIdToSchema(refId));
                } else {
                    targetSchemas.addAll(Arrays.asList(schemas));
                }

                for (String schema : targetSchemas) {
                    for (String table : new String[]{"cim_acnt_subscrptn_dtl", "customer_attribute"}) {
                        String sql = " DELETE FROM " + schema + "." + table + " WHERE msisdn = ?";
                        try (PreparedStatement ps = conn.prepareStatement(sql)) {
                            ps.setString(1, msisdn);
                            int count = ps.executeUpdate();
                            if (count > 0)
                                System.out.println(" Deleted " + count + " from " + schema + "." + table + " for " + msisdn);
                        } catch (SQLException e) {
                            System.err.println(" Delete failed for " + schema + "." + table + ": " + e.getMessage());
                        }
                    }
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static String mapRefIdToSchema(int refId) {
        return switch (refId) {
            case 125 -> "prepod_user1";
            case 126 -> "prepod_user2";
            case 127 -> "prepod_user3";
            case 128 -> "prepod_user4";
            default -> null;
        };
    }

    private static String joinMsisdns(String[] msisdns) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < msisdns.length; i++) {
            sb.append("'").append(msisdns[i]).append("'");
            if (i < msisdns.length - 1) sb.append(",");
        }
        return sb.toString();
    }
}

