/*
 * Copyright (C) 2004 - 2016 TopCoder Inc., All Rights Reserved.
 * This package loads data from the Postgres Database to Redshift
 * Currently loading Connect Projects, Connect Members for now, can load more in future 
 */
package com.topcoder.utilities.dwload.tcspostgresredshift;

import com.topcoder.shared.util.DBMS;
import com.topcoder.shared.util.logging.Logger;
import com.topcoder.utilities.dwload.TCLoadTCSRedshift;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;


public class TCLoadConnectProjects extends TCLoadTCSRedshift {

    private static Logger log = Logger.getLogger(TCLoadConnectProjects.class);

    @Override
    public void performLoad() throws Exception {
        doLoadProjects();
        doLoadProjectMembers();
    }

    public void doLoadProjects() throws Exception {
        log.info("load connect projects");
        PreparedStatement select = null;
        PreparedStatement insert = null;
        PreparedStatement update = null;
        ResultSet rs = null;

        final String SELECT = "select id, \"directProjectId\", \"billingAccountId\", name, external "
                                + " ,description, bookmarks, utm, \"estimatedPrice\", \"actualPrice\",terms,\"type\" " 
                                + " ,status, details, \"challengeEligibility\", \"cancelReason\" "
                                + " ,\"deletedAt\", \"createdAt\", \"updatedAt\", \"createdBy\", \"updatedBy\" "
                                + " from projects order by id";

        final String UPDATE = "update projects set \"directProjectId\" = ?,  \"billingAccountId\" = ?"
                                + ", name = ?, external = ? , description = ? , bookmarks = ?, utm = ? "
                                + ", estimatedPrice = ? , actualPrice = ?,terms = ?, \"type\"= ?, status = ?"
                                + ", details = ?, \"challengeEligibility\" = ?, \"cancelReason\" = ? "
                                + ", \"deletedAt\" = ?, \"createdAt\" = ?, \"updatedAt\" = ?, \"createdBy\" = ? "
                                + ", \"updatedBy\" = ?"
                                + " where id = ?";
                   
        final String INSERT = "insert into projects (id, \"directProjectId\", \"billingAccountId\""
                                + " ,name, external "
                                + " ,description, bookmarks, utm, \"estimatedPrice\", \"actualPrice\" " 
                                + " ,terms, type "
                                + " ,status, details, \"challengeEligibility\", \"cancelReason\" "
                                + " ,\"deletedAt\", \"createdAt\", \"updatedAt\", \"createdBy\", \"updatedBy\" )" +
                    "values (?, ?, ?, ?, ? , ? , ? , ?, ?, ?, ? , ? , ?, ? , ?, ? , ?, ? , ?, ?, ?) ";

        try {
            long start = System.currentTimeMillis();
            select = prepareStatement(SELECT, SOURCE_DB);
            update = prepareStatement(UPDATE, TARGET_DB);
            insert = prepareStatement(INSERT, TARGET_DB);
            rs = select.executeQuery();

            int count = 0;
            while (rs.next()) {
                count++;
                log.debug("PROCESSING Projects " + rs.getLong("id"));

                //update record, if 0 rows affected, insert record
                update.clearParameters();
                update.setLong(1, rs.getLong("directProjectId"));
                update.setLong(2, rs.getLong("billingAccountId"));
                update.setString(3, rs.getString("name"));
                update.setString(4, rs.getString("external"));
                update.setString(5, rs.getString("description"));
                update.setString(6, rs.getString("bookmarks"));
                update.setString(7, rs.getString("utm")); 
                update.setDouble(8, rs.getDouble("estimatedPrice"));
                update.setDouble(9, rs.getDouble("actualPrice")); 
                update.setString(10, rs.getString("terms"));
                update.setString(11, rs.getString("type"));
                update.setString(12, rs.getString("status"));
                update.setString(13, rs.getString("details"));
                update.setString(14, rs.getString("challengeEligibility"));
                update.setString(15, rs.getString("cancelReason"));
                update.setTimestamp(16, rs.getTimestamp("deletedAt"));
                update.setTimestamp(17, rs.getTimestamp("createdAt"));
                update.setTimestamp(18, rs.getTimestamp("updatedAt"));
                update.setLong(19, rs.getLong("createdBy")); 
                update.setLong(20, rs.getLong("updatedBy")); 
                update.setLong(21, rs.getLong("id"));     
               
                int retVal = update.executeUpdate();

                if (retVal == 0) {
                    //need to insert
                    insert.clearParameters();
                    insert.setLong(1, rs.getLong("id"));
                    insert.setLong(2, rs.getLong("directProjectId"));
                    insert.setLong(3, rs.getLong("billingAccountId"));
                    insert.setString(4, rs.getString("name"));
                    insert.setString(5, rs.getString("external"));
                    insert.setString(6, rs.getString("description"));
                    insert.setString(7, rs.getString("bookmarks"));
                    insert.setString(8, rs.getString("utm"));
                    insert.setDouble(9, rs.getDouble("estimatedPrice"));
                    insert.setDouble(10, rs.getDouble("actualPrice"));
                    insert.setString(11, rs.getString("terms"));
                    insert.setString(12, rs.getString("type"));
                    insert.setString(13, rs.getString("status"));
                    insert.setString(14, rs.getString("details"));
                    insert.setString(15, rs.getString("challengeEligibility"));
                    insert.setString(16, rs.getString("cancelReason"));
                    insert.setTimestamp(17, rs.getTimestamp("deletedAt"));
                    insert.setTimestamp(18, rs.getTimestamp("createdAt"));
                    insert.setTimestamp(19, rs.getTimestamp("updatedAt"));
                    insert.setLong(20, rs.getLong("createdBy"));
                    insert.setLong(21, rs.getLong("updatedBy"));

                    insert.executeUpdate();
                }

            }
            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");


        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of 'projects' table failed.\n" +
                    sqle.getMessage());
        } finally {
            close(rs);
            close(select);
            close(insert);
            close(update);
        }
    }

    public void doLoadProjectMembers() throws Exception {
        log.info("load connect project Members");
        PreparedStatement select = null;
        PreparedStatement insert = null;
        PreparedStatement update = null;
        ResultSet rs = null;

        final String SELECT = "select \"userId\", \"role\", \"isPrimary\" "
                                + ", \"deletedAt\",\"createdAt\",\"updatedAt\",\"createdBy\" "
                                + ", \"updatedBy\", \"id\" "
                                + " from project_members order by id";
        final String UPDATE = "update project_members set " 
                                + " \"userId\" = ?"
                                + ", \"role\" = ?"
                                + ", \"isPrimary\" = ?"
                                + ", \"deletedAt\" = ?"
                                + ", \"createdAt\" = ?"
                                + ", \"updatedAt\" = ?"
                                + ", \"createdBy\" = ?"
                                + ", \"updatedBy\" = ?"
                                + " where id = ?";
                   
        final String INSERT = "insert into project_members (" 
                            +  "  \"userId\""
                            +  ", \"role\" "
                            +  ", \"isPrimary\" "
                            +  ", \"deletedAt\" "
                            +  ", \"createdAt\" "
                            +  ", \"updatedAt\" "
                            +  ", \"createdBy\" "
                            +  ", \"updatedBy\""
                            +  ", \"id\""
                            +  " ) "
                            + " values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try {
            long start = System.currentTimeMillis();
            select = prepareStatement(SELECT, SOURCE_DB);
            update = prepareStatement(UPDATE, TARGET_DB);
            insert = prepareStatement(INSERT, TARGET_DB);
            rs = select.executeQuery();

            int count = 0;
            while (rs.next()) {
                count++;
                log.debug("PROCESSING Project Member " + rs.getLong("id"));

                //update record, if 0 rows affected, insert record
                update.clearParameters();
                update.setLong(1, rs.getLong("userId"));
                update.setString(2, rs.getString("role"));
                update.setBoolean(3, rs.getBoolean("isPrimary"));
                update.setTimestamp(4, rs.getTimestamp("deletedAt"));
                update.setTimestamp(5, rs.getTimestamp("createdAt"));
                update.setTimestamp(6, rs.getTimestamp("updatedAt"));
                update.setLong(7, rs.getLong("createdBy")); 
                update.setLong(8, rs.getLong("updatedBy")); 

                update.setLong(9, rs.getLong("id"));     
               
                int retVal = update.executeUpdate();

                if (retVal == 0) {
                    //need to insert
                    insert.clearParameters();
                    insert.setLong(1, rs.getLong("userId"));
                    insert.setString(2, rs.getString("role"));
                    insert.setBoolean(3, rs.getBoolean("isPrimary"));
                    insert.setTimestamp(4, rs.getTimestamp("deletedAt"));
                    insert.setTimestamp(5, rs.getTimestamp("createdAt"));
                    insert.setTimestamp(6, rs.getTimestamp("updatedAt"));
                    insert.setLong(7, rs.getLong("createdBy"));
                    insert.setLong(8, rs.getLong("updatedBy"));
                    insert.setLong(9, rs.getLong("id"));

                    insert.executeUpdate();
                }

            }
            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");


        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of 'project Members' table failed.\n" +
                    sqle.getMessage());
        } finally {
            close(rs);
            close(select);
            close(insert);
            close(update);
        }
    }
      
}