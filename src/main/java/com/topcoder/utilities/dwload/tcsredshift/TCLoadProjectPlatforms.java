/*
 * Copyright (C) 2004 - 2016 TopCoder Inc., All Rights Reserved.
 */
package com.topcoder.utilities.dwload.tcsredshift;

import com.topcoder.shared.util.DBMS;
import com.topcoder.shared.util.logging.Logger;
import com.topcoder.utilities.dwload.TCLoadTCSRedshift;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;


public class TCLoadProjectPlatforms extends TCLoadTCSRedshift {

    private static Logger log = Logger.getLogger(TCLoadProjectPlatforms.class);

    @Override
    public void performLoad() throws Exception {
        doLoadProjectPlatforms();
    }

    /**
     * Load the project platforms data
     *
     * @throws Exception if any error.
     * @since 1.2.3
     */
    public void doLoadProjectPlatforms() throws Exception {
        log.info("load project platforms");

        PreparedStatement firstTimeSelect = null;
        PreparedStatement deletePlatforms = null;
        PreparedStatement selectPlatforms = null;
        PreparedStatement insertPlatforms = null;
        ResultSet rs = null;
        Set<Long> deletedProjects = new HashSet<Long>();

        try {
            long start = System.currentTimeMillis();

            firstTimeSelect = prepareStatement("SELECT count(*) from project_platform", TARGET_DB);
            rs = firstTimeSelect.executeQuery();
            rs.next();

            // no records, it's the first run of loading platforms
            boolean firstRun = rs.getInt(1) == 0;

            if(firstRun) log.info("Loading project platform table for the first time. A complete load will be performed");

            final String SELECT = "select pp.project_id, pp.project_platform_id, ppl.name from project_platform pp, project_platform_lu ppl, project p where pp.project_platform_id = ppl.project_platform_id and pp.project_id = p.project_id \n" +
                    (firstRun ? "" : " and (p.create_date > ? OR p.modify_date > ?);");

            selectPlatforms = prepareStatement(SELECT, SOURCE_DB);

            if(!firstRun) {
                // no the first time, set last loading time
                selectPlatforms.setTimestamp(1, fLastLogTime);
                selectPlatforms.setTimestamp(2, fLastLogTime);
            }


            final String DELETE = "delete from project_platform where project_id = ?";
            deletePlatforms = prepareStatement(DELETE, TARGET_DB);

            final String INSERT = "insert into project_platform (project_id, project_platform_id, name) VALUES(?, ?, ?)";
            insertPlatforms = prepareStatement(INSERT, TARGET_DB);

            rs = selectPlatforms.executeQuery();

            int countRecords = 0;

            while (rs.next()) {
                long projectID = rs.getLong("project_id");
                long projectPlatformID = rs.getLong("project_platform_id");
                String name = rs.getString("name");

                if(!firstRun && !deletedProjects.contains(projectID)) {
                    // the load is not run for the first time && it's not processed in this load, clear the old platforms for the project
                    deletePlatforms.clearParameters();
                    deletePlatforms.setLong(1, projectID);
                    deletePlatforms.executeUpdate();
                    deletedProjects.add(projectID);
                }

                insertPlatforms.clearParameters();
                insertPlatforms.setLong(1, projectID);
                insertPlatforms.setLong(2, projectPlatformID);
                insertPlatforms.setString(3, name);

                insertPlatforms.executeUpdate();
                countRecords ++;
            }

            log.info("Loaded " + countRecords + " records in "  + (System.currentTimeMillis() - start) / 1000 + " seconds");

        } catch(SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load Project Platforms failed.\n" +
                    sqle.getMessage());
        } finally {
            close(rs);
            close(firstTimeSelect);
            close(selectPlatforms);
            close(deletePlatforms);
            close(insertPlatforms);
            deletedProjects.clear();
            deletedProjects = null;
        }

    }

}
