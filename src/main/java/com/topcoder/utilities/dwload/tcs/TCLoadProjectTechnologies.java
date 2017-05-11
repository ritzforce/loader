/*
 * Copyright (C) 2004 - 2016 TopCoder Inc., All Rights Reserved.
 */
package com.topcoder.utilities.dwload.tcs;

import com.topcoder.shared.util.DBMS;
import com.topcoder.shared.util.logging.Logger;
import com.topcoder.utilities.dwload.TCLoadTCS;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;


public class TCLoadProjectTechnologies extends TCLoadTCS {

    private static Logger log = Logger.getLogger(TCLoadProjectTechnologies.class);

    @Override
    public void performLoad() throws Exception {
        doLoadProjectTechnologies();
    }

    /**
     * Loads the project technologies.
     *
     * @throws Exception if any error.
     * @since 1.2.3
     */
    private void doLoadProjectTechnologies() throws Exception {
        log.info("load project technologies");

        PreparedStatement firstTimeSelect = null;
        PreparedStatement deleteTechnologies = null;
        PreparedStatement selectTechnologies = null;
        PreparedStatement insertTechnologies = null;
        ResultSet rs = null;
        Set<Long> deletedProjects = new HashSet<Long>();

        try {
            long start = System.currentTimeMillis();

            firstTimeSelect = prepareStatement("SELECT count(*) from project_technology", TARGET_DB);
            rs = firstTimeSelect.executeQuery();
            rs.next();

            // no records, it's the first run of loading technologies
            boolean firstRun = rs.getInt(1) == 0;

            if(firstRun) log.info("Loading project technology table for the first time. A complete load will be performed");

            final String SELECT = "select p.project_id, ct.technology_type_id, ttl.technology_name from project p, project_info pi, comp_technology ct, technology_types ttl \n" +
                    "where p.project_id = pi.project_id AND pi.project_info_type_id = 1 AND pi.value = ct.comp_vers_id and ct.technology_type_id = ttl.technology_type_id " +
                    (firstRun ? "" : " AND (p.create_date > ? OR p.modify_date > ?)");

            selectTechnologies = prepareStatement(SELECT, SOURCE_DB);

            if(!firstRun) {
                // no the first time, set last loading time
                selectTechnologies.setTimestamp(1, fLastLogTime);
                selectTechnologies.setTimestamp(2, fLastLogTime);
            }


            final String DELETE = "delete from project_technology where project_id = ?";
            deleteTechnologies = prepareStatement(DELETE, TARGET_DB);

            final String INSERT = "insert into project_technology (project_id, project_technology_id, name) VALUES(?, ?, ?)";
            insertTechnologies = prepareStatement(INSERT, TARGET_DB);

            rs = selectTechnologies.executeQuery();

            int countRecords = 0;

            while (rs.next()) {
                long projectID = rs.getLong("project_id");
                long projectTechnologyID = rs.getLong("technology_type_id");
                String name = rs.getString("technology_name");

                if(!firstRun && !deletedProjects.contains(projectID)) {
                    // the load is not run for the first time && it's not processed in this load, clear the old technologies for the project
                    deleteTechnologies.clearParameters();
                    deleteTechnologies.setLong(1, projectID);
                    deleteTechnologies.executeUpdate();
                    deletedProjects.add(projectID);
                }

                insertTechnologies.clearParameters();
                insertTechnologies.setLong(1, projectID);
                insertTechnologies.setLong(2, projectTechnologyID);
                insertTechnologies.setString(3, name);

                insertTechnologies.executeUpdate();
                countRecords ++;
            }

            log.info("Loaded " + countRecords + " records in "  + (System.currentTimeMillis() - start) / 1000 + " seconds");

        } catch(SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load Project technologies failed.\n" +
                    sqle.getMessage());
        } finally {
            close(rs);
            close(firstTimeSelect);
            close(selectTechnologies);
            close(deleteTechnologies);
            close(insertTechnologies);
            deletedProjects.clear();
            deletedProjects = null;
        }
    }

}
