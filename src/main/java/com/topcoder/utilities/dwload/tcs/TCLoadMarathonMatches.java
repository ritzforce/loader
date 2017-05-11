/*
 * Copyright (C) 2004 - 2016 TopCoder Inc., All Rights Reserved.
 */
package com.topcoder.utilities.dwload.tcs;

import com.topcoder.shared.util.DBMS;
import com.topcoder.shared.util.logging.Logger;
import com.topcoder.utilities.dwload.TCLoadTCS;

import java.sql.*;


public class TCLoadMarathonMatches extends TCLoadTCS {

    private static Logger log = Logger.getLogger(TCLoadMarathonMatches.class);

    @Override
    public void performLoad() throws Exception {
        doLoadMarathonMatches();
    }

    /**
     * <p/>
     * Load Marathon Matches to the DW.
     * </p>
     *
     * @throws Exception if any error occurs
     * @since 1.2.5
     */
    private void doLoadMarathonMatches() throws Exception {
        log.info("load marathon matches");
        PreparedStatement select = null;
        PreparedStatement update = null;
        PreparedStatement insert = null;
        PreparedStatement updateAgain = null;
        ResultSet rs = null;

        try {
            //log.debug("PROCESSING PROJECT " + project_id);
            long start = System.currentTimeMillis();

            select = prepareStatement("select count(*) from project where project_category_id = 37", TARGET_DB);
            rs = select.executeQuery();
            rs.next();

            boolean firstRun = rs.getInt(1) == 0;

            if (firstRun) log.info("Loading marathon matches for the first time.  A complete load will be performed.");

            //get data from source DB
            final String SELECT =
                    "SELECT p.project_id ,\n" +
                            "       cc.component_id ,\n" +
                            "       cc.component_name ,\n" +
                            "\n" +
                            "  ( SELECT COUNT(*)\n" +
                            "   FROM informixoltp:round_registration rr\n" +
                            "   WHERE rr.round_id = pimatchid.value::INTEGER) AS num_registrations,\n" +
                            "\n" +
                            "  ( SELECT COUNT(*)\n" +
                            "   FROM informixoltp:long_component_state cs,\n" +
                            "                                          informixoltp:long_submission s\n" +
                            "   WHERE s.example = 0\n" +
                            "     AND s.long_component_state_id = cs.long_component_state_id\n" +
                            "     AND cs.round_id = pimatchid.value::INTEGER) AS num_submissions ,\n" +
                            "\n" +
                            "  ( SELECT SUM(prize_amount)\n" +
                            "   FROM prize pr\n" +
                            "   WHERE pr.prize_type_id = 15\n" +
                            "     AND pr.project_id = p.project_id\n" +
                            "     AND pr.place = 1) AS first_place_prize ,\n" +
                            "       p.project_category_id + 111 AS phase_id ,\n" +
                            "       pcl.NAME AS phase_desc ,\n" +
                            "       cat.category_id ,\n" +
                            "       cat.category_name AS category_desc ,\n" +
                            "\n" +
                            "  ( SELECT rs1.start_time::DATETIME YEAR TO SECOND\n" +
                            "   FROM informixoltp:round_segment rs1\n" +
                            "   WHERE rs1.round_id = pimatchid.value::INTEGER\n" +
                            "     AND rs1.segment_id = 1) AS posting_date -- registration start\n" +
                            "\n" +
                            "                                           ,\n" +
                            "\n" +
                            "  ( SELECT rs2.end_time::DATETIME YEAR TO SECOND\n" +
                            "   FROM informixoltp:round_segment rs2\n" +
                            "   WHERE rs2.round_id = pimatchid.value::INTEGER\n" +
                            "     AND rs2.segment_id = 2) AS submitby_date -- submission end\n" +
                            "\n" +
                            "    ,\n" +
                            "    1 AS level_id ,\n" +
                            "    picompletitiontime.value AS complete_date ,\n" +
                            "    p.project_status_id AS project_stat_id ,\n" +
                            "    psl.NAME AS project_stat_name ,\n" +
                            "    cat.viewable AS viewable ,\n" +
                            "    cv.version AS version_id ,\n" +
                            "    cv.version_text ,\n" +
                            "    p.project_category_id ,\n" +
                            "    pcl.NAME AS project_category_name ,\n" +
                            "    p.tc_direct_project_id ,\n" +
                            "    piadminfee.value::DECIMAL(10, 2) AS admin_fee ,\n" +
                            "    NVL(\n" +
                            "          ( SELECT CAST(NVL(pi57.value, '0') AS DECIMAL(10, 2))\n" +
                            "           FROM project_info pi57\n" +
                            "           WHERE p.project_id = pi57.project_id\n" +
                            "             AND pi57.project_info_type_id = 57), 0) AS contest_fee_percentage ,\n" +
                            "    NVL(\n" +
                            "          ( SELECT SUM(total_amount)\n" +
                            "           FROM informixoltp:payment_detail pmd, informixoltp:payment pm, project_info pi56\n" +
                            "           WHERE pi56.project_id = p.project_id\n" +
                            "             AND pi56.project_info_type_id = 56\n" +
                            "             AND pmd.algorithm_round_id = pi56.value::INTEGER\n" +
                            "             AND pmd.installment_number = 1\n" +
                            "             AND pmd.component_project_id IS NULL\n" +
                            "             AND pm.most_recent_detail_id = pmd.payment_detail_id\n" +
                            "             AND NOT pmd.payment_status_id IN (65, 68, 69)), 0) AS contest_prizes_total ,\n" +
                            "\n" +
                            "  ( SELECT NVL(SUM(pr.number_of_submissions * pr.prize_amount), 0)\n" +
                            "   FROM prize pr\n" +
                            "   WHERE pr.project_id = p.project_id\n" +
                            "     AND pr.prize_type_id IN (14,\n" +
                            "                              15)) AS total_prize ,\n" +
                            "                                            pibilling.value AS billing_project_id ,\n" +
                            "\n" +
                            "  ( SELECT rs4.end_time::DATETIME YEAR TO SECOND\n" +
                            "   FROM informixoltp:round_segment rs4\n" +
                            "   WHERE rs4.round_id = pimatchid.value::INTEGER\n" +
                            "     AND rs4.segment_id = 4) AS actual_complete_date\n" +
                            "FROM project p ,\n" +
                            "     project_info pimatchid ,\n" +
                            "     project_info pir ,\n" +
                            "     project_info pivers ,\n" +
                            "     OUTER project_info picompletitiontime ,\n" +
                            "     OUTER project_info piadminfee ,\n" +
                            "     OUTER project_info pibilling ,\n" +
                            "     categories cat ,\n" +
                            "     comp_catalog cc ,\n" +
                            "     comp_versions cv ,\n" +
                            "     project_status_lu psl ,\n" +
                            "     project_category_lu pcl\n" +
                            "WHERE pir.project_id = p.project_id\n" +
                            "  AND pir.project_info_type_id = 2\n" +
                            "  AND pimatchid.project_id = p.project_id\n" +
                            "  AND pimatchid.project_info_type_id = 56\n" +
                            "  AND pivers.project_id = p.project_id\n" +
                            "  AND pivers.project_info_type_id = 1\n" +
                            "  AND pivers.value = cv.comp_vers_id\n" +
                            "  AND picompletitiontime.project_id = p.project_id\n" +
                            "  AND picompletitiontime.project_info_type_id = 21\n" +
                            "  AND piadminfee.project_id = p.project_id\n" +
                            "  AND piadminfee.project_info_type_id = 31\n" +
                            "  AND pibilling.project_id = p.project_id\n" +
                            "  AND pibilling.project_info_type_id = 32\n" +
                            "  AND pibilling.value > 0\n" +
                            "  AND p.project_id NOT IN\n" +
                            "    ( SELECT ce.contest_id\n" +
                            "     FROM contest_eligibility ce\n" +
                            "     WHERE ce.is_studio = 0)\n" +
                            "  AND cc.component_id = pir.value\n" +
                            "  AND cc.root_category_id = cat.category_id\n" +
                            "  AND psl.project_status_id = p.project_status_id\n" +
                            "  AND pcl.project_category_id = p.project_category_id\n" +
                            "  AND p.project_category_id = 37 -- marathon match only\n" +
                            "AND p.project_status_id = 7 -- completed only\n" + (!firstRun ?
                            "AND (\n" +
                                    "    p.modify_date > ?\n" +
                                    "    OR cv.modify_date > ?\n" +
                                    "    OR p.project_id IN (\n" +
                                    "      SELECT DISTINCT pi.project_id\n" +
                                    "      FROM project_info pi\n" +
                                    "      WHERE project_info_type_id IN (\n" +
                                    "          2\n" +
                                    "          ,3\n" +
                                    "          ,21\n" +
                                    "          ,22\n" +
                                    "          ,23\n" +
                                    "          ,26\n" +
                                    "          ,31\n" +
                                    "          ,32\n" +
                                    "          ,57\n" +
                                    "          ,56\n" +
                                    "          )\n" +
                                    "        AND (\n" +
                                    "          pi.create_date > ?\n" +
                                    "          OR pi.modify_date > ?\n" +
                                    "          )\n" +
                                    "      )\n" +
                                    "    OR pimatchid.value::INT IN (\n" +
                                    "      SELECT DISTINCT pmd.algorithm_round_id::INT\n" +
                                    "      FROM informixoltp: payment pm\n" +
                                    "      INNER JOIN informixoltp: payment_detail pmd ON pm.most_recent_detail_id = pmd.payment_detail_id\n" +
                                    "      WHERE NOT pmd.payment_status_id IN (65,69)\n" +
                                    "        AND (\n" +
                                    "          pmd.create_date > ?\n" +
                                    "          OR pmd.date_modified > ?\n" +
                                    "          OR pm.create_date > ?\n" +
                                    "          OR pm.modify_date > ?\n" +
                                    "          )\n" +
                                    "      )\n" +
                                    "    )\n" : "");

            final String UPDATE = "update project set component_name = ?,  num_registrations = ?, " +
                    "num_submissions = ?," +
                    "phase_id = ?, phase_desc = ?, category_id = ?, category_desc = ?, posting_date = ?, submitby_date " +
                    "= ?, complete_date = ?, component_id = ?, " +
                    "status_id = ?, status_desc = ?, level_id = ?, viewable_category_ind = ?, version_id = ?, version_text = ?, " +
                    "project_category_id = ?, project_category_name = ?, " +
                    "tc_direct_project_id = ?, admin_fee = ?, contest_prizes_total = ?, " +
                    "client_project_id = ?, duration = ? , last_modification_date = current, " +
                    "first_place_prize = ?, total_prize = ? " +
                    "where project_id = ? ";

            final String INSERT = "insert into project (project_id, component_name, num_registrations, num_submissions, " +
                    "phase_id, phase_desc, " +
                    "category_id, category_desc, posting_date, submitby_date, complete_date, component_id, " +
                    "status_id, status_desc, level_id, viewable_category_ind, version_id, " +
                    "version_text, project_category_id, project_category_name, " +
                    "tc_direct_project_id, admin_fee, contest_prizes_total, client_project_id, duration, last_modification_date, first_place_prize, total_prize) " +
                    "values (?, ?, ?, ?, ?, " +
                    "?, ?, ?, ?, ?, " +
                    "?, ?, ?, ?, ?, " +
                    "?, ?, ?, ?, ?, " +
                    "?, ?, ?, ?, ?, " +
                    "current, ?, ?) ";

            // Statements for updating the duration, fulfillment, start_date_calendar_id fields
            final String UPDATE_AGAIN = "UPDATE project SET " +
                    "fulfillment = (CASE WHEN status_id = 7 THEN 1 ELSE 0 END), " +
                    "start_date_calendar_id = (SELECT calendar_id FROM calendar c WHERE YEAR(project.posting_date) = c.year " +
                    "                          AND MONTH(project.posting_date) = c.month_numeric " +
                    "                          AND DAY(project.posting_date) = c.day_of_month) " +
                    "WHERE complete_date IS NOT NULL AND tc_direct_project_id > 0 AND posting_date IS NOT NULL";

            select = prepareStatement(SELECT, SOURCE_DB);

            if(!firstRun) {
                select.setTimestamp(1, fLastLogTime);
                select.setTimestamp(2, fLastLogTime);
                select.setTimestamp(3, fLastLogTime);
                select.setTimestamp(4, fLastLogTime);
                select.setTimestamp(5, fLastLogTime);
                select.setTimestamp(6, fLastLogTime);
                select.setTimestamp(7, fLastLogTime);
                select.setTimestamp(8, fLastLogTime);
            }

            update = prepareStatement(UPDATE, TARGET_DB);
            insert = prepareStatement(INSERT, TARGET_DB);
            updateAgain = prepareStatement(UPDATE_AGAIN, TARGET_DB);
            rs = select.executeQuery();
            int count = 0;
            while (rs.next()) {

                if (rs.getLong("project_stat_id") != DELETED_PROJECT_STATUS) {
                    if (rs.getLong("version_id") > 999) {
                        log.info("the version id is more than 999");
                        continue;
                        // throw new Exception("component " + rs.getString("component_name") + " has a version > 999");
                    }

                    long duration = -1;

                    Timestamp postingDate = rs.getTimestamp("posting_date");
                    //update record, if 0 rows affected, insert record
                    update.setString(1, rs.getString("component_name"));
                    update.setObject(2, rs.getObject("num_registrations"));
                    update.setInt(3, rs.getInt("num_submissions"));
                    update.setInt(4, rs.getInt("phase_id"));
                    update.setString(5, rs.getString("phase_desc"));
                    update.setInt(6, rs.getInt("category_id"));
                    update.setString(7, rs.getString("category_desc"));
                    if (postingDate != null) {
                        update.setDate(8, new Date(postingDate.getTime()));
                    } else {
                        update.setNull(8, Types.DATE);
                    }
                    update.setDate(9, rs.getDate("submitby_date"));
                    Timestamp completeDate = convertToDate(rs.getString("complete_date"));
                    Timestamp actualCompleteDate = convertToDate(rs.getString("actual_complete_date"));
                    if (completeDate != null) {
                        update.setTimestamp(10, completeDate);
                    } else {
                        update.setNull(10, Types.TIMESTAMP);
                    }

                    if (actualCompleteDate != null && postingDate != null) {
                        duration = actualCompleteDate.getTime() - postingDate.getTime();
                    }

                    update.setLong(11, rs.getLong("component_id"));
                    update.setLong(12, rs.getInt("project_stat_id"));
                    update.setString(13, rs.getString("project_stat_name"));
                    update.setLong(14, rs.getLong("level_id"));
                    update.setInt(15, rs.getInt("viewable"));
                    update.setInt(16, (int) rs.getLong("version_id"));
                    update.setString(17, rs.getString("version_text"));

                    update.setInt(18, rs.getInt("project_category_id"));
                    update.setString(19, rs.getString("project_category_name"));
                    update.setLong(20, rs.getLong("tc_direct_project_id"));

                    double prizeTotal = rs.getDouble("contest_prizes_total");
                    double percentage = rs.getDouble("contest_fee_percentage");
                    double adminFee = rs.getDouble("admin_fee");
                    long projectStatusId = rs.getLong("project_stat_id");
                    if (projectStatusId == 4 ||  projectStatusId == 5 || projectStatusId == 6 || projectStatusId == 8 || projectStatusId == 11)
                    {
                        adminFee = 0;
                    }
                    update.setDouble(21, (percentage < 1e-5 ? adminFee : percentage * prizeTotal));
                    update.setDouble(22, prizeTotal);
                    if (rs.getString("billing_project_id") != null
                            && !rs.getString("billing_project_id").equals("0"))
                    {
                        update.setLong(23, rs.getLong("billing_project_id"));
                    }
                    else
                    {
                        update.setNull(23, Types.DECIMAL);
                    }

                    if (duration >= 0) {
                        update.setLong(24, duration / 1000 / 60);
                    } else {
                        update.setNull(24, Types.DECIMAL);
                    }

                    update.setDouble(25, rs.getDouble("first_place_prize"));
                    update.setDouble(26, rs.getDouble("total_prize"));

                    update.setLong(27, rs.getLong("project_id"));

                    System.out.println("------------marathon match project id --------------------------" + rs.getLong("project_id"));

                    int retVal = update.executeUpdate();

                    if (retVal == 0) {
                        //need to insert
                        insert.setLong(1, rs.getLong("project_id"));
                        insert.setString(2, rs.getString("component_name"));
                        insert.setObject(3, rs.getObject("num_registrations"));
                        insert.setInt(4, rs.getInt("num_submissions"));
                        insert.setInt(5, rs.getInt("phase_id"));
                        insert.setString(6, rs.getString("phase_desc"));
                        insert.setInt(7, rs.getInt("category_id"));
                        insert.setString(8, rs.getString("category_desc"));
                        if (postingDate != null) {
                            insert.setDate(9, new Date(postingDate.getTime()));
                        } else {
                            insert.setNull(9, Types.DATE);
                        }
                        insert.setDate(10, rs.getDate("submitby_date"));
                        completeDate = convertToDate(rs.getString("complete_date"));
                        if (completeDate != null) {
                            insert.setDate(11, new java.sql.Date(completeDate.getTime()));
                        } else {
                            insert.setNull(11, Types.DATE);
                        }
                        insert.setLong(12, rs.getLong("component_id"));
                        insert.setLong(13, rs.getInt("project_stat_id"));
                        insert.setString(14, rs.getString("project_stat_name"));
                        insert.setLong(15, rs.getLong("level_id"));
                        insert.setInt(16, rs.getInt("viewable"));
                        insert.setInt(17, (int) rs.getLong("version_id"));
                        insert.setString(18, rs.getString("version_text"));
                        insert.setInt(19, rs.getInt("project_category_id"));
                        insert.setString(20, rs.getString("project_category_name"));
                        insert.setLong(21, rs.getLong("tc_direct_project_id"));
                        insert.setDouble(22, (percentage < 1e-7 ? adminFee : percentage * prizeTotal));
                        insert.setDouble(23, prizeTotal);
                        if (rs.getString("billing_project_id") != null
                                && !rs.getString("billing_project_id").equals("0"))
                        {    //System.out.println("------------billing id-------------------"+rs.getString("billing_project_id")+"!!!");
                            insert.setLong(24, rs.getLong("billing_project_id"));
                        }
                        else
                        {
                            insert.setNull(24, Types.DECIMAL);
                        }
                        if (duration >= 0) {
                            insert.setLong(25, duration / 1000 / 60);
                        } else {
                            insert.setNull(25, Types.DECIMAL);
                        }


                        insert.setDouble(26, rs.getDouble("first_place_prize"));
                        insert.setDouble(27, rs.getDouble("total_prize"));

                        insert.executeUpdate();
                    }
                } else {
                    // we need to delete this project and all related objects in the database.
                    log.info("Found marathon match project to delete: " + rs.getLong("project_id"));
                    deleteProject(rs.getLong("project_id"));
                }
                count++;
            }

            // update the start_date_calendar_id, duration, fulfillment fields
            updateAgain.executeUpdate();
            log.info("loaded " + count + " marathon match records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");

        } catch (SQLException sqle) {
            sqle.printStackTrace();
            DBMS.printSqlException(true, sqle);
            log.error("Load marathon match into 'project' table failed.", sqle);
            throw new Exception("Load marathon match into 'project' table failed.\n" +
                    sqle.getMessage());
        } finally {
            close(rs);
            close(select);
            close(insert);
            close(update);
            close(updateAgain);
        }

    }

}
