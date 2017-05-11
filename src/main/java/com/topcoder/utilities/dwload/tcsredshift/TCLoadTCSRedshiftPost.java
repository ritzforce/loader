/*
 * Copyright (C) 2004 - 2016 TopCoder Inc., All Rights Reserved.
 */
package com.topcoder.utilities.dwload.tcsredshift;

import com.topcoder.shared.util.DBMS;
import com.topcoder.shared.util.dwload.CacheClearer;
import com.topcoder.shared.util.logging.Logger;
import com.topcoder.utilities.dwload.TCLoadTCSRedshift;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;


public class TCLoadTCSRedshiftPost extends TCLoadTCSRedshift {

    private static Logger log = Logger.getLogger(TCLoadTCSRedshiftPost.class);

    @Override
    public void performLoad() throws Exception {
//        doClearCache();     // Cache must be set up & running (see resources/cache.properties)

        setLastUpdateTime();

        log.info("[TCS -> REDSHIFT LOAD] SUCCESS: TCS to Redshift load ran successfully.");

    }

    private void doClearCache() {
        try {
            String[] keys = new String[]{
                    "member_projects", "project_results_all", "contest_prizes", "contest_projects", "project_details",
                    "tccc05_", "tccc06_", "tco07_", "usdc_", "component_history", "tcs_ratings_history",
                    "member_profile", "Coder_Data", "Coder_Track_Data", "Coder_Dev_Data", "Coder_Des_Data", "Component_",
                    "public_home_data", "top_designers", "top_developers", "tco04",
                    "coder_all_ratings", "tco05", "coder_dev", "coder_des", "coder_algo", "dd_track",
                    "dd_design", "dd_development", "dd_component", "comp_list", "find_projects", "get_review_scorecard",
                    "get_screening_scorecard", "project_info", "reviewers_for_project", "scorecard_details", "submissions",
                    "comp_contest_details", "dr_leader_board", "competition_history", "algo_competition_history",
                    "dr_current_period", "dr_stages", "dr_seasons", "component_color_change", "stage_outstanding_projects",
                    "season_outstanding_projects", "dr_results", "dr_stages", "dr_contests_for_stage",
                    "outstanding_projects", "dr_points_detail", "drv2_results", "dr_track_details", "dr_track_list",
                    "dr_concurrent_track", "event_", "software_"
            };

            HashSet<String> s = new HashSet<String>();
            for (String key : keys) {
                s.add(key);
            }
            CacheClearer.removelike(s);
        } catch (Exception e) {
            log.error("An error caught while clearing the cache (ignored).", e);
        }
    }

    private void setLastUpdateTime() throws Exception {
        PreparedStatement psUpd = null;
        StringBuffer query;

        try {
            int retVal;
            query = new StringBuffer(100);
            query.append("INSERT INTO update_log ");
//            query.append("      (log_id ");        // 1
            query.append("       (calendar_id ");  // 2
            query.append("       ,log_timestamp ");   // 3
            query.append("       ,log_type_id) ");   // 4
            query.append("VALUES (?, ?, ").append(TCS_LOG_TYPE).append(")");
            psUpd = prepareStatement(query.toString(), TARGET_DB);

            int calendar_id = lookupCalendarId(fStartTime, TARGET_DB);
            psUpd.setInt(1, calendar_id);
            psUpd.setTimestamp(2, fStartTime);

            retVal = psUpd.executeUpdate();
            if (retVal != 1) {
                throw new SQLException("SetLastUpdateTime " +
                        " modified " + retVal + " rows, not one.");
            }
        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Failed to set last log time.\n" +
                    sqle.getMessage());
        } finally {
            close(psUpd);
        }
    }

}
