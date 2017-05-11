/*
 * Copyright (C) 2004 - 2016 TopCoder Inc., All Rights Reserved.
 */
package com.topcoder.utilities.dwload.tcsredshift;

import com.topcoder.shared.util.DBMS;
import com.topcoder.shared.util.logging.Logger;
import com.topcoder.utilities.dwload.TCLoadTCSRedshift;

import java.sql.*;


public class TCLoadProjects extends TCLoadTCSRedshift {

    private static Logger log = Logger.getLogger(TCLoadProjects.class);

    @Override
    public void performLoad() throws Exception {
        doLoadProjects();
    }

    /**
     * <p/>
     * Load projects to the DW.
     * </p>
     *
     * @throws Exception if any error occurs
     */
    public void doLoadProjects() throws Exception {
        log.info("load projects");
        PreparedStatement select = null;
        PreparedStatement update = null;
        PreparedStatement insert = null;
        PreparedStatement updateAgain = null;
        ResultSet rs = null;

        try {
            //log.debug("PROCESSING PROJECT " + project_id);
            long start = System.currentTimeMillis();

            loadNewColumnsForProjectFirstTime();

            loadNewColumns2ForProjectFirstTime();

            loadNewColumns3ForProjectFirstTime();

            //get data from source DB
            final String SELECT =
                    "select p.project_id " +
                            "   ,cc.component_id " +
                            "   ,cc.component_name " +
                            "   ,(select count(*) from resource where project_id = p.project_id and resource_role_id = 1) as num_registrations " +
                            "   ,(select count(*) from submission sub, upload where sub.submission_type_id = 1 and sub.upload_id = upload.upload_id and project_id = p.project_id and submission_status_id <> 5) as num_submissions " +
                            //todo this should probably use the flag on project result in the dw, not got back to the submission table in transactional
                            "   ,(select count(*) from submission s, upload where s.submission_type_id = 1 and upload.upload_id = s.upload_id and project_id = p.project_id and submission_status_id in (1, 3, 4)) as num_valid_submissions " +

                            "   ,(select count(*) from submission sub, upload where sub.submission_type_id = 3 and sub.upload_id = upload.upload_id and project_id = p.project_id and submission_status_id <> 5) as num_checkpoint_submissions " +
                            "   ,(select count(*) from submission s, upload where s.submission_type_id = 3 and upload.upload_id = s.upload_id and project_id = p.project_id and submission_status_id in (1, 3, 4, 7)) as num_valid_checkpoint_submissions " +
                            "   ,(select SUM(prize_amount) from prize pr where pr.prize_type_id = 15 and  pr.project_id = p.project_id and pr.place = 1) as first_place_prize " +

                            //todo this should use the flag on project result, not go back to transactional
                            "   ,(select count(*) from submission s, upload u where s.submission_type_id = 1 and u.upload_id = s.upload_id and project_id = p.project_id and submission_status_id in (1, 4)) as num_submissions_passed_review " +
                            //todo again...none of this aggregate data should come from transactional
                            "   ,(select avg(case when raw_score is null then 0 else raw_score end) from project_result where project_id = p.project_id and raw_score is not null) as avg_raw_score " +
                            "   ,(select avg(case when final_score is null then 0 else final_score end) from project_result where project_id = p.project_id and final_score is not null) as avg_final_score " +
                            "   ,p.project_category_id + 111 as phase_id " +
                            "   ,pcl.name as phase_desc " +
                            "   ,cat.category_id " +
                            "   ,cat.category_name as category_desc " +
                            "   ,case when ppd.actual_start_time is not null then ppd.actual_start_time else ppd.scheduled_start_time end as posting_date " +
                            "   ,psd.actual_end_time as submitby_date " +
                            "   ,1 as level_id " +
                            "   ,pi1.value as complete_date  " +
                            "   ,(select phase_type_id from project_phase where project_phase_id = (select min(project_phase_id) from project_phase where project_id = p.project_id and phase_status_id = 2)) as review_phase_id " +
                            "   ,(select name from phase_type_lu where phase_type_id = (select phase_type_id from project_phase where project_phase_id = (select min(project_phase_id) from project_phase where project_id = p.project_id and phase_status_id = 2))) as review_phase_name " +
                            "   ,p.project_status_id as project_stat_id " +
                            "   ,psl.name as project_stat_name " +
                            "   ,cat.viewable as viewable " +
                            "   ,cv.version as version_id " +
                            "   ,cv.version_text " +
                            "   ,pivi.value as rating_date " +
                            "   ,case when pivt.value is not null then substr(pivt.value,1,20) else null end as winner_id" +
                            "   ,case when pict.value is not null then substr(pict.value,1,4) else 'On' end as digital_run_ind   " +
                            "   ,cv.suspended_ind " +
                            "   ,p.project_category_id " +
                            "   ,pcl.name " +
                            "   ,p.tc_direct_project_id " +
                            "   ,piaf.value::DECIMAL(10,2) AS admin_fee " +
                            "   ,nvl((select cast (nvl(pi57.value, '0') as DECIMAL (10,2)) from project_info pi57" +
                            "       where p.project_id = pi57.project_id and pi57.project_info_type_id = 57),0) as contest_fee_percentage " +
//                            "   ,(SELECT SUM(value::decimal(10,2)) " +
//                            "     FROM project_info costs " +
//                            "     WHERE costs.project_id = p.project_id " +
//                            "     AND costs.project_info_type_id IN (30, 33, 35, 36, 37, 38, 39)) " +
//                            "     AS contest_prizes_total " +
            /*                "   ,(SELECT SUM(total_amount) " +
                            "     FROM informixoltp:payment pm INNER JOIN informixoltp:payment_detail pmd ON pm.most_recent_detail_id = pmd.payment_detail_id " +
                            "     WHERE pmd.component_project_id::int = p.project_id " +
                            "     AND NOT pmd.payment_status_id IN (65, 69)) " + */
                            "   , case when p.project_status_id = 7 then " +
                            "   NVL((SELECT sum(total_amount) " +
                            "       FROM  informixoltp:payment_detail pmd, informixoltp:payment pm " +
                            "        WHERE pmd.component_project_id = p.project_id and pmd.installment_number = 1 " +
                            "        and pm.most_recent_detail_id = pmd.payment_detail_id  " +
                            "        AND NOT pmd.payment_status_id IN (65, 68, 69)), 0) " +
                            "    + " +
                            "    NVL((SELECT sum(pmd2.total_amount)  " +
                            "           FROM  informixoltp:payment_detail pmd,   " +
                            "                 informixoltp:payment pm LEFT OUTER JOIN informixoltp:payment_detail pmd2 on pm.payment_id = pmd2.parent_payment_id,  " +
                            "                 informixoltp:payment pm2  " +
                            "            WHERE pmd.component_project_id = p.project_id and pmd2.installment_number = 1  " +
                            "            and pm.most_recent_detail_id = pmd.payment_detail_id   " +
                            "            and pm2.most_recent_detail_id = pmd2.payment_detail_id  " +
                            "            AND NOT pmd2.payment_status_id IN (65, 68, 69)), 0) " +
                            "     + " +
                            "    nvl((select nvl(sum (cast (nvl (pi30.value, '0') as DECIMAL (10,2))), 0) from project_info pi30, project_info pi26 " +
                            "        where pi30.project_info_type_id = 30 and pi26.project_info_type_id = 26 and pi26.project_id = pi30.project_id  " +
                            "        and pi26.value = 'On' " +
                            "        and pi26.project_id =  p.project_id ), 0) " +
                            "   else NVL((SELECT sum(total_amount) " +
                            "        FROM  informixoltp:payment_detail pmd, informixoltp:payment pm " +
                            "         WHERE pmd.component_project_id = p.project_id and pmd.installment_number = 1 " +
                            "         and pm.most_recent_detail_id = pmd.payment_detail_id  " +
                            "         AND NOT pmd.payment_status_id IN (65, 68, 69)), 0) " +
                            "     + " +
                            "     NVL((SELECT sum(pmd2.total_amount)  " +
                            "            FROM  informixoltp:payment_detail pmd,   " +
                            "                  informixoltp:payment pm LEFT OUTER JOIN informixoltp:payment_detail pmd2 on pm.payment_id = pmd2.parent_payment_id,  " +
                            "                  informixoltp:payment pm2  " +
                            "             WHERE pmd.component_project_id = p.project_id and pmd2.installment_number = 1  " +
                            "             and pm.most_recent_detail_id = pmd.payment_detail_id   " +
                            "             and pm2.most_recent_detail_id = pmd2.payment_detail_id  " +
                            "             AND NOT pmd2.payment_status_id IN (65, 68, 69)), 0) " +
                            "   end AS contest_prizes_total " +
                            ", (select NVL( SUM(pr.number_of_submissions * pr.prize_amount) , 0 ) from prize pr where pr.project_id = p.project_id and pr.prize_type_id IN (14, 15)) AS total_prize " +
                            "   , pib.value AS billing_project_id " +
                            "   , case when pcl.project_type_id != 3 and p.project_category_id not in (9,29,39) then  " +
                            "         (SELECT MAX(ppfr.actual_end_time) " +
                            "          FROM project_phase ppfr " +
                            "           WHERE ppfr.project_id = p.project_id " +
                            "           AND ppfr.phase_type_id = 10 " +
                            "           AND ppfr.phase_status_id = 3 " +
                            "           AND ppfr.actual_end_time <= (SELECT MIN(NVL(actual_start_time, scheduled_start_time)) " +
                            "                                   FROM project_phase ppappr " +
                            "                                   WHERE ppappr.project_id = p.project_id " +
                            "                                   AND ppappr.phase_type_id = 11)) " +
                            "     else (select actual_end_time from project_phase ph3 " +
                            "           where ph3.project_id = p.project_id and ph3.phase_type_id = 4 and ph3.phase_status_id = 3) " +
                            "     end as actual_complete_date  " +
                            "   , (SELECT u.user_id " +
                            "                FROM resource r, resource_info ri, user  u " +
                            "                    WHERE r.project_id = p.project_id and r.resource_id = ri.resource_id  " +
                            "                        and ri.resource_info_type_id = 1 and r.resource_role_id = 13 and ri.value=u.user_id " +
                            "                        and r.resource_id = " +
                            "                            (select min(r2.resource_id) from resource_info ri2, resource r2  " +
                            "                                 where  r2.project_id = p.project_id and r2.resource_id = ri2.resource_id  " +
                            "                                       and ri2.resource_info_type_id = 1 and r2.resource_role_id = 13 " +
                            "   and ri2.value not in (22770213,22719217)))::lvarchar " +
                            "         AS challenge_manager  " +
                            ", (SELECT u.user_id FROM project p2, user u WHERE p2.create_user = u.user_id and p2.project_id = p.project_id) AS challenge_creator " +
                            "        , (SELECT u.user_id FROM project_info pi58, user u WHERE pi58.project_id = p.project_id and pi58.project_info_type_id = 58 and pi58.value::integer = u.user_id) AS challenge_launcher " +
                            "        , (SELECT u.user_id " +
                            "                FROM resource r, user  u " +
                            "                    WHERE r.project_id = p.project_id and r.user_id = u.user_id " +
                            "                        and r.resource_role_id = 14  " +
                            "                        and r.resource_id = " +
                            "                            (select min(r2.resource_id) from resource r2  " +
                            "                                 where  r2.project_id = p.project_id   " +
                            "                                       and r2.resource_role_id = 14)) as copilot" +
                            ", CASE WHEN pcsd.actual_start_time IS NOT NULL THEN pcsd.actual_start_time ELSE pcsd.scheduled_start_time END as checkpoint_start_date" +
                            ", CASE WHEN pced.actual_end_time   IS NOT NULL THEN pced.actual_end_time ELSE pced.scheduled_end_time END as checkpoint_end_date" +
                            ", NVL(ppd.actual_end_time, ppd.scheduled_end_time) as registration_end_date" +
                            ", (SELECT MAX(scheduled_end_time) FROM project_phase phet WHERE phet.project_id = p.project_id) as scheduled_end_date" +
                            ", NVL((SELECT SUM(pr.number_of_submissions) FROM prize pr WHERE pr.project_id = p.project_id AND pr.prize_type_id = 14), 0) AS checkpoint_prize_number" +
                            ", (SELECT NVL(MAX(prize_amount), 0) FROM prize pr WHERE pr.project_id = p.project_id AND pr.prize_type_id = 14) AS checkpoint_prize_amount" +
                            ",(CASE WHEN pict.value = 'On' THEN NVL((SELECT value::decimal FROM project_info pidr WHERE pidr.project_id = p.project_id AND pidr.project_info_type_id = 30), 0) ELSE 0 END) as dr_points" +
                            ",(NVL((SELECT sum(total_amount)  " +
                            "       FROM  informixoltp:payment_detail pmd, informixoltp:payment pm  " +
                            "        WHERE pmd.component_project_id = p.project_id and pmd.installment_number = 1  " +
                            "        and pm.most_recent_detail_id = pmd.payment_detail_id and pmd.payment_type_id = 24  " +
                            "        AND NOT pmd.payment_status_id IN (65, 68, 69)), 0)  +" +
                            "    NVL((SELECT sum(pmd2.total_amount)   " +
                            "           FROM  informixoltp:payment_detail pmd,    " +
                            "                 informixoltp:payment pm LEFT OUTER JOIN informixoltp:payment_detail pmd2 on pm.payment_id = pmd2.parent_payment_id,   " +
                            "                 informixoltp:payment pm2   " +
                            "            WHERE pmd.component_project_id = p.project_id and pmd2.installment_number = 1   " +
                            "            and pm.most_recent_detail_id = pmd.payment_detail_id    " +
                            "            and pm2.most_recent_detail_id = pmd2.payment_detail_id and pmd2.payment_type_id = 24  " +
                            "            AND NOT pmd2.payment_status_id IN (65, 68, 69)), 0)) as reliability_cost" +
                            ",(NVL((SELECT sum(total_amount)  " +
                            "       FROM  informixoltp:payment_detail pmd, informixoltp:payment pm  " +
                            "        WHERE pmd.component_project_id = p.project_id and pmd.installment_number = 1  " +
                            "        and pm.most_recent_detail_id = pmd.payment_detail_id and pmd.payment_type_id IN (7,26,28,36)  " +
                            "        AND NOT pmd.payment_status_id IN (65, 68, 69)), 0)  +" +
                            "    NVL((SELECT sum(pmd2.total_amount)   " +
                            "           FROM  informixoltp:payment_detail pmd,    " +
                            "                 informixoltp:payment pm LEFT OUTER JOIN informixoltp:payment_detail pmd2 on pm.payment_id = pmd2.parent_payment_id,   " +
                            "                 informixoltp:payment pm2   " +
                            "            WHERE pmd.component_project_id = p.project_id and pmd2.installment_number = 1   " +
                            "            and pm.most_recent_detail_id = pmd.payment_detail_id    " +
                            "            and pm2.most_recent_detail_id = pmd2.payment_detail_id and pmd2.payment_type_id IN (7,26,28,36)  " +
                            "            AND NOT pmd2.payment_status_id IN (65, 68, 69)), 0)) as review_cost" +
                            ", (SELECT value::INTEGER FROM project_info piforum WHERE piforum.project_id = p.project_id and piforum.project_info_type_id = 4) as forum_id" +
                            ", (select CASE when pi53.value == 'true' THEN 1 ELSE 0 END FROM project_info pi53 where pi53.project_info_type_id = 53 and pi53.project_id = p.project_id) as submission_viewable" +
                            ", NVL((SELECT 1 FROM contest_eligibility WHERE contest_id = p.project_id), 0) AS is_private" +

                            // estimated_reliability_cost
                            ",(CASE WHEN pire.value = 'true' THEN NVL((SELECT value::decimal FROM project_info pi38 WHERE pi38.project_id = p.project_id AND pi38.project_info_type_id = 38), 0) ELSE 0 END) as estimated_reliability_cost" +

                            // estimated_review_cost
                            ",(NVL((SELECT value::decimal FROM project_info pi33 WHERE pi33.project_id = p.project_id AND pi33.project_info_type_id = 33), 0) " +

                            "+ NVL((SELECT value::decimal FROM project_info pi35 WHERE pi35.project_id = p.project_id AND pi35.project_info_type_id = 35), 0)) as estimated_review_cost" +

                            // estimated_copilot_cost
                            ",NVL((SELECT value::decimal FROM project_info pi49 WHERE pi49.project_id = p.project_id AND pi49.project_info_type_id = 49), 0) as estimated_copilot_cost" +

                            // estimated_admin_fee
                            ",NVL((SELECT value::decimal FROM project_info pi31 WHERE pi31.project_id = p.project_id AND pi31.project_info_type_id = 31), 0) as estimated_admin_fee" +

                            // actual_total_prize
                            ",(NVL((SELECT sum(total_amount)  " +
                            "       FROM  informixoltp:payment_detail pmd, informixoltp:payment pm  " +
                            "        WHERE pmd.component_project_id = p.project_id and pmd.installment_number = 1  " +
                            "        and pm.most_recent_detail_id = pmd.payment_detail_id and pmd.payment_type_id IN (6, 29, 10, 42, 43, 44, 49, 50, 51, 55, 61, 64, 65, 60, 13, 21)  " +
                            "        AND NOT pmd.payment_status_id IN (65, 68, 69)), 0)  +" +
                            "    NVL((SELECT sum(pmd2.total_amount)   " +
                            "           FROM  informixoltp:payment_detail pmd,    " +
                            "                 informixoltp:payment pm LEFT OUTER JOIN informixoltp:payment_detail pmd2 on pm.payment_id = pmd2.parent_payment_id,   " +
                            "                 informixoltp:payment pm2   " +
                            "            WHERE pmd.component_project_id = p.project_id and pmd2.installment_number = 1   " +
                            "            and pm.most_recent_detail_id = pmd.payment_detail_id    " +
                            "            and pm2.most_recent_detail_id = pmd2.payment_detail_id and pmd2.payment_type_id IN (6, 29, 10, 42, 43, 44, 49, 50, 51, 55, 61, 64, 65, 60, 13, 21)  " +
                            "            AND NOT pmd2.payment_status_id IN (65, 68, 69)), 0)) as actual_total_prize" +

                            // copilot_cost
                            ",(NVL((SELECT sum(total_amount)  " +
                            "       FROM  informixoltp:payment_detail pmd, informixoltp:payment pm  " +
                            "        WHERE pmd.component_project_id = p.project_id and pmd.installment_number = 1  " +
                            "        and pm.most_recent_detail_id = pmd.payment_detail_id and pmd.payment_type_id IN (45, 57)  " +
                            "        AND NOT pmd.payment_status_id IN (65, 68, 69)), 0)  +" +
                            "    NVL((SELECT sum(pmd2.total_amount)   " +
                            "           FROM  informixoltp:payment_detail pmd,    " +
                            "                 informixoltp:payment pm LEFT OUTER JOIN informixoltp:payment_detail pmd2 on pm.payment_id = pmd2.parent_payment_id,   " +
                            "                 informixoltp:payment pm2   " +
                            "            WHERE pmd.component_project_id = p.project_id and pmd2.installment_number = 1   " +
                            "            and pm.most_recent_detail_id = pmd.payment_detail_id    " +
                            "            and pm2.most_recent_detail_id = pmd2.payment_detail_id and pmd2.payment_type_id IN (45, 57)  " +
                            "            AND NOT pmd2.payment_status_id IN (65, 68, 69)), 0)) as copilot_cost" +

                            // task flag
                            ",NVL((SELECT value FROM project_info pi82 WHERE pi82.project_id = p.project_id AND pi82.project_info_type_id = 82), 0) as task_ind" +

                            "   from project p , " +
                            "   project_info pir, " +
                            "   project_info pivers, " +
                            "   outer project_info pivi," +
                            "   outer project_info pivt," +
                            "   outer project_info pict," +
                            "   outer project_info pire," +
                            "   outer project_info pi1," +
                            "   outer project_info pi2," +
                            "   outer project_info piaf," +
                            "   outer project_info pib," +
                            "   categories cat, " +
                            "   comp_catalog cc, " +
                            "   comp_versions cv, " +
                            "   project_status_lu psl, " +
                            "   OUTER project_phase psd, " +
                            "   OUTER project_phase ppd, " +
                            "   OUTER project_phase pcsd, " +
                            "   OUTER project_phase pced, " +
                            "   project_category_lu pcl " +
                            " where pir.project_id = p.project_id " +
                            "   and pir.project_info_type_id = 2 " +
                            "   and pivi.project_id = p.project_id " +
                            "   and pivi.project_info_type_id = 22 " +
                            "   and pivers.project_id = p.project_id " +
                            "   and pivers.project_info_type_id = 1 " +
                            "   and pivers.value = cv.comp_vers_id " +
                            "   and pivt.project_id = p.project_id " +
                            "   and pivt.project_info_type_id = 23 " +
                            "   and pict.project_id = p.project_id " +
                            "   and pict.project_info_type_id = 26 " +
                            "   and pire.project_id = p.project_id " +
                            "   and pire.project_info_type_id = 45 " +
                            "   and pi1.project_id = p.project_id " +
                            "   and pi1.project_info_type_id = 21 " +
                            "   and pi2.project_id = p.project_id " +
                            "   and pi2.project_info_type_id = 3 " +
                            "   and piaf.project_id = p.project_id " +
                            "   and piaf.project_info_type_id = 31 " +
                            "   and pib.project_id = p.project_id " +
                            "   and pib.project_info_type_id = 32 and pib.value > 0 " +
                            ELIGIBILITY_CONSTRAINTS_SQL_FRAGMENT +
                            "   and cc.component_id = pir.value " +
                            "   and cc.root_category_id = cat.category_id " +
                            "   and psl.project_status_id = p.project_status_id " +
                            "   and pcl.project_category_id = p.project_category_id " +
                            "   and psd.project_id = p.project_id " +
                            "   and psd.phase_type_id = 2 " +
                            "   and ppd.project_id = p.project_id " +
                            "   and ppd.phase_type_id = 1 " +
                            "   and pcsd.project_id = p.project_id " +
                            "   and pcsd.phase_type_id = 15 " +
                            "   and pced.project_id = p.project_id " +
                            "   and pced.phase_type_id = 17 " +
                            // we need to process deleted project, otherwise there's a possibility
                            // they will keep living in the DW.
                            //" and p.project_status_id <> 3 " +
                            "   and p.project_category_id in " + LOAD_CATEGORIES +
                            "   and (p.modify_date > ? " +
                            // comp versions with modified date
                            "   or cv.modify_date > ? " +
                            // add projects who have modified resources
                            "   or p.project_id in (select distinct r.project_id from resource r where (r.create_date > ? or r.modify_date > ?)) " +
                            // add projects who have modified upload and submissions
                            "   or p.project_id in (select distinct u.project_id from upload u, submission s where s.submission_type_id = 1 and u.upload_id = s.upload_id and " +
                            "   (u.create_date > ? or u.modify_date > ? or s.create_date > ? or s.modify_date > ?)) " +
                            // add projects who have modified results
                            "   or p.project_id in (select distinct pr.project_id from project_result pr where (pr.create_date > ? or pr.modify_date > ?)) " +
                            "   or p.project_id in (select distinct pi.project_id from project_info pi where project_info_type_id in  (2, 3, 21, 22, 23, 26, 31, 32, 33, 38, 45, 49) and (pi.create_date > ? or pi.modify_date > ?)) " +
                            "   or p.project_id in (select distinct pmd.component_project_id::int " +
                            "      FROM informixoltp:payment pm INNER JOIN informixoltp:payment_detail pmd ON pm.most_recent_detail_id = pmd.payment_detail_id " +
                            "      WHERE NOT pmd.payment_status_id IN (65, 69) AND (pmd.create_date > ? or pmd.date_modified > ? or pm.create_date > ? or pm.modify_date > ?)) " +
                            (needLoadMovedProject() ? " OR p.modify_user <> 'Converter'  OR pir.modify_user <> 'Converter' )" : ")");

            final String UPDATE = "update project set component_name = ?,  num_registrations = ?, " +
                    "num_submissions = ?, num_valid_submissions = ?, avg_raw_score = ?, avg_final_score = ?, " +
                    "phase_id = ?, phase_desc = ?, category_id = ?, category_desc = ?, posting_date = ?, submitby_date " +
                    "= ?, complete_date = ?, component_id = ?, review_phase_id = ?, review_phase_name = ?, " +
                    "status_id = ?, status_desc = ?, level_id = ?, viewable_category_ind = ?, version_id = ?, version_text = ?, " +
                    "rating_date = ?, num_submissions_passed_review=?, winner_id=?, stage_id = ?, digital_run_ind = ?, " +
                    "suspended_ind = ?, project_category_id = ?, project_category_name = ?, " +
                    "tc_direct_project_id = ?, admin_fee = ?, contest_prizes_total = ?, " +
                    "client_project_id = ?, duration = ? , last_modification_date = 'now', " +
                    "first_place_prize = ?, num_checkpoint_submissions = ? , num_valid_checkpoint_submissions = ?, total_prize = ?, " +
                    "challenge_manager = ?, challenge_creator = ?, challenge_launcher = ?, copilot = ?, checkpoint_start_date = ?, checkpoint_end_date = ?, " +
                    "registration_end_date = ?, scheduled_end_date = ?, checkpoint_prize_amount = ?, checkpoint_prize_number = ?, dr_points = ?, " +
                    "reliability_cost = ?, review_cost = ?, forum_id = ?, submission_viewable = ?, is_private = ?," +
                    "estimated_reliability_cost = ?, estimated_review_cost = ?, estimated_copilot_cost = ?, estimated_admin_fee = ?, actual_total_prize = ?, copilot_cost = ?, task_ind = ?" +
                    "where project_id = ? ";

            final String INSERT = "insert into project (project_id, component_name, num_registrations, num_submissions, " +
                    "num_valid_submissions, avg_raw_score, avg_final_score, phase_id, phase_desc, " +
                    "category_id, category_desc, posting_date, submitby_date, complete_date, component_id, " +
                    "review_phase_id, review_phase_name, status_id, status_desc, level_id, viewable_category_ind, version_id, " +
                    "version_text, rating_date, num_submissions_passed_review, winner_id, stage_id, digital_run_ind, suspended_ind, project_category_id, project_category_name, " +
                    "tc_direct_project_id, admin_fee, contest_prizes_total, client_project_id, duration, " +
                    "last_modification_date, first_place_prize, num_checkpoint_submissions, num_valid_checkpoint_submissions, total_prize, " +
                    "challenge_manager, challenge_creator, challenge_launcher, copilot, checkpoint_start_date, checkpoint_end_date," +
                    "registration_end_date, scheduled_end_date, checkpoint_prize_amount, checkpoint_prize_number, dr_points," +
                    "reliability_cost, review_cost, forum_id, submission_viewable, is_private," +
                    "estimated_reliability_cost, estimated_review_cost, estimated_copilot_cost, estimated_admin_fee, actual_total_prize, copilot_cost, task_ind)" +
                    "values (?, ?, ?, ?, ?, " +
                    "?, ?, ?, ?, ?, " +
                    "?, ?, ?, ?, ?, " +
                    "?, ?, ?, ?, ?, " +
                    "?, ?, ?, ?, ?, " +
                    "?, ?, ?, ?, ?, ?, ?, ?, ?, " +
                    "?, ?, 'now', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?) ";

            // Statements for updating the duration, fulfillment, start_date_calendar_id fields
            final String UPDATE_AGAIN = "UPDATE project SET " +
                    "fulfillment = (CASE WHEN status_id = 7 THEN 1 ELSE 0 END), " +
                    "start_date_calendar_id = (SELECT calendar_id FROM calendar c WHERE DATE_PART(y, project.posting_date) = c.year " +
                    "                          AND DATE_PART(mon, project.posting_date) = c.month_numeric " +
                    "                          AND DATE_PART(d, project.posting_date) = c.day_of_month) " +
                    "WHERE complete_date IS NOT NULL AND tc_direct_project_id > 0 AND posting_date IS NOT NULL";

            select = prepareStatement(SELECT, SOURCE_DB);
            select.setTimestamp(1, fLastLogTime);
            select.setTimestamp(2, fLastLogTime);
            select.setTimestamp(3, fLastLogTime);
            select.setTimestamp(4, fLastLogTime);
            select.setTimestamp(5, fLastLogTime);
            select.setTimestamp(6, fLastLogTime);
            select.setTimestamp(7, fLastLogTime);
            select.setTimestamp(8, fLastLogTime);
            select.setTimestamp(9, fLastLogTime);
            select.setTimestamp(10, fLastLogTime);
            select.setTimestamp(11, fLastLogTime);
            select.setTimestamp(12, fLastLogTime);
            select.setTimestamp(13, fLastLogTime);
            select.setTimestamp(14, fLastLogTime);
            select.setTimestamp(15, fLastLogTime);
            select.setTimestamp(16, fLastLogTime);
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
                    update.setInt(4, rs.getInt("num_valid_submissions"));

                    if (rs.getObject("avg_raw_score") == null) {
                        update.setNull(5, Types.DOUBLE);
                    } else {
                        update.setObject(5, rs.getObject("avg_raw_score"));
                    }

                    if (rs.getObject("avg_final_score") == null) {
                        update.setNull(6, Types.DOUBLE);
                    } else {
                        update.setObject(6, rs.getObject("avg_final_score"));
                    }

                    update.setInt(7, rs.getInt("phase_id"));
                    update.setString(8, rs.getString("phase_desc"));
                    update.setInt(9, rs.getInt("category_id"));
                    update.setString(10, rs.getString("category_desc"));
                    if (postingDate != null) {
                        update.setDate(11, new Date(postingDate.getTime()));
                    } else {
                        update.setNull(11, Types.DATE);
                    }
                    update.setTimestamp(12, rs.getTimestamp("submitby_date"));
                    Timestamp completeDate = convertToDate(rs.getString("complete_date"));
                    Timestamp actualCompleteDate = convertToDate(rs.getString("actual_complete_date"));
                    if (completeDate != null) {
                        update.setTimestamp(13, completeDate);
                    } else {
                        update.setNull(13, Types.TIMESTAMP);
                    }

                    if (actualCompleteDate != null && postingDate != null) {
                        duration = actualCompleteDate.getTime() - postingDate.getTime();
                    }

                    update.setLong(14, rs.getLong("component_id"));
                    update.setLong(15, rs.getLong("review_phase_id"));
                    update.setString(16, rs.getString("review_phase_name"));
                    update.setLong(17, rs.getInt("project_stat_id"));
                    update.setString(18, rs.getString("project_stat_name"));
                    update.setLong(19, rs.getLong("level_id"));
                    update.setInt(20, rs.getInt("viewable"));
                    update.setInt(21, (int) rs.getLong("version_id"));
                    update.setString(22, rs.getString("version_text"));
                    Timestamp ratingDate = convertToDate(rs.getString("rating_date"));
                    if (ratingDate != null) {
                        update.setTimestamp(23, ratingDate);
                    } else {
                        update.setNull(23, Types.TIMESTAMP);
                    }
                    update.setInt(24, rs.getInt("num_submissions_passed_review"));
                    if (rs.getString("winner_id") == null) {
                        update.setNull(25, Types.DECIMAL);
                    } else {
                        update.setLong(25, rs.getLong("winner_id"));
                    }


                    if (postingDate == null) {
                        update.setNull(26, Types.BIGINT);
                    } else {
                        try {
                            update.setLong(26, calculateStage(new java.sql.Date(postingDate.getTime())));
                        } catch (Exception e) {
                            update.setNull(26, Types.BIGINT);
                        }
                    }
                    String digitRun = rs.getString("digital_run_ind");
                    update.setInt(27, "On".equals(digitRun) || "Yes".equals(digitRun) ? 1 : 0);
                    update.setInt(28, rs.getInt("suspended_ind"));

                    update.setInt(29, rs.getInt("project_category_id"));
                    update.setString(30, rs.getString("name"));
                    update.setLong(31, rs.getLong("tc_direct_project_id"));

                    double prizeTotal = rs.getDouble("contest_prizes_total");
                    double percentage = rs.getDouble("contest_fee_percentage");
                    double adminFee = rs.getDouble("admin_fee");
                    long projectStatusId = rs.getLong("project_stat_id");
                    if (projectStatusId == 4 ||  projectStatusId == 5 || projectStatusId == 6 || projectStatusId == 8 || projectStatusId == 11)
                    {
                        adminFee = 0;
                    }
                    update.setDouble(32, (percentage < 1e-5 ? adminFee : percentage * prizeTotal));
                    update.setDouble(33, prizeTotal);
                    if (rs.getString("billing_project_id") != null
                            && !rs.getString("billing_project_id").equals("0"))
                    {
                        update.setLong(34, rs.getLong("billing_project_id"));
                    }
                    else
                    {
                        update.setNull(34, Types.DECIMAL);
                    }

                    if (duration >= 0) {
                        update.setLong(35, duration / 1000 / 60);
                    } else {
                        update.setNull(35, Types.DECIMAL);
                    }

                    update.setDouble(36, rs.getDouble("first_place_prize"));
                    update.setInt(37, rs.getInt("num_checkpoint_submissions"));
                    update.setInt(38, rs.getInt("num_valid_checkpoint_submissions"));
                    update.setDouble(39, rs.getDouble("total_prize"));

                    setLongParameter(rs, update, "challenge_manager", 40);
                    setLongParameter(rs, update, "challenge_creator", 41);
                    setLongParameter(rs, update, "challenge_launcher", 42);
                    setLongParameter(rs, update, "copilot", 43);


                    Timestamp checkpointStartDate = rs.getTimestamp("checkpoint_start_date");
                    Timestamp checkpointEndDate = rs.getTimestamp("checkpoint_end_date");

                    if (checkpointStartDate != null) {
                        update.setTimestamp(44, checkpointStartDate);
                    } else {
                        update.setNull(44, Types.TIMESTAMP);
                    }

                    if (checkpointEndDate != null) {
                        update.setTimestamp(45, checkpointEndDate);
                    } else {
                        update.setNull(45, Types.TIMESTAMP);
                    }

                    setTimestampParameter(rs, update, "registration_end_date", 46);
                    setTimestampParameter(rs, update, "scheduled_end_date", 47);
                    update.setDouble(48, rs.getDouble("checkpoint_prize_amount"));
                    update.setDouble(49, rs.getDouble("checkpoint_prize_number"));
                    update.setDouble(50, rs.getDouble("dr_points"));
                    update.setDouble(51, rs.getDouble("reliability_cost"));
                    update.setDouble(52, rs.getDouble("review_cost"));

                    setLongParameter(rs, update, "forum_id", 53);
                    setLongParameter(rs, update, "submission_viewable", 54);
                    setLongParameter(rs, update, "is_private", 55);


                    update.setDouble(56, rs.getDouble("estimated_reliability_cost"));
                    update.setDouble(57, rs.getDouble("estimated_review_cost"));
                    update.setDouble(58, rs.getDouble("estimated_copilot_cost"));
                    update.setDouble(59, rs.getDouble("estimated_admin_fee"));
                    update.setDouble(60, rs.getDouble("actual_total_prize"));
                    update.setDouble(61, rs.getDouble("copilot_cost"));
                    update.setInt(62, rs.getInt("task_ind"));


                    update.setLong(63, rs.getLong("project_id"));
                    System.out.println("------------project id --------------------------"+rs.getLong("project_id"));

                    int retVal = update.executeUpdate();

                    if (retVal == 0) {
                        //need to insert
                        insert.setLong(1, rs.getLong("project_id"));
                        insert.setString(2, rs.getString("component_name"));
                        insert.setObject(3, rs.getObject("num_registrations"));
                        insert.setInt(4, rs.getInt("num_submissions"));
                        insert.setInt(5, rs.getInt("num_valid_submissions"));
                        insert.setDouble(6, rs.getDouble("avg_raw_score"));
                        insert.setDouble(7, rs.getDouble("avg_final_score"));
                        insert.setInt(8, rs.getInt("phase_id"));
                        insert.setString(9, rs.getString("phase_desc"));
                        insert.setInt(10, rs.getInt("category_id"));
                        insert.setString(11, rs.getString("category_desc"));
                        if (postingDate != null) {
                            insert.setDate(12, new Date(postingDate.getTime()));
                        } else {
                            insert.setNull(12, Types.DATE);
                        }
                        insert.setTimestamp(13, rs.getTimestamp("submitby_date"));
                        completeDate = convertToDate(rs.getString("complete_date"));
                        if (completeDate != null) {
                            insert.setDate(14, new java.sql.Date(completeDate.getTime()));
                        } else {
                            insert.setNull(14, Types.DATE);
                        }
                        insert.setLong(15, rs.getLong("component_id"));
                        insert.setLong(16, rs.getLong("review_phase_id"));
                        insert.setString(17, rs.getString("review_phase_name"));
                        insert.setLong(18, rs.getInt("project_stat_id"));
                        insert.setString(19, rs.getString("project_stat_name"));
                        insert.setLong(20, rs.getLong("level_id"));
                        insert.setInt(21, rs.getInt("viewable"));
                        insert.setInt(22, (int) rs.getLong("version_id"));
                        insert.setString(23, rs.getString("version_text"));
                        ratingDate = convertToDate(rs.getString("rating_date"));
                        if (ratingDate != null) {
                            insert.setDate(24, new java.sql.Date(ratingDate.getTime()));
                        } else {
                            insert.setNull(24, Types.DATE);
                        }
                        insert.setInt(25, rs.getInt("num_submissions_passed_review"));
                        if (rs.getString("winner_id") == null) {
                            insert.setNull(26, Types.DECIMAL);
                        } else {
                            insert.setLong(26, rs.getLong("winner_id"));
                        }
                        if (postingDate == null) {
                            insert.setNull(27, Types.BIGINT);
                        } else {
                            try {
                                insert.setLong(27, calculateStage(new java.sql.Date(postingDate.getTime())));
                            } catch (Exception e) {
                                insert.setNull(27, Types.BIGINT);
                            }
                        }
                        digitRun = rs.getString("digital_run_ind");
                        insert.setInt(28, "On".equals(digitRun) || "Yes".equals(digitRun) ? 1 : 0);
                        insert.setInt(29, rs.getInt("suspended_ind"));
                        insert.setInt(30, rs.getInt("project_category_id"));
                        insert.setString(31, rs.getString("name"));
                        insert.setLong(32, rs.getLong("tc_direct_project_id"));
                        insert.setDouble(33, (percentage < 1e-7 ? adminFee : percentage * prizeTotal));
                        insert.setDouble(34, prizeTotal);
                        if (rs.getString("billing_project_id") != null
                                && !rs.getString("billing_project_id").equals("0"))
                        {    //System.out.println("------------billing id-------------------"+rs.getString("billing_project_id")+"!!!");
                            insert.setLong(35, rs.getLong("billing_project_id"));
                        }
                        else
                        {
                            insert.setNull(35, Types.DECIMAL);
                        }
                        if (duration >= 0) {
                            insert.setLong(36, duration / 1000 / 60);
                        } else {
                            insert.setNull(36, Types.DECIMAL);
                        }


                        insert.setDouble(37, rs.getDouble("first_place_prize"));
                        insert.setInt(38, rs.getInt("num_checkpoint_submissions"));
                        insert.setInt(39, rs.getInt("num_valid_checkpoint_submissions"));
                        insert.setDouble(40, rs.getDouble("total_prize"));


                        setLongParameter(rs, insert, "challenge_manager", 41);
                        setLongParameter(rs, insert, "challenge_creator", 42);
                        setLongParameter(rs, insert, "challenge_launcher", 43);
                        setLongParameter(rs, insert, "copilot", 44);


                        if (checkpointStartDate != null) {
                            insert.setTimestamp(45, checkpointStartDate);
                        } else {
                            insert.setNull(45, Types.TIMESTAMP);
                        }

                        if (checkpointEndDate != null) {
                            insert.setTimestamp(46, checkpointEndDate);
                        } else {
                            insert.setNull(46, Types.TIMESTAMP);
                        }

                        setTimestampParameter(rs, insert, "registration_end_date", 47);
                        setTimestampParameter(rs, insert, "scheduled_end_date", 48);
                        insert.setDouble(49, rs.getDouble("checkpoint_prize_amount"));
                        insert.setDouble(50, rs.getDouble("checkpoint_prize_number"));
                        insert.setDouble(51, rs.getDouble("dr_points"));
                        insert.setDouble(52, rs.getDouble("reliability_cost"));
                        insert.setDouble(53, rs.getDouble("review_cost"));

                        setLongParameter(rs, insert, "forum_id", 54);
                        setLongParameter(rs, insert, "submission_viewable", 55);
                        setLongParameter(rs, insert, "is_private", 56);

                        insert.setDouble(57, rs.getDouble("estimated_reliability_cost"));
                        insert.setDouble(58, rs.getDouble("estimated_review_cost"));
                        insert.setDouble(59, rs.getDouble("estimated_copilot_cost"));
                        insert.setDouble(60, rs.getDouble("estimated_admin_fee"));
                        insert.setDouble(61, rs.getDouble("actual_total_prize"));
                        insert.setDouble(62, rs.getDouble("copilot_cost"));
                        insert.setInt(63, rs.getInt("task_ind"));

                        insert.executeUpdate();
                    }
                } else {
                    // we need to delete this project and all related objects in the database.
                    log.info("Found project to delete: " + rs.getLong("project_id"));
                    deleteProject(rs.getLong("project_id"));
                }
                count++;
            }

            // update the start_date_calendar_id, duration, fulfillment fields
            updateAgain.executeUpdate();
            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");

        } catch (SQLException sqle) {
            sqle.printStackTrace();
            DBMS.printSqlException(true, sqle);
            log.error("Load of 'project_result / project' table failed.", sqle);
            throw new Exception("Load of 'project_result / project' table failed.\n" +
                    sqle.getMessage());
        } finally {
            close(rs);
            close(select);
            close(insert);
            close(update);
            close(updateAgain);
        }
    }

    /**
     * Loads the new columns (challenge_manager, challenge_creator, challenge_launcher, copilot, checkpoint_start_date,
     * checkpoint_end_date) data for the existing tcs_dw:project records.
     *
     * @throws Exception if any error.
     * @since 1.3
     */
    private void loadNewColumnsForProjectFirstTime() throws Exception {
        PreparedStatement countChallengeCreatorPS = null;
        PreparedStatement selectExistingProjectToUpdatePS = null;
        PreparedStatement selectNewColumnsDataPS = null;
        PreparedStatement updateProjectPS = null;


        ResultSet rs = null;
        ResultSet projectDataRS = null;

        StringBuffer query = null;
        int totalCount = 0;
        long projectId = -1;


        try {

            query = new StringBuffer(100);
            // check if there're existing any records in tcs_dw:project which have challenge_creator populated
            query.append("SELECT count(*) from project WHERE challenge_creator IS NOT NULL");
            countChallengeCreatorPS = prepareStatement(query.toString(), TARGET_DB);

            rs = countChallengeCreatorPS.executeQuery();
            rs.next();

            boolean firstRun = (rs.getInt(1) == 0);

            if(firstRun) {

                log.info("Start to do the first full load of challenge_manager, challenge_creator, challenge_launcher, copilot, checkpoint_start_date and checkpoint_end_date for the existing DW project records");

                // load project_id for the existing project records in tcs_dw
                query.delete(0, query.length());
                query.append("SELECT project_id FROM project");
                selectExistingProjectToUpdatePS = prepareStatement(query.toString(), TARGET_DB);
                rs = selectExistingProjectToUpdatePS.executeQuery();

                // query to get challenge_manager, challenge_creator, challenge_launcher, copilot,
                // checkpoint_start_date and checkpoint_end_date data to update from source DB
                String selectNewColumnsDataSQL = "SELECT (SELECT u.user_id\n" +
                        "    FROM resource r,\n" +
                        "      resource_info ri,\n" +
                        "      user u\n" +
                        "    WHERE r.project_id = p.project_id AND r.\n" +
                        "      resource_id = ri.resource_id AND ri.\n" +
                        "      resource_info_type_id = 1 AND r.\n" +
                        "      resource_role_id = 13 AND ri.value = u.\n" +
                        "      user_id AND r.resource_id = (\n" +
                        "        SELECT min(r2.resource_id)\n" +
                        "        FROM resource_info ri2,\n" +
                        "          resource r2\n" +
                        "        WHERE r2.project_id = p.project_id AND \n" +
                        "          r2.resource_id = ri2.resource_id \n" +
                        "          AND ri2.resource_info_type_id = 1 \n" +
                        "          AND r2.resource_role_id = 13 AND \n" +
                        "          ri2.value NOT IN (22770213, 22719217\n" +
                        "            )\n" +
                        "        )\n" +
                        "    )::lvarchar AS challenge_manager,\n" +
                        "  (\n" +
                        "    SELECT u.user_id\n" +
                        "    FROM project p2,\n" +
                        "      user u\n" +
                        "    WHERE p2.create_user = u.user_id AND p2.\n" +
                        "      project_id = p.project_id\n" +
                        "    ) AS challenge_creator,\n" +
                        "  (\n" +
                        "    SELECT u.user_id\n" +
                        "    FROM project_info pi58,\n" +
                        "      user u\n" +
                        "    WHERE pi58.project_id = p.project_id AND pi58.\n" +
                        "      project_info_type_id = 58 AND pi58.value::\n" +
                        "      INT = u.user_id\n" +
                        "    ) AS challenge_launcher,\n" +
                        "  (\n" +
                        "    SELECT u.user_id\n" +
                        "    FROM resource r,\n" +
                        "      user u\n" +
                        "    WHERE r.project_id = p.project_id AND r.user_id \n" +
                        "      = u.user_id AND r.resource_role_id = 14 AND r\n" +
                        "      .resource_id = (\n" +
                        "        SELECT min(r2.resource_id)\n" +
                        "        FROM resource r2\n" +
                        "        WHERE r2.project_id = p.project_id AND \n" +
                        "          r2.resource_role_id = 14\n" +
                        "        )\n" +
                        "    ) AS copilot,\n" +
                        "  CASE WHEN pcsd.actual_start_time IS NOT NULL THEN \n" +
                        "        pcsd.actual_start_time ELSE pcsd.\n" +
                        "      scheduled_start_time END AS \n" +
                        "  checkpoint_start_date,\n" +
                        "  CASE WHEN pced.actual_end_time IS NOT NULL THEN pced.\n" +
                        "        actual_end_time ELSE pced.\n" +
                        "      scheduled_end_time END AS \n" +
                        "  checkpoint_end_date\n" +
                        "from project p, OUTER project_phase pcsd, OUTER project_phase pced\n" +
                        "WHERE pcsd.project_id = p.project_id and pcsd.phase_type_id = 15 and pced.project_id = p.project_id and pced.phase_type_id = 17\n" +
                        "and p.project_id = ?";
                selectNewColumnsDataPS = prepareStatement(selectNewColumnsDataSQL, SOURCE_DB);

                // query to update the existing payment records in topcoder_dw
                query.delete(0, query.length());
                query.append("UPDATE project SET challenge_manager = ?, challenge_creator = ?, challenge_launcher = ?, copilot = ?, checkpoint_start_date = ?, checkpoint_end_date = ?  WHERE project_id = ?");
                updateProjectPS = prepareStatement(query.toString(), TARGET_DB);

                while (rs.next()) {
                    projectId = rs.getLong(1);

                    selectNewColumnsDataPS.clearParameters();
                    selectNewColumnsDataPS.setLong(1, projectId);
                    projectDataRS = selectNewColumnsDataPS.executeQuery();
                    boolean hasNext = projectDataRS.next();

                    if(!hasNext) continue;

                    updateProjectPS.clearParameters();

                    setLongParameter(projectDataRS, updateProjectPS, "challenge_manager", 1);
                    setLongParameter(projectDataRS, updateProjectPS, "challenge_creator", 2);
                    setLongParameter(projectDataRS, updateProjectPS, "challenge_launcher", 3);
                    setLongParameter(projectDataRS, updateProjectPS, "copilot", 4);

                    Timestamp checkpointStartDate = projectDataRS.getTimestamp("checkpoint_start_date");
                    Timestamp checkpointEndDate = projectDataRS.getTimestamp("checkpoint_end_date");

                    if (checkpointStartDate != null) {
                        updateProjectPS.setTimestamp(5, checkpointStartDate);
                    } else {
                        updateProjectPS.setNull(5, Types.TIMESTAMP);
                    }

                    if (checkpointEndDate != null) {
                        updateProjectPS.setTimestamp(6, checkpointEndDate);
                    } else {
                        updateProjectPS.setNull(6, Types.TIMESTAMP);
                    }

                    updateProjectPS.setLong(7, projectId);

                    int countUpdated = updateProjectPS.executeUpdate();

                    if (countUpdated == 1) {
                        log.info(String.format("Update Project %s with new columns data", projectId));
                        totalCount++;
                    }
                }
            }

            log.info("total project records updated with new columns data = " + totalCount);
        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Full Load of new columns data for existing records in 'project' table failed.\n"
                    + "project_id = " + projectId + "\n" + sqle.getMessage());
        } finally {
            DBMS.close(projectDataRS);
            DBMS.close(rs);
            DBMS.close(countChallengeCreatorPS);
            DBMS.close(selectExistingProjectToUpdatePS);
            DBMS.close(selectNewColumnsDataPS);
            DBMS.close(updateProjectPS);
        }
    }

    /**
     * Loads the new columns (registration_end_date, scheduled_end_date, checkpoint_prize_amount,
     * checkpoint_prize_number, dr_points, reliability_cost, review_cost, forum_id,
     * submission_viewable, is_private) data for the existing tcs_dw:project records.
     *
     * @throws Exception if any error.
     * @since 1.4
     */
    private void loadNewColumns2ForProjectFirstTime() throws Exception {
        PreparedStatement countChallengeCreatorPS = null;
        PreparedStatement selectExistingProjectToUpdatePS = null;
        PreparedStatement selectNewColumnsDataPS = null;
        PreparedStatement updateProjectPS = null;


        ResultSet rs = null;
        ResultSet projectDataRS = null;

        StringBuffer query = null;
        int totalCount = 0;
        long projectId = -1;


        try {

            query = new StringBuffer(100);
            // check if there're existing any records in tcs_dw:project which have review_cost populated
            query.append("SELECT count(*) from project WHERE review_cost IS NOT NULL");
            countChallengeCreatorPS = prepareStatement(query.toString(), TARGET_DB);

            rs = countChallengeCreatorPS.executeQuery();
            rs.next();

            boolean firstRun = (rs.getInt(1) == 0);

            if(firstRun) {

                log.info("Start to do the first full load of (registration_end_date, scheduled_end_date, " +
                        "checkpoint_prize_amount, checkpoint_prize_number, dr_points, " +
                        "reliability_cost, review_cost, forum_id, submission_viewable, is_private)" +
                        "for the existing DW project records");

                // load project_id for the existing project records in tcs_dw
                query.delete(0, query.length());
                query.append("SELECT project_id FROM project");
                selectExistingProjectToUpdatePS = prepareStatement(query.toString(), TARGET_DB);
                rs = selectExistingProjectToUpdatePS.executeQuery();

                // query to get (registration_end_date, scheduled_end_date, checkpoint_prize_amount, checkpoint_prize_number,
                // dr_points, reliability_cost, review_cost, forum_id, submission_viewable, is_private)  data to update from source DB
                String selectNewColumnsDataSQL = "SELECT (select CASE when pi53.value == 'true' THEN 1 ELSE 0 END FROM project_info pi53 where pi53.project_info_type_id = 53 and pi53.project_id = p.project_id) as submission_viewable\n" +
                        ", NVL(ppd.actual_end_time, ppd.scheduled_end_time) as registration_end_date\n" +
                        ",(SELECT MAX(scheduled_end_time) FROM project_phase phet WHERE phet.project_id = p.project_id) as scheduled_end_date\n" +
                        ", NVL((SELECT SUM(pr.number_of_submissions) FROM prize pr WHERE pr.project_id = p.project_id AND pr.prize_type_id = 14), 0) AS checkpoint_prize_number\n" +
                        ", (SELECT NVL(MAX(prize_amount), 0) FROM prize pr WHERE pr.project_id = p.project_id AND pr.prize_type_id = 14) AS checkpoint_prize_amount\n" +
                        ",(CASE WHEN pict.value = 'On' THEN NVL((SELECT value::decimal FROM project_info pidr WHERE pidr.project_id = p.project_id AND pidr.project_info_type_id = 30), 0) ELSE 0 END) as dr_points\n" +
                        ",(NVL((SELECT sum(total_amount)         FROM  informixoltp:payment_detail pmd, informixoltp:payment pm          \n" +
                        "WHERE pmd.component_project_id = p.project_id and pmd.installment_number = 1         \n" +
                        " and pm.most_recent_detail_id = pmd.payment_detail_id and pmd.payment_type_id = 24         \n" +
                        " AND NOT pmd.payment_status_id IN (65, 68, 69)), 0)  +    NVL((SELECT sum(pmd2.total_amount)            \n" +
                        "  FROM  informixoltp:payment_detail pmd,   \n" +
                        "informixoltp:payment pm LEFT OUTER JOIN informixoltp:payment_detail pmd2 on pm.payment_id = pmd2.parent_payment_id,    \n" +
                        "informixoltp:payment pm2  \n" +
                        "WHERE pmd.component_project_id = p.project_id and pmd2.installment_number = 1  \n" +
                        "  and pm.most_recent_detail_id = pmd.payment_detail_id   \n" +
                        "and pm2.most_recent_detail_id = pmd2.payment_detail_id and pmd2.payment_type_id = 24 \n" +
                        "AND NOT pmd2.payment_status_id IN (65, 68, 69)), 0)) as reliability_cost\n" +
                        ",(NVL((SELECT sum(total_amount)         FROM  informixoltp:payment_detail pmd, informixoltp:payment pm  \n" +
                        " WHERE pmd.component_project_id = p.project_id and pmd.installment_number = 1  \n" +
                        "and pm.most_recent_detail_id = pmd.payment_detail_id and pmd.payment_type_id IN (7,26,28,36)\n" +
                        " AND NOT pmd.payment_status_id IN (65, 68, 69)), 0)  +    NVL((SELECT sum(pmd2.total_amount)  \n" +
                        "  FROM  informixoltp:payment_detail pmd,   \n" +
                        "informixoltp:payment pm LEFT OUTER JOIN informixoltp:payment_detail pmd2 on pm.payment_id = pmd2.parent_payment_id,   \n" +
                        " informixoltp:payment pm2               WHERE pmd.component_project_id = p.project_id and pmd2.installment_number = 1  \n" +
                        "and pm.most_recent_detail_id = pmd.payment_detail_id   \n" +
                        " and pm2.most_recent_detail_id = pmd2.payment_detail_id and pmd2.payment_type_id IN (7,26,28,36)  \n" +
                        " AND NOT pmd2.payment_status_id IN (65, 68, 69)), 0)) as review_cost\n" +
                        ",(SELECT value::INTEGER FROM project_info piforum WHERE piforum.project_id = p.project_id and piforum.project_info_type_id = 4) as forum_id\n" +
                        ", (select CASE when pi53.value == 'true' THEN 1 ELSE 0 END FROM project_info pi53 where pi53.project_info_type_id = 53 and pi53.project_id = p.project_id) as submission_viewable\n" +
                        ", NVL((SELECT MAX(1) FROM contest_eligibility WHERE contest_id = p.project_id), 0) AS is_private\n" +
                        "FROM project p,\n" +
                        "OUTER project_phase ppd,\n" +
                        "OUTER project_info pict\n" +
                        "WHERE ppd.project_id = p.project_id and ppd.phase_type_id = 1 and p.project_status_id = 7\n" +
                        "AND pict.project_id = p.project_id and pict.project_info_type_id = 26 and p.project_id = ?";

                selectNewColumnsDataPS = prepareStatement(selectNewColumnsDataSQL, SOURCE_DB);

                // query to update the existing payment records in topcoder_dw
                query.delete(0, query.length());
                query.append("UPDATE project SET registration_end_date = ?, scheduled_end_date = ?, checkpoint_prize_amount = ?, checkpoint_prize_number = ?, dr_points = ?, " +
                        "reliability_cost = ?, review_cost = ?, forum_id = ?, submission_viewable = ?, is_private = ?  WHERE project_id = ?");
                updateProjectPS = prepareStatement(query.toString(), TARGET_DB);

                while (rs.next()) {
                    projectId = rs.getLong(1);

                    selectNewColumnsDataPS.clearParameters();
                    selectNewColumnsDataPS.setLong(1, projectId);
                    projectDataRS = selectNewColumnsDataPS.executeQuery();
                    boolean hasNext = projectDataRS.next();

                    if(!hasNext) continue;

                    updateProjectPS.clearParameters();

                    setTimestampParameter(projectDataRS,   updateProjectPS, "registration_end_date", 1);
                    setTimestampParameter(projectDataRS,   updateProjectPS, "scheduled_end_date", 2);
                    updateProjectPS.setDouble(3, projectDataRS.getDouble("checkpoint_prize_amount"));
                    updateProjectPS.setDouble(4, projectDataRS.getDouble("checkpoint_prize_number"));
                    updateProjectPS.setDouble(5, projectDataRS.getDouble("dr_points"));
                    updateProjectPS.setDouble(6, projectDataRS.getDouble("reliability_cost"));
                    updateProjectPS.setDouble(7, projectDataRS.getDouble("review_cost"));

                    setLongParameter(projectDataRS,   updateProjectPS, "forum_id", 8);
                    setLongParameter(projectDataRS,   updateProjectPS, "submission_viewable", 9);
                    setLongParameter(projectDataRS,   updateProjectPS, "is_private", 10);

                    updateProjectPS.setLong(11, projectId);

                    int countUpdated = updateProjectPS.executeUpdate();

                    if (countUpdated == 1) {
                        log.info(String.format("Update Project %s with new columns data", projectId));
                        totalCount++;
                    }
                }
            }

            log.info("total project records updated with new columns data = " + totalCount);
        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Full Load of new columns data for existing records in 'project' table failed.\n"
                    + "project_id = " + projectId + "\n" + sqle.getMessage());
        } finally {
            DBMS.close(projectDataRS);
            DBMS.close(rs);
            DBMS.close(countChallengeCreatorPS);
            DBMS.close(selectExistingProjectToUpdatePS);
            DBMS.close(selectNewColumnsDataPS);
            DBMS.close(updateProjectPS);
        }
    }

    /**
     * Loads the new columns (estimated_reliability_cost, estimated_review_cost, estimated_copilot_cost, estimated_admin_fee, actual_total_prize,  copilot_cost)
     * data for the existing tcs_dw:project records.
     *
     * @throws Exception if any error.
     * @since 1.4.2
     */
    private void loadNewColumns3ForProjectFirstTime() throws Exception {
        PreparedStatement countEstimatedReviewCostPS = null;
        PreparedStatement selectExistingProjectToUpdatePS = null;
        PreparedStatement selectNewColumnsDataPS = null;
        PreparedStatement updateProjectPS = null;


        ResultSet rs = null;
        ResultSet projectDataRS = null;

        StringBuffer query = null;
        int totalCount = 0;
        long projectId = -1;


        try {

            query = new StringBuffer(100);
            // check if there're existing any records in tcs_dw:project which have estimated_review_cost populated
            query.append("SELECT count(*) from project WHERE estimated_review_cost IS NOT NULL");
            countEstimatedReviewCostPS = prepareStatement(query.toString(), TARGET_DB);

            rs = countEstimatedReviewCostPS.executeQuery();
            rs.next();

            boolean firstRun = (rs.getInt(1) == 0);

            if(firstRun) {

                log.info("Start to do the first full load of (estimated_reliability_cost, estimated_review_cost, " +
                        " estimated_copilot_cost, estimated_admin_fee, actual_total_prize,  copilot_cost)");

                // load project_id for the existing project records in tcs_dw
                query.delete(0, query.length());
                query.append("SELECT project_id FROM project WHERE project_category_id IN " + LOAD_CATEGORIES);
                selectExistingProjectToUpdatePS = prepareStatement(query.toString(), TARGET_DB);
                rs = selectExistingProjectToUpdatePS.executeQuery();

                // query to get (estimated_reliability_cost, estimated_review_cost, estimated_copilot_cost,
                // estimated_admin_fee, actual_total_prize,  copilot_cost)  data to update from source DB
                String selectNewColumnsDataSQL = "SELECT (\n" +
                        "    CASE WHEN pire.value = 'true' THEN NVL((\n" +
                        "              SELECT value::DECIMAL\n" +
                        "              FROM project_info pi38\n" +
                        "              WHERE pi38.project_id = p\n" +
                        "                .project_id AND pi38\n" +
                        "                .\n" +
                        "                project_info_type_id \n" +
                        "                = 38\n" +
                        "              ), 0) ELSE 0 END\n" +
                        "    ) AS estimated_reliability_cost,\n" +
                        "  NVL((\n" +
                        "      SELECT value::DECIMAL\n" +
                        "      FROM project_info pi33\n" +
                        "      WHERE pi33.project_id = p.project_id AND \n" +
                        "        pi33.project_info_type_id = 33\n" +
                        "      ), 0) AS estimated_review_cost,\n" +
                        "  NVL((\n" +
                        "      SELECT value::DECIMAL\n" +
                        "      FROM project_info pi49\n" +
                        "      WHERE pi49.project_id = p.project_id AND \n" +
                        "        pi49.project_info_type_id = 49\n" +
                        "      ), 0) AS estimated_copilot_cost,\n" +
                        "  NVL((\n" +
                        "      SELECT value::DECIMAL\n" +
                        "      FROM project_info pi31\n" +
                        "      WHERE pi31.project_id = p.project_id AND \n" +
                        "        pi31.project_info_type_id = 31\n" +
                        "      ), 0) AS estimated_admin_fee,\n" +
                        "  (\n" +
                        "    NVL((\n" +
                        "        SELECT sum(total_amount)\n" +
                        "        FROM informixoltp: payment_detail pmd\n" +
                        "          ,\n" +
                        "          informixoltp: payment pm\n" +
                        "        WHERE pmd.component_project_id = p.\n" +
                        "          project_id AND pmd.\n" +
                        "          installment_number = 1 AND pm.\n" +
                        "          most_recent_detail_id = pmd.\n" +
                        "          payment_detail_id AND pmd.\n" +
                        "          payment_type_id IN (\n" +
                        "            6, 29, 10, 42, 43, 44, 49, 50, 51, \n" +
                        "            55, 61, 64, 65, 60, 13, 21\n" +
                        "            ) AND NOT pmd.\n" +
                        "          payment_status_id IN (65, 68, 69\n" +
                        "            )\n" +
                        "        ), 0) + NVL((\n" +
                        "        SELECT sum(pmd2.total_amount)\n" +
                        "        FROM informixoltp: payment_detail pmd\n" +
                        "          ,\n" +
                        "          informixoltp: payment pm\n" +
                        "        LEFT JOIN informixoltp: \n" +
                        "          payment_detail pmd2 ON pm.\n" +
                        "          payment_id = pmd2.\n" +
                        "          parent_payment_id,\n" +
                        "          informixoltp: payment pm2\n" +
                        "        WHERE pmd.component_project_id = p.\n" +
                        "          project_id AND pmd2.\n" +
                        "          installment_number = 1 AND pm.\n" +
                        "          most_recent_detail_id = pmd.\n" +
                        "          payment_detail_id AND pm2.\n" +
                        "          most_recent_detail_id = pmd2.\n" +
                        "          payment_detail_id AND pmd2.\n" +
                        "          payment_type_id IN (\n" +
                        "            6, 29, 10, 42, 43, 44, 49, 50, 51, \n" +
                        "            55, 61, 64, 65, 60, 13, 21\n" +
                        "            ) AND NOT pmd2.\n" +
                        "          payment_status_id IN (65, 68, 69\n" +
                        "            )\n" +
                        "        ), 0)\n" +
                        "    ) AS actual_total_prize,\n" +
                        "  (\n" +
                        "    NVL((\n" +
                        "        SELECT sum(total_amount)\n" +
                        "        FROM informixoltp: payment_detail pmd\n" +
                        "          ,\n" +
                        "          informixoltp: payment pm\n" +
                        "        WHERE pmd.component_project_id = p.\n" +
                        "          project_id AND pmd.\n" +
                        "          installment_number = 1 AND pm.\n" +
                        "          most_recent_detail_id = pmd.\n" +
                        "          payment_detail_id AND pmd.\n" +
                        "          payment_type_id IN (45, 57\n" +
                        "            ) AND NOT pmd.\n" +
                        "          payment_status_id IN (65, 68, 69\n" +
                        "            )\n" +
                        "        ), 0) + NVL((\n" +
                        "        SELECT sum(pmd2.total_amount)\n" +
                        "        FROM informixoltp: payment_detail pmd\n" +
                        "          ,\n" +
                        "          informixoltp: payment pm\n" +
                        "        LEFT JOIN informixoltp: \n" +
                        "          payment_detail pmd2 ON pm.\n" +
                        "          payment_id = pmd2.\n" +
                        "          parent_payment_id,\n" +
                        "          informixoltp: payment pm2\n" +
                        "        WHERE pmd.component_project_id = p.\n" +
                        "          project_id AND pmd2.\n" +
                        "          installment_number = 1 AND pm.\n" +
                        "          most_recent_detail_id = pmd.\n" +
                        "          payment_detail_id AND pm2.\n" +
                        "          most_recent_detail_id = pmd2.\n" +
                        "          payment_detail_id AND pmd2.\n" +
                        "          payment_type_id IN (45, 57\n" +
                        "            ) AND NOT pmd2.\n" +
                        "          payment_status_id IN (65, 68, 69\n" +
                        "            )\n" +
                        "        ), 0)\n" +
                        "    ) AS copilot_cost\n" +
                        "FROM project p, \n" +
                        "OUTER project_info pire\n" +
                        "WHERE \n" +
                        "pire.project_id = p.project_id\n" +
                        "AND pire.project_info_type_id = 45\n" +
                        "AND p.project_id = ?";

                selectNewColumnsDataPS = prepareStatement(selectNewColumnsDataSQL, SOURCE_DB);

                // query to update the existing payment records in topcoder_dw
                query.delete(0, query.length());
                query.append("UPDATE project SET estimated_reliability_cost = ?, estimated_review_cost = ?, estimated_copilot_cost = ?, estimated_admin_fee = ?, actual_total_prize = ?, " +
                        "copilot_cost = ? WHERE project_id = ?");
                updateProjectPS = prepareStatement(query.toString(), TARGET_DB);

                while (rs.next()) {
                    projectId = rs.getLong(1);

                    selectNewColumnsDataPS.clearParameters();
                    selectNewColumnsDataPS.setLong(1, projectId);
                    projectDataRS = selectNewColumnsDataPS.executeQuery();
                    boolean hasNext = projectDataRS.next();

                    if(!hasNext) continue;

                    updateProjectPS.clearParameters();

                    updateProjectPS.setDouble(1, projectDataRS.getDouble("estimated_reliability_cost"));
                    updateProjectPS.setDouble(2, projectDataRS.getDouble("estimated_review_cost"));
                    updateProjectPS.setDouble(3, projectDataRS.getDouble("estimated_copilot_cost"));
                    updateProjectPS.setDouble(4, projectDataRS.getDouble("estimated_admin_fee"));
                    updateProjectPS.setDouble(5, projectDataRS.getDouble("actual_total_prize"));
                    updateProjectPS.setDouble(6, projectDataRS.getDouble("copilot_cost"));

                    updateProjectPS.setLong(7, projectId);

                    int countUpdated = updateProjectPS.executeUpdate();

                    if (countUpdated == 1) {
                        log.info(String.format("Update Project %s with new columns data", projectId));
                        totalCount++;
                    }
                }
            }

            log.info("total project records updated with new columns data = " + totalCount);
        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Full Load of new columns data for existing records in 'project' table failed.\n"
                    + "project_id = " + projectId + "\n" + sqle.getMessage());
        } finally {
            DBMS.close(projectDataRS);
            DBMS.close(rs);
            DBMS.close(countEstimatedReviewCostPS);
            DBMS.close(selectExistingProjectToUpdatePS);
            DBMS.close(selectNewColumnsDataPS);
            DBMS.close(updateProjectPS);
        }
    }

    /**
     * <p/>
     * Calculates stage based on a date.
     * </p>
     *
     * @param date The date used to calculate the stage.
     * @return the stage ID.
     * @since 1.1.0
     */
    private long calculateStage(java.sql.Date date) throws Exception {
        PreparedStatement select = null;
        ResultSet rs = null;

        try {
            //get data from source DB
            final String SELECT = "select " +
                    "   stage_id " +
                    "from " +
                    "   stage s, calendar c1, calendar c2 " +
                    "where " +
                    "   s.start_calendar_id = c1.calendar_id and " +
                    "   s.end_calendar_id = c2.calendar_id and " +
                    "   c1.date <= DATE(?) and " +
                    "   c2.date >= DATE(?)";

            select = prepareStatement(SELECT, TARGET_DB);
            select.setDate(1, date);
            select.setDate(2, date);

            rs = select.executeQuery();
            if (!rs.next()) {
                throw new Exception("Stage calculation failed for date: " + date.toString() + ". (no stage found)");
            }

            //log.debug("Date " + date.toString() + " has been assigned stageId = " + rs.getLong("stage_id"));
            return (rs.getLong("stage_id"));

        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Stage calculation failed for date: " + date.toString() + ".\n" + sqle.getMessage());
        } finally {
            close(rs);
            close(select);
        }
    }

    private void setTimestampParameter(ResultSet rs, PreparedStatement statement, String name, int index) throws SQLException {
        if (rs.getObject(name) != null) {
            statement.setTimestamp(index, rs.getTimestamp(name));
        } else {
            statement.setNull(index, Types.TIMESTAMP);
        }
    }

    private void setLongParameter(ResultSet rs, PreparedStatement statement, String name, int index) throws SQLException {
        if (rs.getObject(name) != null) {
            statement.setLong(index, rs.getLong(name));
        } else {
            statement.setNull(index, Types.DECIMAL);
        }
    }

}
