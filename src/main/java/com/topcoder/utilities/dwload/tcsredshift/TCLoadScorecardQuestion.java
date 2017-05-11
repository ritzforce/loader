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
import java.sql.Types;


public class TCLoadScorecardQuestion extends TCLoadTCSRedshift {

    private static Logger log = Logger.getLogger(TCLoadScorecardQuestion.class);

    @Override
    public void performLoad() throws Exception {
        doLoadScorecardQuestion();
    }

    private void doLoadScorecardQuestion() throws Exception {
        log.info("load scorecard_question");
        ResultSet rs;

        PreparedStatement select = null;
        PreparedStatement update = null;
        PreparedStatement insert = null;


        final String SELECT =
                "select qt.scorecard_question_id " +
                        "   ,sg.scorecard_id as scorecard_template_id " +
                        "   ,qt.description || qt.guideline as question_text " +
                        "   ,qt.weight as question_weight " +
                        "   ,qt.scorecard_section_id as section_id " +
                        "   ,ss.name as section_desc " +
                        "   ,(ss.weight*sg.weight/100) as section_weight " +
                        "   ,ss.scorecard_group_id as section_group_id " +
                        "   ,sg.name as section_group_desc " +
                        "   ,(sg.sort + 1) || '.' || (ss.sort + 1) || '.' || (qt.sort + 1)  as question_desc " +
                        "   ,sg.sort + 1 as group_seq_loc " +
                        "   ,ss.sort + 1 as section_seq_loc  " +
                        "   ,qt.sort + 1 as question_seq_loc " +
                        "from scorecard_question qt," +
                        "   scorecard_section ss," +
                        "   scorecard_group sg " +
                        "where qt.scorecard_section_id = ss.scorecard_section_id " +
                        "   and ss.scorecard_group_id = sg.scorecard_group_id " +
                        "   and (qt.modify_date > ? " +
                        "   or ss.modify_date > ? " +
                        "   or sg.modify_date > ? " +
                        (needLoadMovedProject() ? " OR qt.modify_user <> 'Converter' " +
                                " OR ss.modify_user <> 'Converter' " +
                                " OR sg.modify_user <> 'Converter' " +
                                ")"
                                : ")") +
                        "    order by scorecard_template_id, group_seq_loc, section_seq_loc, question_seq_loc ";

        final String UPDATE =
                "update scorecard_question set scorecard_template_id=?, question_text=?,question_weight=?, section_id=?,section_desc=?, " +
                        "section_weight=?, section_group_id=?, section_group_desc=?, question_desc=?, sort=?, question_header = ? " +
                        "where scorecard_question_id = ?";

        final String INSERT =
                "insert into scorecard_question (scorecard_template_id, question_text,question_weight, section_id, section_desc, " +
                        "section_weight, section_group_id, section_group_desc, question_desc, sort, question_header, scorecard_question_id)" +
                        "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";


        long questionId = 0;
        try {
            long start = System.currentTimeMillis();

            select = prepareStatement(SELECT, SOURCE_DB);
            select.setTimestamp(1, fLastLogTime);
            select.setTimestamp(2, fLastLogTime);
            select.setTimestamp(3, fLastLogTime);
            update = prepareStatement(UPDATE, TARGET_DB);
            insert = prepareStatement(INSERT, TARGET_DB);

            int count = 0;

            rs = select.executeQuery();

            long prevTemplate = -1;
            int sort = 0;

            while (rs.next()) {
                if (rs.getLong("scorecard_template_id") != prevTemplate) {
                    sort = 0;
                    prevTemplate = rs.getLong("scorecard_template_id");
                } else {
                    sort++;
                }

                String questionHeader = (String) rs.getObject("question_text");
                if (questionHeader != null) {
                    int p = questionHeader.lastIndexOf("\n", 250);

                    if (p >= 0) {
                        questionHeader = questionHeader.substring(0, p + 1) + "...";
                    } else {
                        p = questionHeader.lastIndexOf(" ", 250);
                        if (p >= 0) {
                            questionHeader = questionHeader.substring(0, p) + "...";
                        }
                    }

                }

                update.clearParameters();

                questionId = rs.getLong("scorecard_template_id");

                update.setLong(1, questionId);
                update.setObject(2, rs.getObject("question_text"));
                update.setObject(3, rs.getObject("question_weight"));
                update.setObject(4, rs.getObject("section_id"));
                update.setObject(5, rs.getObject("section_desc"));
                update.setObject(6, rs.getObject("section_weight"));
                update.setObject(7, rs.getObject("section_group_id"));
                update.setObject(8, rs.getObject("section_group_desc"));
                update.setObject(9, rs.getObject("question_desc"));
                update.setInt(10, sort);
                update.setObject(11, questionHeader);
                update.setLong(12, rs.getLong("scorecard_question_id"));

                int retVal = update.executeUpdate();

                if (retVal == 0) {
                    insert.clearParameters();

                    insert.setObject(1, rs.getObject("scorecard_template_id"));
                    insert.setObject(2, rs.getObject("question_text"));
                    insert.setObject(3, rs.getObject("question_weight"));
                    insert.setObject(4, rs.getObject("section_id"));
                    insert.setObject(5, rs.getObject("section_desc"));
                    insert.setObject(6, rs.getObject("section_weight"));
                    insert.setObject(7, rs.getObject("section_group_id"));
                    insert.setObject(8, rs.getObject("section_group_desc"));
                    insert.setObject(9, rs.getObject("question_desc"));
                    insert.setInt(10, sort);
                    insert.setObject(11, questionHeader);
                    insert.setLong(12, rs.getLong("scorecard_question_id"));

                    insert.executeUpdate();
                }
                count++;

            }

            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");


        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of 'scorecard_question' table failed on " + questionId + ".\n" +
                    sqle.getMessage());
        } finally {
            close(insert);
            close(update);
            close(select);
        }
    }

}
