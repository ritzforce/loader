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


public class TCLoadStreak extends TCLoadTCS {

    private static Logger log = Logger.getLogger(TCLoadStreak.class);

    @Override
    public void performLoad() throws Exception {
        doLoadStreak();
    }

    private void doLoadStreak() throws Exception {
        log.info("load streak");
        ResultSet rs;

        PreparedStatement select = null;
        PreparedStatement insert = null;
        PreparedStatement delete = null;

        final String DELETE = "delete from streak";

        final String SELECT = "select pr.user_id " +
                " , p.project_id " +
                " , pr.placed " +
                "  , round(pr.new_rating) as new_rating  " +
                "  , p.phase_id  " +
                "  , p.category_desc  " +
                " from project_result pr  " +
                "    , project p  " +
                " where p.project_id = pr.project_id  " +
                " and pr.valid_submission_ind = 1  " +
                " and pr.rating_ind = 1  " +
                " and p.status_id in (4,5,7)  " +
                " and p.phase_id in (112,113)  " +
                " order by pr.user_id, pr.rating_order";

        final String INSERT = "INSERT INTO streak (coder_id, streak_type_id, phase_id, start_project_id, end_project_id, length, is_current) " +
                " VALUES(?,?,?,?,?,?,?)";

        int phases[] = {112, 113};
        Streak[][] streaks = new Streak[4][phases.length];

        for (int i = 0; i < phases.length; i++) {
            streaks[0][i] = new ConsecutiveWinningsStreak(phases[i]);
            streaks[1][i] = new ConsecutivePaidStreak(phases[i]);
            streaks[2][i] = new ConsecutiveRatingIncrease(phases[i]);
            streaks[3][i] = new ConsecutiveRatingDecrease(phases[i]);
        }

        try {

            long start = System.currentTimeMillis();
            delete = prepareStatement(DELETE, TARGET_DB);
            delete.executeUpdate();

            select = prepareStatement(SELECT, TARGET_DB);
            insert = prepareStatement(INSERT, TARGET_DB);

            int count = 0;

            rs = select.executeQuery();

            long userId = 0;
            long projectId = 0;
            int placed = 0;
            int rating = 0;
            int phaseId = 0;
            String category = null;

            boolean hasNext = true;

            while (hasNext) {
                hasNext = rs.next();

                if (hasNext) {
                    userId = rs.getLong("user_id");
                    projectId = rs.getLong("project_id");
                    placed = rs.getInt("placed");
                    rating = rs.getInt("new_rating");
                    phaseId = rs.getInt("phase_id");
                    category = rs.getString("category_desc");
                }

                for (int k = 0; k < 4; k++) {
                    for (int i = 0; i < phases.length; i++) {
                        StreakRow sr = hasNext ? streaks[k][i].add(userId, projectId, placed, rating, phaseId, category) : streaks[k][i].flush();

                        if (sr != null) {
//                          log.debug("Save coder=" + sr.getCoderId() + " type= " + sr.getTypeId() + " length=" + sr.getLength());
                            insert.setLong(1, sr.getCoderId());
                            insert.setInt(2, sr.getTypeId());
                            insert.setInt(3, sr.getPhaseId());
                            insert.setLong(4, sr.getStartProjectId());
                            insert.setLong(5, sr.getEndProjectId());
                            insert.setInt(6, sr.getLength());
                            insert.setInt(7, sr.isCurrent() ? 1 : 0);

                            insert.executeUpdate();
                            count++;
                        }

                    }
                }
            }

            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");


        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of 'streak' table failed.\n" +
                    sqle.getMessage());
        } finally {
            close(insert);
            close(select);
            close(delete);
        }
    }

    /**
     * Represents a Streak of rating or placement.
     *
     * @author Cucu
     */
    private static abstract class Streak {
        public static final String OVERALL = "Overall";

        public static final int CONSECUTIVE_WINNING = 1;
        public static final int CONSECUTIVE_PAID = 2;
        public static final int CONSECUTIVE_RATING_INCREASE = 3;
        public static final int CONSECUTIVE_RATING_DECREASE = 4;

        private int typeId;
        private int phaseId;
        private String category;

        protected long coderId = -1;
        private long startProjectId = -1;
        private long endProjectId = -1;
        private int length = 0;


        public Streak(int typeId, int phaseId, String category) {
            this.typeId = typeId;
            this.phaseId = phaseId;
            this.category = category;

        }

        public StreakRow flush() {
            return length > 1 ? new StreakRow(coderId, typeId, phaseId, category, startProjectId, endProjectId, length, true) : null;
        }

        public StreakRow add(long coderId, long projectId, int placed, int rating, int phaseId, String category) {
            StreakRow sr = null;

            if (this.coderId != coderId) {
                if (length > 1) {
                    sr = new StreakRow(this.coderId, typeId, this.phaseId, this.category, this.startProjectId, this.endProjectId, length, true);
                }
                length = 0;
                this.coderId = coderId;
                reset();
            }

            // check if the project belongs to the specified phase and category
            if (this.phaseId != phaseId ||
                    (!OVERALL.equals(this.category) && !this.category.equals(category))) {
                return sr;
            }

            if (addToStreak(placed, rating)) {
                if (length == 0) {
                    startProjectId = projectId;
                }
                endProjectId = projectId;
                length++;
            } else {
                if (length > 1) {
                    sr = new StreakRow(this.coderId, typeId, this.phaseId, this.category, startProjectId, endProjectId, length, false);
                }
                length = 0;
            }

            return sr;
        }


        protected abstract boolean addToStreak(int placed, int rating);

        protected void reset() {
        }
    }

    private static class ConsecutiveWinningsStreak extends Streak {

        public ConsecutiveWinningsStreak(int phaseId, String category) {
            super(CONSECUTIVE_WINNING, phaseId, category);
        }

        public ConsecutiveWinningsStreak(int phaseId) {
            this(phaseId, OVERALL);
        }

        protected boolean addToStreak(int placed, int rating) {
            return placed == 1;
        }
    }

    private static class ConsecutivePaidStreak extends Streak {

        public ConsecutivePaidStreak(int phaseId, String category) {
            super(CONSECUTIVE_PAID, phaseId, category);
        }

        public ConsecutivePaidStreak(int phaseId) {
            this(phaseId, OVERALL);
        }

        protected boolean addToStreak(int placed, int rating) {
            return placed == 1 || placed == 2;
        }
    }

    private static class ConsecutiveRatingIncrease extends Streak {
        private int currentRating = -1;

        public ConsecutiveRatingIncrease(int phaseId, String category) {
            super(CONSECUTIVE_RATING_INCREASE, phaseId, category);
        }

        public ConsecutiveRatingIncrease(int phaseId) {
            this(phaseId, OVERALL);
        }

        protected boolean addToStreak(int placed, int rating) {
            boolean accept = currentRating < 0 ? false : rating > currentRating;
            currentRating = rating;
            return accept;
        }

        protected void reset() {
            currentRating = -1;
        }

    }

    private static class ConsecutiveRatingDecrease extends Streak {
        private int currentRating = -1;

        public ConsecutiveRatingDecrease(int phaseId, String category) {
            super(CONSECUTIVE_RATING_DECREASE, phaseId, category);
        }

        public ConsecutiveRatingDecrease(int phaseId) {
            this(phaseId, OVERALL);
        }

        protected boolean addToStreak(int placed, int rating) {
            boolean accept = currentRating < 0 ? false : rating < currentRating;
            currentRating = rating;
            return accept;
        }

        protected void reset() {
            currentRating = -1;
        }
    }


    private static class StreakRow {
        long coderId;
        int typeId;
        int phaseId;
        String category;
        long startProjectId;
        long endProjectId;
        int length;
        boolean isCurrent;

        public StreakRow(long coderId, int typeId, int phaseId, String category, long startProjectId, long endProjectId, int length, boolean isCurrent) {
            super();
            this.coderId = coderId;
            this.typeId = typeId;
            this.phaseId = phaseId;
            this.category = category;
            this.startProjectId = startProjectId;
            this.endProjectId = endProjectId;
            this.length = length;
            this.isCurrent = isCurrent;
        }

        public String getCategory() {
            return category;
        }

        public long getCoderId() {
            return coderId;
        }

        public long getEndProjectId() {
            return endProjectId;
        }

        public boolean isCurrent() {
            return isCurrent;
        }

        public int getLength() {
            return length;
        }

        public int getPhaseId() {
            return phaseId;
        }

        public long getStartProjectId() {
            return startProjectId;
        }

        public int getTypeId() {
            return typeId;
        }
    }

}
