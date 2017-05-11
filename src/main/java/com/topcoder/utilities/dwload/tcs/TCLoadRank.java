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
import java.util.*;


public class TCLoadRank extends TCLoadTCS {

    private static Logger log = Logger.getLogger(TCLoadRank.class);

    /**
     * <p>An <code>int</code> array representing all project categories that are currently being rated.
     * IF YOU CHANGE THIS LIST, YOU MUST ALSO UPDATE THE <code>getCurrentRatings</code> METHOD!</p>
     */
    private static final int[] RATED_CATEGORIES = new int[] {1, 2, 6, 7, 13, 14, 23, 26, 19, 24, 35, 36};

    private static final int OVERALL_RATING_RANK_TYPE_ID = 1;
    private static final int ACTIVE_RATING_RANK_TYPE_ID = 2;


    @Override
    public void performLoad() throws Exception {
        List<CoderRating> list = getCurrentRatings();

        for (int cat : RATED_CATEGORIES) {
            final int phase = cat + 111;
            doLoadRank(phase, ACTIVE_RATING_RANK_TYPE_ID, list);
            doLoadRank(phase, OVERALL_RATING_RANK_TYPE_ID, list);
            doLoadSchoolRatingRank(phase, ACTIVE_RATING_RANK_TYPE_ID, list);
            doLoadSchoolRatingRank(phase, OVERALL_RATING_RANK_TYPE_ID, list);
            doLoadCountryRatingRank(phase, ACTIVE_RATING_RANK_TYPE_ID, list);
            doLoadCountryRatingRank(phase, OVERALL_RATING_RANK_TYPE_ID, list);
        }
    }

    private void doLoadRank(int phaseId, int rankTypeId, List<CoderRating> list) throws Exception {
        log.info("load rank");
        StringBuffer query = null;
        PreparedStatement psDel = null;
        PreparedStatement psSel = null;
        PreparedStatement psIns = null;
        ResultSet rs = null;
        int count = 0;
        int coderCount = 0;

        try {

            long start = System.currentTimeMillis();
            query = new StringBuffer(100);
            query.append(" DELETE");
            query.append(" FROM user_rank");
            query.append(" WHERE phase_id = " + phaseId);
            query.append("  AND user_rank_type_id = " + rankTypeId);
            psDel = prepareStatement(query.toString(), TARGET_DB);

            query = new StringBuffer(100);
            query.append(" INSERT");
            query.append(" INTO user_rank (user_id, percentile, rank, phase_id, user_rank_type_id)");
            query.append(" VALUES (?, ?, ?, " + phaseId + ", " + rankTypeId + ")");
            psIns = prepareStatement(query.toString(), TARGET_DB);

            /* coder_rank table should be kept "up-to-date" so get the most recent stuff
             * from the rating table
             */
            ArrayList<CoderRating> ratings = new ArrayList<CoderRating>(list.size() / 2);
            CoderRating cr = null;
            for (int i = 0; i < list.size(); i++) {
                cr = list.get(i);
                if (cr.getPhaseId() == phaseId) {
                    if ((rankTypeId == ACTIVE_RATING_RANK_TYPE_ID && cr.isActive()) ||
                            rankTypeId != ACTIVE_RATING_RANK_TYPE_ID) {
                        ratings.add(cr);
                    }
                }
            }
            Collections.sort(ratings);

            coderCount = ratings.size();

            psDel.executeUpdate();

            int i = 0;
            int rating = 0;
            int rank = 0;
            int size = ratings.size();
            int tempRating;
            long tempCoderId;
            for (int j = 0; j < size; j++) {
                i++;
                tempRating = ((CoderRating) ratings.get(j)).getRating();
                tempCoderId = ((CoderRating) ratings.get(j)).getCoderId();
                if (tempRating != rating) {
                    rating = tempRating;
                    rank = i;
                }
                psIns.setLong(1, tempCoderId);
                psIns.setFloat(2, (float) 100 * ((float) (coderCount - rank) / coderCount));
                psIns.setInt(3, rank);
                count += psIns.executeUpdate();
            }
            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");


        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of 'user_rank' table failed for rating rank.\n" +
                    sqle.getMessage());
        } finally {
            close(rs);
            close(psSel);
            close(psIns);
            close(psDel);
        }

    }

    /**
     * Loads the school_coder_rank table with information about
     * rating rank within a school.
     */
    private void doLoadSchoolRatingRank(int phaseId, int rankTypeId, List<CoderRating> list) throws Exception {
        log.debug("loadSchoolRatingRank called...");
        StringBuffer query = null;
        PreparedStatement psDel = null;
        PreparedStatement psIns = null;
        ResultSet rs = null;
        int count = 0;
        int coderCount = 0;
        List<CoderRating> ratings = null;

        try {
            long start = System.currentTimeMillis();
            query = new StringBuffer(100);
            query.append(" DELETE");
            query.append(" FROM school_user_rank");
            query.append(" WHERE user_rank_type_id = " + rankTypeId);
            query.append(" and phase_id = " + phaseId);
            psDel = prepareStatement(query.toString(), TARGET_DB);

            query = new StringBuffer(100);
            query.append(" INSERT");
            query.append(" INTO school_user_rank (user_id, percentile, rank, rank_no_tie, school_id, user_rank_type_id, phase_id)");
            query.append(" VALUES (?, ?, ?, ?, ?, ?, ?)");
            psIns = prepareStatement(query.toString(), TARGET_DB);

            // delete all the records from the country ranking table
            psDel.executeUpdate();

            HashMap<Long, List<CoderRating>> schools = new HashMap<Long, List<CoderRating>>();
            Long tempId;
            List<CoderRating> tempList;
            CoderRating temp;
            /**
             * iterate through our big list and pluck out only those where:
             * the phase lines up
             * they have a school
             * and their status lines up
             */
            for (int i = 0; i < list.size(); i++) {
                temp = list.get(i);
                if (phaseId == temp.getPhaseId() && temp.getSchoolId() > 0) {
                    if ((rankTypeId == ACTIVE_RATING_RANK_TYPE_ID && temp.isActive()) ||
                            rankTypeId != ACTIVE_RATING_RANK_TYPE_ID) {
                        tempId = new Long(temp.getSchoolId());
                        if (schools.containsKey(tempId)) {
                            tempList = schools.get(tempId);
                        } else {
                            tempList = new ArrayList<CoderRating>(10);
                        }
                        tempList.add(list.get(i));
                        schools.put(tempId, tempList);
                        tempList = null;
                    }
                }
            }

            for (Iterator<Map.Entry<Long,List<CoderRating>>> it = schools.entrySet().iterator(); it.hasNext();) {
                ratings = it.next().getValue();
                Collections.sort(ratings);
                coderCount = ratings.size();

                int i = 0;
                int rating = 0;
                int rank = 0;
                int size = ratings.size();
                int tempRating = 0;
                long tempCoderId = 0;
                for (int j = 0; j < size; j++) {
                    i++;
                    tempRating = ((CoderRating) ratings.get(j)).getRating();
                    tempCoderId = ((CoderRating) ratings.get(j)).getCoderId();
                    if (tempRating != rating) {
                        rating = tempRating;
                        rank = i;
                    }
                    psIns.setLong(1, tempCoderId);
                    psIns.setFloat(2, (float) 100 * ((float) (coderCount - rank) / coderCount));
                    psIns.setInt(3, rank);
                    psIns.setInt(4, j + 1);
                    psIns.setLong(5, ((CoderRating) ratings.get(j)).getSchoolId());
                    psIns.setInt(6, rankTypeId);
                    psIns.setInt(7, ((CoderRating) ratings.get(j)).getPhaseId());
                    count += psIns.executeUpdate();
                }
            }
            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");


        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of 'school_coder_rank' table failed for school coder rating rank.\n" +
                    sqle.getMessage());
        } finally {
            close(rs);
            close(psIns);
            close(psDel);
        }

    }

    private void doLoadCountryRatingRank(int phaseId, int rankTypeId, List<CoderRating> list) throws Exception {
        log.debug("loadCountryRatingRank called...");
        StringBuffer query = null;
        PreparedStatement psDel = null;
        PreparedStatement psIns = null;
        ResultSet rs = null;
        int count = 0;
        int coderCount;
        List<CoderRating> ratings;

        try {
            long start = System.currentTimeMillis();
            query = new StringBuffer(100);
            query.append(" DELETE");
            query.append(" FROM country_user_rank");
            query.append(" WHERE user_rank_type_id = " + rankTypeId);
            query.append(" and phase_id = " + phaseId);
            psDel = prepareStatement(query.toString(), TARGET_DB);

            query = new StringBuffer(100);
            query.append(" INSERT");
            query.append(" INTO country_user_rank (user_id, percentile, rank, rank_no_tie, user_rank_type_id, phase_id, country_code)");
            query.append(" VALUES (?, ?, ?, ?, ?, ?, ?)");
            psIns = prepareStatement(query.toString(), TARGET_DB);

            // delete all the records from the country ranking table
            psDel.executeUpdate();

            HashMap<String, List<CoderRating>> countries = new HashMap<String, List<CoderRating>>();
            String tempCode = null;
            List<CoderRating> tempList = null;
            CoderRating temp = null;
            /**
             * iterate through our big list and pluck out only those where:
             * the phase lines up
             * they have a school
             * and their status lines up
             */
            for (int i = 0; i < list.size(); i++) {
                temp = list.get(i);
                if (temp.getPhaseId() == phaseId) {
                    if ((rankTypeId == ACTIVE_RATING_RANK_TYPE_ID && temp.isActive()) ||
                            rankTypeId != ACTIVE_RATING_RANK_TYPE_ID) {
                        tempCode = temp.getCountryCode();
                        if (countries.containsKey(tempCode)) {
                            tempList = countries.get(tempCode);
                        } else {
                            tempList = new ArrayList<CoderRating>(100);
                        }
                        tempList.add(list.get(i));
                        countries.put(tempCode, tempList);
                        tempList = null;
                    }
                }
            }

            for (Iterator<Map.Entry<String, List<CoderRating>>> it = countries.entrySet().iterator(); it.hasNext();) {
                ratings = it.next().getValue();
                Collections.sort(ratings);
                coderCount = ratings.size();

                int i = 0;
                int rating = 0;
                int rank = 0;
                int size = ratings.size();
                int tempRating;
                long tempCoderId;
                for (int j = 0; j < size; j++) {
                    i++;
                    tempRating = ((CoderRating) ratings.get(j)).getRating();
                    tempCoderId = ((CoderRating) ratings.get(j)).getCoderId();
                    if (tempRating != rating) {
                        rating = tempRating;
                        rank = i;
                    }
                    psIns.setLong(1, tempCoderId);
                    psIns.setFloat(2, (float) 100 * ((float) (coderCount - rank) / coderCount));
                    psIns.setInt(3, rank);
                    psIns.setInt(4, j + 1);
                    psIns.setInt(5, rankTypeId);
                    psIns.setInt(6, ((CoderRating) ratings.get(j)).getPhaseId());
                    psIns.setString(7, ((CoderRating) ratings.get(j)).getCountryCode());
                    count += psIns.executeUpdate();
                }
            }
            log.info("loaded " + count + " records in " + (System.currentTimeMillis() - start) / 1000 + " seconds");


        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Load of 'school_coder_rank' table failed for school coder rating rank.\n" +
                    sqle.getMessage());
        } finally {
            close(rs);
            close(psIns);
            close(psDel);
        }

    }

    /**
     * Get a sorted list (by rating_desc) of all the active coders
     * and their ratings.
     *
     * @return List containing CoderRating objects
     * @throws Exception if something goes wrong when querying
     */
    private List<CoderRating> getCurrentRatings() throws Exception {
        StringBuffer query;
        PreparedStatement psSel = null;
        ResultSet rs = null;
        List<CoderRating> ret = null;

        try {

            query = new StringBuffer(100);

            query.append(" select ur.user_id");
            query.append(" , ur.rating");
            query.append(" , ur.phase_id");
            query.append(" , case");
            query.append(" when ur.phase_id = 113 and exists (select '1' from active_developers adev where adev.user_id = ur.user_id)");
            query.append(" then 1 else 0 end as active_dev");
            query.append(" , case");
            query.append(" when ur.phase_id = 112 and exists (select '1' from active_designers ades where ades.user_id = ur.user_id)");
            query.append(" then 1 else 0 end as active_des");
            query.append(" , case");
            query.append(" when ur.phase_id = 134 and exists (select '1' from active_conceptualizers acon where acon.user_id = ur.user_id)");
            query.append(" then 1 else 0 end as active_con");
            query.append(" , case");
            query.append(" when ur.phase_id = 117 and exists (select '1' from active_specifiers aspe where aspe.user_id = ur.user_id)");
            query.append(" then 1 else 0 end as active_spe");
            query.append(" , case");
            query.append(" when ur.phase_id = 118 and exists (select '1' from active_architects aarc where aarc.user_id = ur.user_id)");
            query.append(" then 1 else 0 end as active_arc");
            query.append(" , case");
            query.append(" when ur.phase_id = 125 and exists (select '1' from active_assemblers aass where aass.user_id = ur.user_id)");
            query.append(" then 1 else 0 end as active_ass");
            query.append(" , case");
            query.append(" when ur.phase_id = 124 and exists (select '1' from active_application_testers ates where ates.user_id = ur.user_id)");
            query.append(" then 1 else 0 end as active_tes");
            query.append(" , case");
            query.append(" when ur.phase_id = 137 and exists (select '1' from active_test_scenarios_competitors asce where asce.user_id = ur.user_id)");
            query.append(" then 1 else 0 end as active_sce");
            query.append(" , case");
            query.append(" when ur.phase_id = 130 and exists (select '1' from active_ui_prototypes_competitors auip where auip.user_id = ur.user_id)");
            query.append(" then 1 else 0 end as active_uip");
            query.append(" , case");
            query.append(" when ur.phase_id = 135 and exists (select '1' from active_ria_builds_competitors arbu where arbu.user_id = ur.user_id)");
            query.append(" then 1 else 0 end as active_rbu");
            query.append(" , case");
            query.append(" when ur.phase_id = 146 and exists (select '1' from active_content_creation_competitors acc where acc.user_id = ur.user_id)");
            query.append(" then 1 else 0 end as active_cc");
            query.append(" , case");
            query.append(" when ur.phase_id = 147 and exists (select '1' from active_reporting_competitors arep where arep.user_id = ur.user_id)");
            query.append(" then 1 else 0 end as active_rep");
            query.append(" , cs.school_id");
            query.append(" , c.coder_type_id");
            query.append(" , c.comp_country_code");
            query.append(" from user_rating ur");
            query.append(" , outer current_school cs");
            query.append(" , coder c");
            query.append(" where ur.user_id = cs.coder_id");
            query.append(" and ur.user_id = c.coder_id");
            query.append(" and c.status = 'A'");

            psSel = prepareStatement(query.toString(), TARGET_DB);

            rs = psSel.executeQuery();
            ret = new ArrayList<CoderRating>();
            while (rs.next()) {
                //pros
                if (rs.getInt("coder_type_id") == 2) {
                    if (rs.getInt("phase_id") == 113) {
                        ret.add(new CoderRating(rs.getLong("user_id"), rs.getInt("rating"),
                                rs.getInt("active_dev") == 1, rs.getInt("phase_id"), rs.getString("comp_country_code")));
                    } else if (rs.getInt("phase_id") == 112) {
                        ret.add(new CoderRating(rs.getLong("user_id"), rs.getInt("rating"),
                                rs.getInt("active_des") == 1, rs.getInt("phase_id"), rs.getString("comp_country_code")));
                    } else if (rs.getInt("phase_id") == 134) {
                        ret.add(new CoderRating(rs.getLong("user_id"), rs.getInt("rating"),
                                rs.getInt("active_con") == 1, rs.getInt("phase_id"), rs.getString("comp_country_code")));
                    } else if (rs.getInt("phase_id") == 117) {
                        ret.add(new CoderRating(rs.getLong("user_id"), rs.getInt("rating"),
                                rs.getInt("active_spe") == 1, rs.getInt("phase_id"), rs.getString("comp_country_code")));
                    } else if (rs.getInt("phase_id") == 118) {
                        ret.add(new CoderRating(rs.getLong("user_id"), rs.getInt("rating"),
                                rs.getInt("active_arc") == 1, rs.getInt("phase_id"), rs.getString("comp_country_code")));
                    } else if (rs.getInt("phase_id") == 125) {
                        ret.add(new CoderRating(rs.getLong("user_id"), rs.getInt("rating"),
                                rs.getInt("active_ass") == 1, rs.getInt("phase_id"), rs.getString("comp_country_code")));
                    } else if (rs.getInt("phase_id") == 124) {
                        ret.add(new CoderRating(rs.getLong("user_id"), rs.getInt("rating"),
                                rs.getInt("active_tes") == 1, rs.getInt("phase_id"), rs.getString("comp_country_code")));
                    } else if (rs.getInt("phase_id") == 137) {
                        ret.add(new CoderRating(rs.getLong("user_id"), rs.getInt("rating"),
                                rs.getInt("active_sce") == 1, rs.getInt("phase_id"), rs.getString("comp_country_code")));
                    } else if (rs.getInt("phase_id") == 130) {
                        ret.add(new CoderRating(rs.getLong("user_id"), rs.getInt("rating"),
                                rs.getInt("active_uip") == 1, rs.getInt("phase_id"), rs.getString("comp_country_code")));
                    } else if (rs.getInt("phase_id") == 135) {
                        ret.add(new CoderRating(rs.getLong("user_id"), rs.getInt("rating"),
                                rs.getInt("active_rbu") == 1, rs.getInt("phase_id"), rs.getString("comp_country_code")));
                    } else if (rs.getInt("phase_id") == 146) {
                        ret.add(new CoderRating(rs.getLong("user_id"), rs.getInt("rating"),
                                rs.getInt("active_cc") == 1, rs.getInt("phase_id"), rs.getString("comp_country_code")));
                    } else if (rs.getInt("phase_id") == 147) {
                        ret.add(new CoderRating(rs.getLong("user_id"), rs.getInt("rating"),
                                rs.getInt("active_rep") == 1, rs.getInt("phase_id"), rs.getString("comp_country_code")));
                    }
                } else {
                    //students
                    if (rs.getInt("phase_id") == 113) {
                        ret.add(new CoderRating(rs.getLong("user_id"), rs.getInt("rating"), rs.getLong("school_id"),
                                rs.getInt("active_dev") == 1, rs.getInt("phase_id"), rs.getString("comp_country_code")));
                    } else if (rs.getInt("phase_id") == 112) {
                        ret.add(new CoderRating(rs.getLong("user_id"), rs.getInt("rating"), rs.getLong("school_id"),
                                rs.getInt("active_des") == 1, rs.getInt("phase_id"), rs.getString("comp_country_code")));
                    } else if (rs.getInt("phase_id") == 134) {
                        ret.add(new CoderRating(rs.getLong("user_id"), rs.getInt("rating"), rs.getLong("school_id"),
                                rs.getInt("active_con") == 1, rs.getInt("phase_id"), rs.getString("comp_country_code")));
                    } else if (rs.getInt("phase_id") == 117) {
                        ret.add(new CoderRating(rs.getLong("user_id"), rs.getInt("rating"), rs.getLong("school_id"),
                                rs.getInt("active_spe") == 1, rs.getInt("phase_id"), rs.getString("comp_country_code")));
                    } else if (rs.getInt("phase_id") == 118) {
                        ret.add(new CoderRating(rs.getLong("user_id"), rs.getInt("rating"), rs.getLong("school_id"),
                                rs.getInt("active_arc") == 1, rs.getInt("phase_id"), rs.getString("comp_country_code")));
                    } else if (rs.getInt("phase_id") == 125) {
                        ret.add(new CoderRating(rs.getLong("user_id"), rs.getInt("rating"), rs.getLong("school_id"),
                                rs.getInt("active_ass") == 1, rs.getInt("phase_id"), rs.getString("comp_country_code")));
                    } else if (rs.getInt("phase_id") == 124) {
                        ret.add(new CoderRating(rs.getLong("user_id"), rs.getInt("rating"), rs.getLong("school_id"),
                                rs.getInt("active_tes") == 1, rs.getInt("phase_id"), rs.getString("comp_country_code")));
                    } else if (rs.getInt("phase_id") == 137) {
                        ret.add(new CoderRating(rs.getLong("user_id"), rs.getInt("rating"), rs.getLong("school_id"),
                                rs.getInt("active_sce") == 1, rs.getInt("phase_id"), rs.getString("comp_country_code")));
                    } else if (rs.getInt("phase_id") == 130) {
                        ret.add(new CoderRating(rs.getLong("user_id"), rs.getInt("rating"), rs.getLong("school_id"),
                                rs.getInt("active_uip") == 1, rs.getInt("phase_id"), rs.getString("comp_country_code")));
                    } else if (rs.getInt("phase_id") == 135) {
                        ret.add(new CoderRating(rs.getLong("user_id"), rs.getInt("rating"), rs.getLong("school_id"),
                                rs.getInt("active_rbu") == 1, rs.getInt("phase_id"), rs.getString("comp_country_code")));
                    } else if (rs.getInt("phase_id") == 146) {
                        ret.add(new CoderRating(rs.getLong("user_id"), rs.getInt("rating"), rs.getLong("school_id"),
                                rs.getInt("active_cc") == 1, rs.getInt("phase_id"), rs.getString("comp_country_code")));
                    } else if (rs.getInt("phase_id") == 147) {
                        ret.add(new CoderRating(rs.getLong("user_id"), rs.getInt("rating"), rs.getLong("school_id"),
                                rs.getInt("active_rep") == 1, rs.getInt("phase_id"), rs.getString("comp_country_code")));
                    }
                }
            }


        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
            throw new Exception("Getting list of ratings failed.\n" +
                    sqle.getMessage());
        } finally {
            close(rs);
            close(psSel);
        }
        return ret;

    }

    private class CoderRating implements Comparable<CoderRating> {
        private long coderId = 0;
        private int rating = 0;
        private long schoolId = 0;
        private boolean active = false;
        private int phaseId = 0;
        private String countryCode = null;

        CoderRating(long coderId, int rating, long schoolId, boolean active, int phaseId, String countryCode) {
            this.coderId = coderId;
            this.rating = rating;
            this.schoolId = schoolId;
            this.active = active;
            this.phaseId = phaseId;
            this.countryCode = countryCode;
        }

        CoderRating(long coderId, int rating, boolean active, int phaseId, String countryCode) {
            this.coderId = coderId;
            this.rating = rating;
            this.active = active;
            this.phaseId = phaseId;
            this.countryCode = countryCode;
        }

        public int compareTo(CoderRating other) {
            if (other.getRating() > rating)
                return 1;
            else if (other.getRating() < rating)
                return -1;
            else
                return 0;
        }

        long getCoderId() {
            return coderId;
        }

        int getRating() {
            return rating;
        }

        void setCoderId(long coderId) {
            this.coderId = coderId;
        }

        void setRating(int rating) {
            this.rating = rating;
        }

        long getSchoolId() {
            return schoolId;
        }

        void setSchoolId(long schoolId) {
            this.schoolId = schoolId;
        }

        boolean isActive() {
            return active;
        }

        void setActive(boolean active) {
            this.active = active;
        }

        int getPhaseId() {
            return phaseId;
        }

        void setPhaseId(int phaseId) {
            this.phaseId = phaseId;
        }

        String getCountryCode() {
            return countryCode;
        }

        void setCountryCode(String countryCode) {
            this.countryCode = countryCode;
        }

        public String toString() {
            return coderId + ":" + rating + ":" + schoolId + ":" + active + ":" + phaseId;
        }
    }

}
