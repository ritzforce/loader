CREATE TABLE tcs_dw.appeal
(
   appeal_id              numeric(12)      NOT NULL,
   scorecard_question_id  numeric(12),
   scorecard_id           numeric(12),
   project_id             numeric(12),
   user_id                numeric(12),
   reviewer_id            numeric(12),
   raw_evaluation_id      numeric(3),
   final_evaluation_id    numeric(3),
   appeal_text            varchar(65535),
   appeal_response        varchar(65535),
   successful_ind         numeric(1)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.appeal TO tcs_dw;

CREATE TABLE tcs_dw.calendar
(
   calendar_id      integer,
   year             integer,
   month_numeric    integer,
   month_alpha      varchar(10),
   day_of_month     integer,
   day_of_week      integer,
   week_day         varchar(15),
   year_month       varchar(7),
   week_of_year     integer,
   day_of_year      integer,
   holiday          char(1),
   weekend          char(1),
   date             timestamp,
   week_year        integer,
   quarter_of_year  integer
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.calendar TO tcs_dw;

CREATE TABLE tcs_dw.client_project_dim
(
   client_project_id           integer        NOT NULL,
   client_id                   numeric(12)    NOT NULL,
   client_name                 varchar(64)    NOT NULL,
   client_create_date          timestamp      NOT NULL,
   client_modification_date    timestamp      NOT NULL,
   billing_project_id          numeric(12)    NOT NULL,
   project_name                varchar(64)    NOT NULL,
   project_create_date         timestamp      NOT NULL,
   project_modification_date   timestamp      NOT NULL,
   billing_account_code        varchar(64)    NOT NULL,
   cmc_account_id              varchar(100),
   customer_number             varchar(100),
   subscription_number         varchar(64),
   billing_account_status      integer,
   billing_account_start_date  timestamp,
   billing_account_end_date    timestamp,
   billing_account_budget      decimal(10,3)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.client_project_dim TO tcs_dw;

CREATE TABLE tcs_dw.client_user_stats
(
   client_user_stats_id  integer   NOT NULL,
   client_id             integer   NOT NULL,
   year                  integer   NOT NULL,
   month                 integer   NOT NULL,
   user_count            integer   NOT NULL
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.client_user_stats TO tcs_dw;

CREATE TABLE tcs_dw.coder
(
   coder_id               integer,
   state_code             varchar(2),
   country_code           varchar(3),
   first_name             varchar(100),
   last_name              varchar(100),
   home_phone             varchar(100),
   work_phone             varchar(100),
   address1               varchar(200),
   address2               varchar(200),
   city                   varchar(100),
   zip                    varchar(20),
   middle_name            varchar(100),
   activation_code        varchar(17),
   member_since           timestamp,
   notify                 char(1),
   quote                  varchar(255),
   employer_search        char(1),
   relocate               char(1),
   modify_date            timestamp,
   editor_id              numeric(1),
   notify_inquiry         char(1),
   language_id            numeric(3),
   coder_type_id          numeric(3),
   status                 varchar(3),
   email                  varchar(100),
   last_login             timestamp,
   image                  integer,
   comp_country_code      varchar(3),
   last_site_hit_date     timestamp,
   reg_source             varchar(20),
   utm_source             varchar(50),
   utm_medium             varchar(50),
   utm_campaign           varchar(50),
   create_date            timestamp,
   dw_stats_updated_time  timestamp,
   handle                 varchar(50),
   handle_lower           varchar(50)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.coder TO tcs_dw;

CREATE TABLE tcs_dw.command
(
   command_id        numeric(10)    NOT NULL,
   command_desc      varchar(100),
   command_group_id  numeric(5)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.command TO tcs_dw;

CREATE TABLE tcs_dw.command_execution
(
   command_id      numeric(10)     NOT NULL,
   timestamp       timestamp       DEFAULT getdate(),
   execution_time  integer,
   inputs          varchar(2000)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.command_execution TO tcs_dw;

CREATE TABLE tcs_dw.command_group_lu
(
   command_group_id    numeric(5)     NOT NULL,
   command_group_name  varchar(100)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.command_group_lu TO tcs_dw;

CREATE TABLE tcs_dw.command_query_xref
(
   command_id  numeric(10)   NOT NULL,
   query_id    numeric(10)   NOT NULL,
   sort_order  numeric(3)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.command_query_xref TO tcs_dw;

CREATE TABLE tcs_dw.consulting_time_and_material
(
   consulting_time_and_material_id                bigint          DEFAULT "identity"(210849, 0, ('0,1'::character varying)::text),
   account_name                                   varchar(256),
   project_name                                   varchar(256),
   resource_full_name                             varchar(256),
   resource_topcoder_user_id                      integer,
   resource_topcoder_handle                       varchar(64),
   timecard_id                                    varchar(128),
   timecard_split_id                              varchar(128),
   start_date                                     timestamp,
   end_date                                       timestamp,
   total_hours                                    numeric(10,2),
   total_billable_hours                           numeric(10,2),
   currency_t_and_m_equivalent_revenue_converted  varchar(3),
   t_and_m_equivalent_revenue_converted           numeric(10,2),
   planned_bill_rate                              numeric(10,2),
   currency_planned_bill_rate                     varchar(3)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.consulting_time_and_material TO tcs_dw;

CREATE TABLE tcs_dw.contest
(
   contest_id               numeric(12),
   contest_name             varchar(128),
   contest_type_id          numeric(5),
   contest_type_desc        varchar(64),
   contest_start_timestamp  timestamp,
   contest_end_timestamp    timestamp,
   phase_id                 numeric(5),
   event_id                 integer,
   project_category_id      integer,
   project_category_name    varchar(254)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.contest TO tcs_dw;

CREATE TABLE tcs_dw.contest_prize
(
   contest_prize_id  integer,
   contest_id        numeric(12),
   prize_type_id     numeric(5),
   prize_type_desc   varchar(64),
   place             numeric(2),
   prize_amount      numeric(10,2),
   prize_desc        varchar(64)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.contest_prize TO tcs_dw;

CREATE TABLE tcs_dw.contest_project_xref
(
   project_id  numeric(12),
   contest_id  numeric(12)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.contest_project_xref TO tcs_dw;

alter table contest_project_xref
add UNIQUE (project_id, contest_id);

CREATE TABLE tcs_dw.contest_result
(
   contest_id        numeric(12),
   coder_id          numeric(12),
   initial_points    float8        NOT NULL,
   final_points      float8        NOT NULL,
   potential_points  float8        NOT NULL,
   current_place     integer,
   current_prize     float8
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.contest_result TO tcs_dw;

CREATE TABLE tcs_dw.contest_season_xref
(
   contest_id  numeric(12),
   season_id   numeric(6)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.contest_season_xref TO tcs_dw;

CREATE TABLE tcs_dw.contest_stage_xref
(
   contest_id             numeric(12),
   stage_id               numeric(6),
   top_performers_factor  numeric(4,2)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.contest_stage_xref TO tcs_dw;

CREATE TABLE tcs_dw.copilot_statistics
(
   copilot_profile_id      integer,
   user_id                 integer,
   projects_count          integer,
   contests_count          integer,
   reposts_count           integer,
   failures_count          integer,
   bug_races_count         integer,
   current_projects_count  integer,
   current_contests_count  integer,
   fulfillment             numeric(5,2),
   submission_rate         numeric(5,2),
   total_earnings          numeric(10,2)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.copilot_statistics TO tcs_dw;

CREATE TABLE tcs_dw.country_user_rank
(
   user_id            integer,
   country_code       varchar(3),
   rank               numeric(7),
   rank_no_tie        numeric(7),
   percentile         numeric(7,4),
   user_rank_type_id  numeric(3),
   phase_id           numeric(3)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.country_user_rank TO tcs_dw;

CREATE TABLE tcs_dw.data_type_lu
(
   data_type_id    numeric(5),
   data_type_desc  varchar(16)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.data_type_lu TO tcs_dw;

CREATE TABLE tcs_dw.design_project_result
(
   project_id                 numeric(12),
   user_id                    numeric(12),
   submission_id              numeric(12),
   upload_id                  numeric(12),
   prize_id                   numeric(12),
   prize_amount               numeric(10,2),
   placement                  numeric(6),
   dr_points                  float8,
   is_checkpoint              numeric(1),
   client_selection           numeric(1),
   submit_timestamp           timestamp,
   review_complete_timestamp  timestamp,
   inquire_timestamp          timestamp,
   submit_ind                 numeric(1),
   valid_submission_ind       numeric(1)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.design_project_result TO tcs_dw;

CREATE TABLE tcs_dw.direct_project_dim
(
   direct_project_id          integer          NOT NULL,
   name                       varchar(200)     NOT NULL,
   project_status_id          integer          DEFAULT 1 NOT NULL,
   project_create_date        timestamp        NOT NULL,
   project_modification_date  timestamp        NOT NULL,
   billing_project_id         numeric(12),
   description                varchar(10000)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.direct_project_dim TO tcs_dw;

CREATE TABLE tcs_dw.dr_points
(
   dr_points_id                   integer         NOT NULL,
   track_id                       integer         NOT NULL,
   dr_points_reference_type_id    integer         NOT NULL,
   dr_points_reference_type_desc  varchar(50)     NOT NULL,
   dr_points_operation_id         integer         NOT NULL,
   dr_points_operation_desc       varchar(50)     NOT NULL,
   dr_points_type_id              integer         NOT NULL,
   dr_points_type_desc            varchar(50)     NOT NULL,
   dr_points_status_id            integer         NOT NULL,
   dr_points_status_desc          varchar(50)     NOT NULL,
   dr_points_desc                 varchar(100)    NOT NULL,
   user_id                        integer         NOT NULL,
   amount                         numeric(10,2)   NOT NULL,
   application_date               timestamp       NOT NULL,
   award_date                     timestamp       NOT NULL,
   reference_id                   integer,
   is_potential                   boolean
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.dr_points TO tcs_dw;

CREATE TABLE tcs_dw.dual
(
   value  integer
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.dual TO tcs_dw;

CREATE TABLE tcs_dw.evaluation_lu
(
   evaluation_id         numeric(3)    NOT NULL,
   evaluation_desc       varchar(50),
   evaluation_value      float8,
   evaluation_type_id    numeric(3),
   evaluation_type_desc  varchar(50)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.evaluation_lu TO tcs_dw;

CREATE TABLE tcs_dw.event
(
   event_id    integer,
   event_name  varchar(128)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.event TO tcs_dw;

CREATE TABLE tcs_dw.event_registration
(
   event_id      integer,
   user_id       integer,
   eligible_ind  numeric(1),
   notes         varchar(255),
   create_date   timestamp,
   modify_date   timestamp
);

GRANT INSERT, SELECT, TRIGGER, DELETE, UNKNOWN, RULE, REFERENCES, UPDATE ON tcs_dw.event_registration TO tcs_dw;

CREATE TABLE tcs_dw.input_lu
(
   input_id      numeric(10)    NOT NULL,
   input_code    varchar(25),
   data_type_id  numeric(5),
   input_desc    varchar(100)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.input_lu TO tcs_dw;

CREATE TABLE tcs_dw.jira_issue
(
   jira_issue_id    integer,
   ticket_id        varchar(255),
   reporter         varchar(255),
   assignee         varchar(255),
   summary          varchar(255),
   created          timestamp,
   updated          timestamp,
   due_date         timestamp,
   resolution_date  timestamp,
   votes            integer,
   winner           varchar(255),
   payment_amount   numeric(15,2),
   contest_id       integer,
   status           varchar(255),
   tco_points       integer,
   project_id       integer,
   admin_fee        numeric(10,2),
   issue_type       varchar(255),
   payment_status   varchar(255)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.jira_issue TO tcs_dw;

CREATE TABLE tcs_dw.log_type_lu
(
   log_type_id    numeric(3)     NOT NULL,
   log_type_desc  varchar(100)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.log_type_lu TO tcs_dw;

CREATE TABLE tcs_dw.monthly_contest_stats
(
   client_project_id         integer         NOT NULL,
   tc_direct_project_id      numeric(12)     NOT NULL,
   project_category_id       numeric(12)     NOT NULL,
   month                     numeric(2)      NOT NULL,
   year                      numeric(5)      NOT NULL,
   avg_contest_fees          numeric(10,2)   NOT NULL,
   avg_member_fees           numeric(10,2)   NOT NULL,
   avg_duration              numeric(10,2)   NOT NULL,
   avg_fulfillment           numeric(10,2)   NOT NULL,
   total_completed_contests  numeric(8)      NOT NULL,
   total_failed_contests     numeric(8)      NOT NULL
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.monthly_contest_stats TO tcs_dw;

CREATE TABLE tcs_dw.participation
(
   user_id             integer     NOT NULL,
   participation_type  integer     NOT NULL,
   participation_date  timestamp
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.participation TO tcs_dw;

CREATE TABLE tcs_dw.participation_metrics_report_copilot
(
   contest_id    integer,
   copilot_id    integer,
   country_code  varchar(3),
   country       varchar(100)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.participation_metrics_report_copilot TO tcs_dw;

CREATE TABLE tcs_dw.participation_metrics_report_member
(
   contest_id             integer,
   registrant_id          integer,
   is_submitter           boolean,
   is_milestone_winner    boolean,
   is_final_winner        boolean,
   country_code           varchar(3),
   country                varchar(100),
   num_of_milestone_subs  numeric(8),
   num_of_final_subs      numeric(8),
   num_of_final_wins      numeric(8),
   num_of_milestone_wins  numeric(8)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.participation_metrics_report_member TO tcs_dw;

CREATE TABLE tcs_dw.payment
(
   payment_id            integer,
   payment_type_id       numeric(3),
   payment_type_desc     varchar(100),
   reference_id          integer,
   parent_payment_id     integer,
   charity_ind           numeric(1)     DEFAULT 1 NOT NULL,
   show_in_profile_ind   numeric(1)     DEFAULT 1 NOT NULL,
   show_details_ind      numeric(1)     DEFAULT 1 NOT NULL,
   payment_status_id     numeric(3),
   payment_status_desc   varchar(200),
   client                varchar(100),
   modified_calendar_id  integer,
   modified_time_id      integer,
   installment_number    numeric(3),
   jira_ticket_id        varchar(100),
   created_calendar_id   integer,
   created_time_id       integer,
   payment_desc          varchar(200)
);

GRANT INSERT, SELECT, TRIGGER, DELETE, UNKNOWN, RULE, REFERENCES, UPDATE ON tcs_dw.payment TO tcs_dw;

CREATE TABLE tcs_dw.project
(
   project_id                        numeric(12)     NOT NULL,
   component_id                      numeric(12),
   component_name                    varchar(256),
   num_registrations                 numeric(5),
   num_submissions                   numeric(5),
   num_valid_submissions             numeric(5),
   avg_raw_score                     numeric(5,2),
   avg_final_score                   numeric(5,2),
   phase_id                          numeric(5),
   phase_desc                        varchar(64),
   category_id                       numeric(12),
   category_desc                     varchar(64),
   posting_date                      timestamp,
   submitby_date                     timestamp,
   complete_date                     timestamp,
   checkpoint_start_date             timestamp,
   checkpoint_end_date               timestamp,
   review_phase_id                   numeric(3),
   review_phase_name                 varchar(30),
   status_id                         numeric(3),
   status_desc                       varchar(50),
   level_id                          numeric(5),
   version_id                        numeric(5),
   version_text                      char(20),
   rating_date                       timestamp,
   viewable_category_ind             numeric(1),
   num_submissions_passed_review     numeric(5),
   winner_id                         numeric(10),
   stage_id                          numeric(6),
   digital_run_ind                   numeric(1)      DEFAULT 1,
   suspended_ind                     numeric(1)      DEFAULT 0,
   project_category_id               integer,
   project_category_name             varchar(254),
   tc_direct_project_id              integer,
   admin_fee                         numeric(10,2),
   first_place_prize                 numeric(10,2),
   num_checkpoint_submissions        numeric(5),
   num_valid_checkpoint_submissions  numeric(5),
   total_prize                       numeric(10,2),
   contest_prizes_total              numeric(10,2),
   client_project_id                 integer,
   start_date_calendar_id            numeric(12),
   duration                          numeric(10,2),
   fulfillment                       numeric(10,2),
   last_modification_date            timestamp,
   challenge_manager                 numeric(10),
   challenge_creator                 numeric(10),
   challenge_launcher                numeric(10),
   copilot                           numeric(10),
   registration_end_date             timestamp,
   scheduled_end_date                timestamp,
   checkpoint_prize_amount           numeric(10,2),
   checkpoint_prize_number           numeric(5),
   dr_points                         numeric(10,2),
   reliability_cost                  numeric(10,2),
   review_cost                       numeric(10,2),
   forum_id                          numeric(10),
   submission_viewable               numeric(1),
   is_private                        numeric(1),
   actual_total_prize                numeric(10,2),
   copilot_cost                      numeric(10,2),
   estimated_reliability_cost        numeric(10,2),
   estimated_review_cost             numeric(10,2),
   estimated_copilot_cost            numeric(10,2),
   estimated_admin_fee               numeric(10,2),
   task_ind                          numeric(1)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.project TO tcs_dw;

CREATE TABLE tcs_dw.project_platform
(
   project_id           numeric(12),
   project_platform_id  numeric(12),
   name                 varchar(255)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.project_platform TO tcs_dw;

CREATE TABLE tcs_dw.project_result
(
   project_id                 numeric(12),
   user_id                    numeric(12),
   submit_ind                 numeric(1),
   valid_submission_ind       numeric(1),
   raw_score                  numeric(5,2),
   final_score                numeric(5,2),
   inquire_timestamp          timestamp,
   submit_timestamp           timestamp,
   review_complete_timestamp  timestamp,
   payment                    numeric(10,2),
   old_rating                 numeric(5),
   new_rating                 numeric(5),
   old_reliability            numeric(5,4),
   new_reliability            numeric(5,4),
   placed                     numeric(6),
   rating_ind                 numeric(1),
   passed_review_ind          numeric(1),
   points_awarded             float8,
   final_points               float8,
   potential_points           float8,
   reliable_submission_ind    numeric(1),
   num_appeals                numeric(3),
   num_successful_appeals     numeric(3),
   old_rating_id              integer,
   new_rating_id              integer,
   num_ratings                numeric(6),
   rating_order               integer
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.project_result TO tcs_dw;

CREATE TABLE tcs_dw.project_review
(
   project_id     numeric(12),
   reviewer_id    numeric(12),
   total_payment  numeric(10,2)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.project_review TO tcs_dw;

CREATE TABLE tcs_dw.project_spec_review_xref
(
   project_id              numeric(12),
   spec_review_project_id  numeric(12)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.project_spec_review_xref TO tcs_dw;

CREATE TABLE tcs_dw.project_technology
(
   project_id             numeric(12),
   project_technology_id  numeric(12),
   name                   varchar(100)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.project_technology TO tcs_dw;

CREATE TABLE tcs_dw.query
(
   query_id      numeric(10)      NOT NULL,
   text          varchar(65535),
   name          varchar(100),
   ranking       integer,
   column_index  integer
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.query TO tcs_dw;

CREATE TABLE tcs_dw.query_input_xref
(
   query_id       numeric(10)    NOT NULL,
   optional       char(1),
   default_value  varchar(100),
   input_id       numeric(10)    NOT NULL,
   sort_order     numeric(3)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.query_input_xref TO tcs_dw;

CREATE TABLE tcs_dw.review_resp
(
   review_resp_id    numeric(3),
   review_resp_desc  varchar(50)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.review_resp TO tcs_dw;

CREATE TABLE tcs_dw.rookie
(
   user_id        integer,
   season_id      integer,
   phase_id       numeric(5),
   confirmed_ind  numeric(1)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.rookie TO tcs_dw;

CREATE TABLE tcs_dw.royalty
(
   user_id       integer,
   amount        numeric(7,2),
   royalty_date  date,
   description   varchar(254)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.royalty TO tcs_dw;

CREATE TABLE tcs_dw.scheduled_start_time_reg
(
   project_id            integer,
   phase_type_id         integer,
   scheduled_start_time  timestamp
);

GRANT INSERT, SELECT, TRIGGER, DELETE, UNKNOWN, RULE, REFERENCES, UPDATE ON tcs_dw.scheduled_start_time_reg TO tcs_dw;

CREATE TABLE tcs_dw.school_user_rank
(
   user_id            integer,
   school_id          integer,
   rank               numeric(7),
   rank_no_tie        numeric(7),
   percentile         numeric(7,4),
   user_rank_type_id  numeric(3),
   phase_id           numeric(3)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.school_user_rank TO tcs_dw;

CREATE TABLE tcs_dw.scorecard_question
(
   scorecard_question_id  numeric(12)      NOT NULL,
   scorecard_template_id  numeric(12),
   question_text          varchar(65535),
   question_weight        float8,
   section_id             numeric(12),
   section_desc           varchar(254),
   section_weight         float8,
   section_group_id       numeric(12),
   section_group_desc     varchar(254),
   question_desc          char(8),
   sort                   numeric(3),
   question_header        varchar(254)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.scorecard_question TO tcs_dw;

CREATE TABLE tcs_dw.scorecard_response
(
   scorecard_question_id  numeric(12)   NOT NULL,
   scorecard_id           numeric(12)   NOT NULL,
   user_id                numeric(12),
   reviewer_id            numeric(12),
   project_id             numeric(12),
   evaluation_id          numeric(3)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.scorecard_response TO tcs_dw;

CREATE TABLE tcs_dw.scorecard_template
(
   scorecard_template_id  numeric(12)   NOT NULL,
   scorecard_type_id      numeric(3),
   scorecard_type_desc    varchar(50)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.scorecard_template TO tcs_dw;

CREATE TABLE tcs_dw.season
(
   season_id               numeric(6),
   start_calendar_id       integer,
   end_calendar_id         integer,
   name                    varchar(254),
   rookie_competition_ind  numeric(1),
   next_rookie_season_id   numeric(6)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.season TO tcs_dw;

CREATE TABLE tcs_dw.stage
(
   stage_id           numeric(6),
   season_id          numeric(6),
   start_calendar_id  integer,
   end_calendar_id    integer,
   name               varchar(254)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.stage TO tcs_dw;

CREATE TABLE tcs_dw.streak
(
   coder_id          integer,
   streak_type_id    numeric(3),
   phase_id          numeric(3),
   start_project_id  numeric(12),
   end_project_id    numeric(12),
   length            numeric(3),
   is_current        numeric(1)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.streak TO tcs_dw;

CREATE TABLE tcs_dw.streak_type_lu
(
   streak_type_id  numeric(3),
   streak_desc     varchar(100)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.streak_type_lu TO tcs_dw;

CREATE TABLE tcs_dw.subjective_response
(
   scorecard_question_id  numeric(12),
   scorecard_id           numeric(12),
   user_id                numeric(12),
   reviewer_id            numeric(12),
   project_id             numeric(12),
   response_text          varchar(256),
   response_type_id       numeric(12),
   response_type_desc     varchar(2048),
   sort                   numeric(3)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.subjective_response TO tcs_dw;

CREATE TABLE tcs_dw.submission
(
   submission_id    numeric(12)     NOT NULL,
   submitter_id     numeric(12)     NOT NULL,
   project_id       numeric(12)     NOT NULL,
   submission_url   varchar(2048)   NOT NULL,
   submission_type  integer         NOT NULL
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.submission TO tcs_dw;

CREATE TABLE tcs_dw.submission_review
(
   project_id              numeric(12),
   user_id                 numeric(12),
   reviewer_id             numeric(12),
   num_appeals             numeric(3),
   num_successful_appeals  numeric(3),
   final_score             numeric(5,2),
   raw_score               numeric(5,2),
   review_resp_id          numeric(3),
   scorecard_id            numeric(12),
   scorecard_template_id   numeric(12)
);

GRANT INSERT, SELECT, TRIGGER, DELETE, UNKNOWN, RULE, REFERENCES, UPDATE ON tcs_dw.submission_review TO tcs_dw;

CREATE TABLE tcs_dw.submission_screening
(
   project_id             numeric(12)    NOT NULL,
   user_id                numeric(12)    NOT NULL,
   reviewer_id            numeric(12),
   final_score            numeric(5,2),
   scorecard_id           numeric(12),
   scorecard_template_id  numeric(12)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.submission_screening TO tcs_dw;

CREATE TABLE tcs_dw.tcd_project_stat
(
   tcd_project_id       integer,
   project_category_id  integer,
   stat_date            timestamp,
   cost                 numeric(10,2),
   duration             numeric(10,2),
   fulfillment          numeric(10,2),
   total_project        integer,
   create_user          varchar(64),
   create_date          timestamp,
   modify_user          varchar(64),
   modify_date          timestamp
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.tcd_project_stat TO tcs_dw;

CREATE TABLE tcs_dw.testcase_appeal
(
   appeal_id              numeric(12)      NOT NULL,
   scorecard_question_id  numeric(12),
   scorecard_id           numeric(12),
   project_id             numeric(12),
   user_id                numeric(12),
   reviewer_id            numeric(12),
   raw_num_passed         numeric(7),
   raw_num_tests          numeric(7),
   final_num_passed       numeric(7),
   final_num_tests        numeric(7),
   appeal_text            varchar(65535),
   appeal_response        varchar(65535),
   successful_ind         numeric(1)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.testcase_appeal TO tcs_dw;

CREATE TABLE tcs_dw.testcase_response
(
   scorecard_question_id  numeric(12)   NOT NULL,
   scorecard_id           numeric(12)   NOT NULL,
   user_id                numeric(12),
   reviewer_id            numeric(12),
   project_id             numeric(12),
   num_tests              numeric(7),
   num_passed             numeric(7)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.testcase_response TO tcs_dw;

CREATE TABLE tcs_dw.time
(
   time_id    integer       NOT NULL,
   minute     integer       NOT NULL,
   hour       integer       NOT NULL,
   hour_24    integer       NOT NULL,
   meridiem   varchar(20)   NOT NULL,
   full_time  varchar(20)   NOT NULL,
   time_12    varchar(8)    NOT NULL,
   time_24    varchar(8)    NOT NULL
);

GRANT INSERT, SELECT, TRIGGER, DELETE, UNKNOWN, RULE, REFERENCES, UPDATE ON tcs_dw.time TO tcs_dw;

CREATE TABLE tcs_dw.track
(
   track_id           integer       NOT NULL,
   track_type_id      integer       NOT NULL,
   track_type_desc    varchar(50)   NOT NULL,
   track_status_id    integer       NOT NULL,
   track_status_desc  varchar(50)   NOT NULL,
   track_desc         varchar(50)   NOT NULL,
   track_start_date   timestamp     NOT NULL,
   track_end_date     timestamp     NOT NULL
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.track TO tcs_dw;

CREATE TABLE tcs_dw.track_contest
(
   track_contest_id         integer        NOT NULL,
   track_id                 integer        NOT NULL,
   track_contest_desc       varchar(128)   NOT NULL,
   track_contest_type_id    integer        NOT NULL,
   track_contest_type_desc  varchar(128)   NOT NULL
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.track_contest TO tcs_dw;

CREATE TABLE tcs_dw.track_contest_results
(
   track_contest_id             integer         NOT NULL,
   user_id                      integer         NOT NULL,
   track_contest_placement      integer         NOT NULL,
   track_contest_prize          numeric(12,2),
   taxable_track_contest_prize  numeric(12,2)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.track_contest_results TO tcs_dw;

CREATE TABLE tcs_dw.update_log
(
   log_id         integer       DEFAULT "identity"(220606, 0, '1,1'::text) NOT NULL,
   calendar_id    numeric(10),
   log_timestamp  timestamp,
   log_type_id    numeric(3)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.update_log TO tcs_dw;

CREATE TABLE tcs_dw.user_achievement_rule
(
   user_achievement_rule_id          numeric(12)    NOT NULL,
   user_achievement_name             varchar(254)   NOT NULL,
   user_achievement_rule_desc        varchar(254)   NOT NULL,
   user_achievement_rule_sql_file    varchar(254),
   user_achievement_type_id          numeric(12)    NOT NULL,
   is_automated                      boolean,
   db_schema                         varchar(50)    DEFAULT 'tcs_catalog'::character varying,
   user_achievement_earned_sql_file  varchar(254),
   user_achievement_count_sql_file   varchar(254),
   user_achievement_count_query      varchar(254)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.user_achievement_rule TO tcs_dw;

CREATE TABLE tcs_dw.user_achievement_type_lu
(
   user_achievement_type_id    numeric(12)    NOT NULL,
   user_achievement_type_desc  varchar(254)   NOT NULL
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.user_achievement_type_lu TO tcs_dw;

CREATE TABLE tcs_dw.user_achievement_xref
(
   user_id                   numeric(12)   NOT NULL,
   user_achievement_rule_id  numeric(12)   NOT NULL,
   create_date               timestamp     NOT NULL,
   auto_loaded               boolean,
   is_earned_date_populated  boolean
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.user_achievement_xref TO tcs_dw;

CREATE TABLE tcs_dw.user_component_score
(
   user_component_score_id  integer         NOT NULL,
   component_id             integer         NOT NULL,
   component_name           varchar(255)    NOT NULL,
   user_id                  integer         NOT NULL,
   level_id                 integer         NOT NULL,
   comp_vers_id             integer         NOT NULL,
   phase_id                 integer         NOT NULL,
   score                    numeric(5,2)    NOT NULL,
   money                    numeric(10,2)   DEFAULT 0 NOT NULL,
   processed                integer         DEFAULT 0 NOT NULL,
   rating                   integer,
   place                    integer,
   submission_date          timestamp,
   mod_date_time            timestamp       NOT NULL,
   create_date_time         timestamp       NOT NULL
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.user_component_score TO tcs_dw;

CREATE TABLE tcs_dw.user_contest_prize
(
   user_id            numeric(12),
   contest_id         numeric(12),
   prize_type_id      numeric(5),
   place              numeric(3),
   prize_description  varchar(64),
   prize_amount       numeric(10,2),
   prize_payment      numeric(10,2)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.user_contest_prize TO tcs_dw;

CREATE TABLE tcs_dw.user_event_xref
(
   user_id      integer     NOT NULL,
   event_id     integer     NOT NULL,
   create_date  timestamp
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.user_event_xref TO tcs_dw;

CREATE TABLE tcs_dw.user_payment
(
   payment_id        integer,
   user_id           integer,
   net_amount        numeric(12,2),
   gross_amount      numeric(12,2),
   due_calendar_id   integer,
   paid_calendar_id  integer,
   total_amount      numeric(12,2)
);

GRANT INSERT, SELECT, TRIGGER, DELETE, UNKNOWN, RULE, REFERENCES, UPDATE ON tcs_dw.user_payment TO tcs_dw;

CREATE TABLE tcs_dw.user_permission_grant
(
   user_permission_grant_id  numeric(10)   NOT NULL,
   user_id                   numeric(10)   NOT NULL,
   resource_id               numeric(10)   NOT NULL,
   permission_type_id        numeric(10)   NOT NULL,
   is_studio                 smallint
);

ALTER TABLE tcs_dw.user_permission_grant
   ADD CONSTRAINT pk_user_permission_grant_id
   PRIMARY KEY (user_permission_grant_id);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.user_permission_grant TO tcs_dw;

CREATE TABLE tcs_dw.user_rank
(
   user_id            integer,
   phase_id           numeric(3),
   rank               numeric(7),
   percentile         numeric(7,4),
   user_rank_type_id  numeric(3)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.user_rank TO tcs_dw;

CREATE TABLE tcs_dw.user_rank_type_lu
(
   user_rank_type_id    numeric(3),
   user_rank_type_desc  varchar(100)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.user_rank_type_lu TO tcs_dw;

CREATE TABLE tcs_dw.user_rating
(
   user_id                integer       NOT NULL,
   rating                 integer       DEFAULT 0 NOT NULL,
   phase_id               numeric(3)    NOT NULL,
   vol                    integer       DEFAULT 0 NOT NULL,
   rating_no_vol          integer       DEFAULT 0 NOT NULL,
   num_ratings            numeric(5)    DEFAULT 0 NOT NULL,
   mod_date_time          timestamp     NOT NULL,
   create_date_time       timestamp     NOT NULL,
   last_rated_project_id  numeric(12),
   highest_rating         integer,
   lowest_rating          integer
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.user_rating TO tcs_dw;

CREATE TABLE tcs_dw.user_reliability
(
   user_id      integer,
   rating       numeric(5,4),
   modify_date  timestamp,
   create_date  timestamp,
   phase_id     numeric(12)
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.user_reliability TO tcs_dw;

CREATE TABLE tcs_dw.weekly_contest_stats
(
   client_project_id         integer         NOT NULL,
   tc_direct_project_id      numeric(12)     NOT NULL,
   project_category_id       numeric(12)     NOT NULL,
   week                      numeric(4)      NOT NULL,
   month                     numeric(2)      NOT NULL,
   year                      numeric(5)      NOT NULL,
   avg_contest_fees          numeric(10,2)   NOT NULL,
   avg_member_fees           numeric(10,2)   NOT NULL,
   avg_duration              numeric(10,2)   NOT NULL,
   avg_fulfillment           numeric(10,2)   NOT NULL,
   total_completed_contests  numeric(8)      NOT NULL,
   total_failed_contests     numeric(8)      NOT NULL
);

GRANT INSERT, SELECT, DELETE, UNKNOWN, REFERENCES, UPDATE ON tcs_dw.weekly_contest_stats TO tcs_dw;



COMMIT;
