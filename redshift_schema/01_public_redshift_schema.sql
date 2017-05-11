create table user_component_score (
    user_component_score_id DECIMAL(10) not null,
    component_id DECIMAL(10) not null,
    component_name VARCHAR(255) not null,
    user_id DECIMAL(10) not null,
    level_id DECIMAL(4) not null,
    comp_vers_id DECIMAL(10) not null,
    phase_id DECIMAL(3) not null,
    score DECIMAL(5,2) not null,
    money DECIMAL(10,2) default 0 not null,
    processed DECIMAL(16) default 0 not null,
    rating DECIMAL(6),
    place DECIMAL(4),
    submission_date DATE not null,
    mod_date_time TIMESTAMP default GETDATE() not null,
    create_date_time TIMESTAMP default GETDATE() not null
);

CREATE TABLE log_type_lu (
    log_type_id DECIMAL(3,0) NOT NULL,
    log_type_desc VARCHAR(100)
);

create table update_log (
    log_id INT not null,
    calendar_id DECIMAL(10,0),
    log_timestamp TIMESTAMP,
    log_type_id DECIMAL(3,0)
);

CREATE TABLE project (
    project_id DECIMAL(12,0) NOT NULL,
    component_id DECIMAL(12,0),
    component_name VARCHAR(128),
    num_registrations DECIMAL(5,0),
    num_submissions DECIMAL(5,0),
    num_valid_submissions DECIMAL(5,0),
    avg_raw_score DECIMAL(5,2),
    avg_final_score DECIMAL(5,2),
    phase_id DECIMAL(5,0),
    phase_desc VARCHAR(64),
    category_id DECIMAL(12,0),
    category_desc VARCHAR(64),
    posting_date TIMESTAMP,
    submitby_date TIMESTAMP,
    complete_date TIMESTAMP,
    checkpoint_start_date TIMESTAMP,
    checkpoint_end_date   TIMESTAMP,
    review_phase_id DECIMAL(3,0),
    review_phase_name VARCHAR(30),
    status_id DECIMAL(3,0),
    status_desc VARCHAR(50),
    level_id DECIMAL(5,0),
    version_id DECIMAL(3,0),
    version_text CHAR(20),
    rating_date TIMESTAMP,
    viewable_category_ind DECIMAL(1,0),
    num_submissions_passed_review DECIMAL(5,0),
    winner_id DECIMAL(10,0),
    stage_id DECIMAL(6,0),
    digital_run_ind DECIMAL(1,0) default 1,
    suspended_ind DECIMAL(1,0) default 0,
    project_category_id INT,
    project_category_name VARCHAR(254),
    tc_direct_project_id INT,
    admin_fee DECIMAL(10, 2), -- loaded from project_info 31 admin fee
    first_place_prize DECIMAL(10, 2),
    num_checkpoint_submissions DECIMAL(5,0),
    num_valid_checkpoint_submissions DECIMAL(5,0),
    total_prize DECIMAL(10, 2), -- the total offered prizes when the challenge launched
    contest_prizes_total DECIMAL(10, 2), -- the actual prizes earned by members in this challenge
    client_project_id INTEGER,
    start_date_calendar_id DECIMAL(12, 0),
    duration DECIMAL(10, 2),
    fulfillment DECIMAL(10, 2),
	  last_modification_date TIMESTAMP,
    challenge_manager DECIMAL(10,0),
    challenge_creator DECIMAL(10,0),
    challenge_launcher DECIMAL(10,0),
    copilot DECIMAL(10,0),
    registration_end_date TIMESTAMP,
    scheduled_end_date TIMESTAMP,
    checkpoint_prize_amount DECIMAL(10, 2),
    checkpoint_prize_number DECIMAL(5, 0),
    dr_points DECIMAL(10, 2),
    reliability_cost DECIMAL(10, 2),
    review_cost DECIMAL(10, 2),
    forum_id DECIMAL(10, 0),
    submission_viewable DECIMAL(1,0),
    is_private DECIMAL(1,0),
    actual_total_prize DECIMAL(10, 2),
    copilot_cost DECIMAL(10, 2),
    estimated_reliability_cost DECIMAL(10, 2),
    estimated_review_cost DECIMAL(10, 2),
    estimated_copilot_cost DECIMAL(10, 2),
    estimated_admin_fee DECIMAL(10, 2)
);


CREATE TABLE project_technology (
    project_id DECIMAL(12,0) NOT NULL,
    project_technology_id DECIMAL(12,0) NOT NULL,
    name VARCHAR(100)
);


CREATE TABLE project_platform (
    project_id DECIMAL(12,0) NOT NULL,
    project_platform_id DECIMAL(12,0) NOT NULL,
    name VARCHAR(255)
);



CREATE TABLE contest (
    contest_id DECIMAL(12,0) NOT NULL,
    contest_name VARCHAR(128),
    contest_type_id DECIMAL(5,0),
    contest_type_desc VARCHAR(64),
    contest_start_timestamp TIMESTAMP,
    contest_end_timestamp TIMESTAMP,
    phase_id DECIMAL(5,0),
    event_id DECIMAL(10,0),
    project_category_id INT,
    project_category_name VARCHAR(254)
);

CREATE TABLE contest_project_xref (
    project_id DECIMAL(12,0) NOT NULL,
    contest_id DECIMAL(12,0) NOT NULL
);

CREATE TABLE project_result (
    project_id DECIMAL(12,0) NOT NULL,
    user_id DECIMAL(12,0) NOT NULL,
    submit_ind DECIMAL(1,0),
    valid_submission_ind DECIMAL(1,0),
    raw_score DECIMAL(5,2),
    final_score DECIMAL(5,2),
    inquire_timestamp TIMESTAMP,
    submit_timestamp TIMESTAMP,
    review_complete_timestamp TIMESTAMP,
    payment DECIMAL(10,2),
    old_rating DECIMAL(5,0),
    new_rating DECIMAL(5,0),
    old_reliability DECIMAL(5,4),
    new_reliability DECIMAL(5,4),
    placed DECIMAL(6,0),
    rating_ind DECIMAL(1,0),
    passed_review_ind DECIMAL(1,0),
    points_awarded FLOAT,
    final_points FLOAT,
    potential_points FLOAT,
    reliable_submission_ind DECIMAL(1,0),
    num_appeals DECIMAL(3,0),
    num_successful_appeals DECIMAL(3,0),
    old_rating_id INT,
    new_rating_id INT,
    num_ratings DECIMAL(6,0),
    rating_order INT
);

CREATE TABLE submission_review (
    project_id DECIMAL(12,0) NOT NULL,
    user_id DECIMAL(12,0) NOT NULL,
    reviewer_id DECIMAL(12,0) NOT NULL,
    num_appeals DECIMAL(3,0),
    num_successful_appeals DECIMAL(3,0),
    final_score DECIMAL(5,2),
    raw_score DECIMAL(5,2),
    review_resp_id DECIMAL(3,0),
    scorecard_id DECIMAL(12,0),
    scorecard_template_id DECIMAL(12,0)
);

CREATE TABLE user_contest_prize (
    user_id DECIMAL(12,0) NOT NULL,
    contest_id DECIMAL(12,0),
    prize_type_id DECIMAL(5,0),
    place DECIMAL(3,0),
    prize_description VARCHAR(64),
    prize_amount DECIMAL(10,2),
    prize_payment DECIMAL(10,2)
);

CREATE TABLE royalty (
    user_id DECIMAL(10,0) NOT NULL,
    amount DECIMAL(7,2),
    description VARCHAR(254),
    royalty_date DATE
);

CREATE TABLE user_reliability (
    user_id DECIMAL(10,0) NOT NULL,
    rating DECIMAL(5,4),
    modify_date TIMESTAMP default GETDATE(),
    create_date TIMESTAMP default GETDATE(),
    phase_id DECIMAL(12,0) NOT NULL
);

CREATE TABLE event (
    event_id DECIMAL(10,0) NOT NULL,
    event_name VARCHAR(128)
);

CREATE TABLE user_event_xref (
    user_id DECIMAL(10,0) not null,
    event_id DECIMAL(10,0) not null,
    create_date TIMESTAMP default GETDATE()
);

CREATE TABLE user_rank (
    user_id DECIMAL(10,0) NOT NULL,
    phase_id DECIMAL(3,0),
    rank DECIMAL(7,0),
    percentile DECIMAL(7,4),
    user_rank_type_id DECIMAL(3,0)
);

CREATE TABLE user_rank_type_lu (
    user_rank_type_id DECIMAL(3,0) NOT NULL,
    user_rank_type_desc VARCHAR(100)
);

CREATE TABLE school_user_rank (
    user_id DECIMAL(10,0) NOT NULL,
    school_id DECIMAL(10,0),
    rank DECIMAL(7,0),
    rank_no_tie DECIMAL(7,0),
    percentile DECIMAL(7,4),
    user_rank_type_id DECIMAL(3,0),
    phase_id DECIMAL(3,0)
);

CREATE TABLE country_user_rank (
    user_id DECIMAL(10,0) NOT NULL,
    country_code VARCHAR(3),
    rank DECIMAL(7,0),
    rank_no_tie DECIMAL(7,0),
    percentile DECIMAL(7,4),
    user_rank_type_id DECIMAL(3,0),
    phase_id DECIMAL(3,0)
);

CREATE TABLE command (
    command_id DECIMAL(10,0) NOT NULL,
    command_desc VARCHAR(100),
    command_group_id DECIMAL(5,0)
);

create table command_group_lu (
    command_group_id DECIMAL(5,0) not null,
    command_group_name VARCHAR(100)
);

CREATE TABLE data_type_lu (
    data_type_id DECIMAL(5,0) NOT NULL,
    data_type_desc VARCHAR(16)
);

CREATE TABLE input_lu (
    input_id DECIMAL(10,0) NOT NULL,
    input_code VARCHAR(25),
    data_type_id DECIMAL(5,0),
    input_desc VARCHAR(100)
);

CREATE TABLE query (
    query_id DECIMAL(10,0) NOT NULL,
    text VARCHAR(65535),
    name VARCHAR(100),
    ranking INT,
    column_index INT
);

CREATE TABLE query_input_xref (
    query_id DECIMAL(10,0) NOT NULL,
    optional CHAR(1),
    default_value VARCHAR(100),
    input_id DECIMAL(10,0) NOT NULL,
    sort_order DECIMAL(3,0)
);

CREATE TABLE command_query_xref (
    command_id DECIMAL(10,0) NOT NULL,
    query_id DECIMAL(10,0) NOT NULL,
    sort_order DECIMAL(3,0)
);

create table dual (
    value INT
);

CREATE TABLE submission_screening (
    project_id DECIMAL(12,0) NOT NULL,
    user_id DECIMAL(12,0) NOT NULL,
    reviewer_id DECIMAL(12,0),
    final_score DECIMAL(5,2),
    scorecard_id DECIMAL(12,0),
    scorecard_template_id DECIMAL(12,0)
);

CREATE TABLE review_resp (
    review_resp_id DECIMAL(3,0) NOT NULL,
    review_resp_desc VARCHAR(50)
);

CREATE TABLE scorecard_template (
    scorecard_template_id DECIMAL(12,0) NOT NULL,
    scorecard_type_id DECIMAL(3,0),
    scorecard_type_desc VARCHAR(50)
);

CREATE TABLE evaluation_lu (
    evaluation_id DECIMAL(3,0) NOT NULL,
    evaluation_desc VARCHAR(50),
    evaluation_value FLOAT,
    evaluation_type_id DECIMAL(3,0),
    evaluation_type_desc VARCHAR(50)
);

CREATE TABLE scorecard_response (
    scorecard_question_id DECIMAL(12,0) NOT NULL,
    scorecard_id DECIMAL(12,0) NOT NULL,
    user_id DECIMAL(12,0),
    reviewer_id DECIMAL(12,0),
    project_id DECIMAL(12,0),
    evaluation_id DECIMAL(3,0)
);

CREATE TABLE testcase_response (
    scorecard_question_id DECIMAL(12,0) NOT NULL,
    scorecard_id DECIMAL(12,0) NOT NULL,
    user_id DECIMAL(12,0),
    reviewer_id DECIMAL(12,0),
    project_id DECIMAL(12,0),
    num_tests DECIMAL(7,0),
    num_passed DECIMAL(7,0)
);

CREATE TABLE subjective_response (
    scorecard_question_id DECIMAL(12,0) NOT NULL,
    scorecard_id DECIMAL(12,0) NOT NULL,
    user_id DECIMAL(12,0),
    reviewer_id DECIMAL(12,0),
    project_id DECIMAL(12,0),
    response_text VARCHAR(65535),
    response_type_id DECIMAL(12,0),
    response_type_desc VARCHAR,
    sort DECIMAL(3,0) NOT NULL
);

CREATE TABLE appeal (
    appeal_id DECIMAL(12,0) NOT NULL,
    scorecard_question_id DECIMAL(12,0),
    scorecard_id DECIMAL(12,0),
    project_id DECIMAL(12,0),
    user_id DECIMAL(12,0),
    reviewer_id DECIMAL(12,0),
    raw_evaluation_id DECIMAL(3,0),
    final_evaluation_id DECIMAL(3,0),
    appeal_text VARCHAR(65535),
    appeal_response VARCHAR(65535),
    successful_ind DECIMAL(1,0)
);

CREATE TABLE testcase_appeal (
    appeal_id DECIMAL(12,0) NOT NULL,
    scorecard_question_id DECIMAL(12,0),
    scorecard_id DECIMAL(12,0),
    project_id DECIMAL(12,0),
    user_id DECIMAL(12,0),
    reviewer_id DECIMAL(12,0),
    raw_num_passed DECIMAL(7,0),
    raw_num_tests DECIMAL(7,0),
    final_num_passed DECIMAL(7,0),
    final_num_tests DECIMAL(7,0),
    appeal_text VARCHAR(65535),
    appeal_response VARCHAR(65535),
    successful_ind DECIMAL(1,0)
);

CREATE TABLE project_review (
    project_id DECIMAL(12,0) NOT NULL,
    reviewer_id DECIMAL(12,0) NOT NULL,
    total_payment DECIMAL(10,2)
);

CREATE TABLE season (
    season_id DECIMAL(6,0) NOT NULL,
    start_calendar_id DECIMAL(10,0),
    end_calendar_id DECIMAL(10,0),
    name VARCHAR(254),
    rookie_competition_ind DECIMAL(1,0),
    next_rookie_season_id DECIMAL(6,0)
);

CREATE TABLE stage (
    stage_id DECIMAL(6,0) NOT NULL,
    season_id DECIMAL(6,0),
    start_calendar_id DECIMAL(10,0),
    end_calendar_id DECIMAL(10,0),
    name VARCHAR(254)
);

CREATE TABLE rookie (
    user_id DECIMAL(10,0) NOT NULL,
    season_id DECIMAL(10,0),
    phase_id DECIMAL(5,0),
    confirmed_ind DECIMAL(1,0)
);

create table submission (
    submission_id DECIMAL(12,0) not null,
    submitter_id DECIMAL(12,0) not null,
    project_id DECIMAL(12,0) not null,
    submission_url VARCHAR(65535) not null,
    submission_type DECIMAL(7) not null
);

CREATE TABLE command_execution (
    command_id DECIMAL(10,0) NOT NULL,
    timestamp TIMESTAMP default GETDATE(),
    execution_time INT,
    inputs VARCHAR(2000)
);

CREATE TABLE contest_prize (
    contest_prize_id DECIMAL(10,0) NOT NULL,
    contest_id DECIMAL(12,0),
    prize_type_id DECIMAL(5,0),
    prize_type_desc VARCHAR(64),
    place DECIMAL(2,0),
    prize_amount DECIMAL(10,2),
    prize_desc VARCHAR(64)
);

CREATE TABLE contest_stage_xref (
    contest_id DECIMAL(12,0) NOT NULL,
    stage_id DECIMAL(6,0) NOT NULL,
    top_performers_factor DECIMAL(4,2)
);

CREATE TABLE contest_season_xref (
    contest_id DECIMAL(12,0) NOT NULL,
    season_id DECIMAL(6,0) NOT NULL
);

CREATE TABLE streak_type_lu (
    streak_type_id DECIMAL(3,0) NOT NULL,
    streak_desc VARCHAR(100)
);

CREATE TABLE streak (
    coder_id DECIMAL(10,0) NOT NULL,
    streak_type_id DECIMAL(3,0),
    phase_id DECIMAL(3,0),
    start_project_id DECIMAL(12,0),
    end_project_id DECIMAL(12,0),
    length DECIMAL(3,0),
    is_current DECIMAL(1,0)
);

CREATE TABLE project_result_022707 (
    project_id DECIMAL(12,0) NOT NULL,
    user_id DECIMAL(12,0) NOT NULL,
    submit_ind DECIMAL(1,0),
    valid_submission_ind DECIMAL(1,0),
    raw_score DECIMAL(5,2),
    final_score DECIMAL(5,2),
    inquire_timestamp TIMESTAMP,
    submit_timestamp TIMESTAMP,
    review_complete_timestamp TIMESTAMP,
    payment DECIMAL(10,2),
    old_rating DECIMAL(5,0),
    new_rating DECIMAL(5,0),
    old_reliability DECIMAL(5,4),
    new_reliability DECIMAL(5,4),
    placed DECIMAL(6,0),
    rating_ind DECIMAL(1,0),
    reliability_ind DECIMAL(1,0),
    passed_review_ind DECIMAL(1,0),
    points_awarded DECIMAL(5,0),
    final_points DECIMAL(5,0),
    point_adjustment DECIMAL(5,0) default 0,
    current_reliability_ind DECIMAL(1,0),
    reliable_submission_ind DECIMAL(1,0),
    num_appeals DECIMAL(3,0),
    num_successful_appeals DECIMAL(3,0),
    old_rating_id INT,
    new_rating_id INT,
    num_ratings DECIMAL(6,0)
);

CREATE TABLE contest_result (
    contest_id DECIMAL(12,0) NOT NULL,
    coder_id DECIMAL(12,0) NOT NULL,
    initial_points FLOAT not null,
    final_points FLOAT not null,
    potential_points FLOAT not null,
    current_place INT,
    current_prize FLOAT
);

CREATE TABLE scorecard_question (
    scorecard_question_id DECIMAL(12,0) NOT NULL,
    scorecard_template_id DECIMAL(12,0),
    question_text VARCHAR(65535),
    question_weight FLOAT,
    section_id DECIMAL(12,0),
    section_desc VARCHAR(254),
    section_weight FLOAT,
    section_group_id DECIMAL(12,0),
    section_group_desc VARCHAR(254),
    question_desc CHAR(8),
    sort DECIMAL(3,0),
    question_header VARCHAR(254)
);

CREATE TABLE sr_bad (
    scorecard_question_id DECIMAL(12,0) NOT NULL,
    scorecard_id DECIMAL(12,0)
);

CREATE TABLE sr_hold (
    scorecard_question_id DECIMAL(12,0) NOT NULL,
    scorecard_id DECIMAL(12,0),
    user_id DECIMAL(12,0),
    reviewer_id DECIMAL(12,0),
    project_id DECIMAL(12,0),
    response_text VARCHAR(65535),
    response_type_id DECIMAL(12,0),
    response_type_desc VARCHAR(65535),
    sort DECIMAL(3,0)
);

CREATE TABLE user_rating (
    user_id DECIMAL(10,0) not null,
    rating DECIMAL(10,0) default 0 not null,
    phase_id DECIMAL(3,0) not null,
    vol DECIMAL(10,0) default 0 not null,
    rating_no_vol DECIMAL(10,0) default 0 not null,
    num_ratings DECIMAL(5,0) default 0 not null,
    mod_date_time TIMESTAMP default GETDATE() not null,
    create_date_time TIMESTAMP default GETDATE() not null,
    last_rated_project_id DECIMAL(12,0),
    highest_rating DECIMAL(10,0),
    lowest_rating DECIMAL(10,0)
);

CREATE TABLE dr_points (
    dr_points_id DECIMAL(10,0) not null,
    track_id DECIMAL(10,0) not null,
    dr_points_reference_type_id INT not null,
    dr_points_reference_type_desc VARCHAR(50) not null,
    dr_points_operation_id INT not null,
    dr_points_operation_desc VARCHAR(50) not null,
    dr_points_type_id INT not null,
    dr_points_type_desc VARCHAR(50) not null,
    dr_points_status_id INT not null,
    dr_points_status_desc VARCHAR(50) not null,
    dr_points_desc VARCHAR(100) not null,
    user_id DECIMAL(10,0) not null,
    amount DECIMAL(10,2) not null,
    application_date TIMESTAMP not null,
    award_date TIMESTAMP not null,
    reference_id DECIMAL(10,0),
    is_potential boolean
);

create table track (
    track_id DECIMAL(10,0) not null,
    track_type_id INT not null,
    track_type_desc VARCHAR(50) not null,
    track_status_id INT not null,
    track_status_desc VARCHAR(50) not null,
    track_desc VARCHAR(50) not null,
    track_start_date TIMESTAMP not null,
    track_end_date TIMESTAMP not null
);

create table track_contest (
    track_contest_id DECIMAL(10,0) not null,
    track_id DECIMAL(10,0) not null,
    track_contest_desc VARCHAR(128) not null,
    track_contest_type_id DECIMAL(10,0) not null,
    track_contest_type_desc VARCHAR(128) not null
);

create table track_contest_results (
    track_contest_id DECIMAL(10,0) not null,
    user_id DECIMAL(10,0) not null,
    track_contest_placement INT not null,
    track_contest_prize DECIMAL(12,2),
	taxable_track_contest_prize DECIMAL(12,2)
);

create table project_spec_review_xref
  (
    project_id DECIMAL(12,0),
    spec_review_project_id DECIMAL(12,0)
  );

CREATE TABLE tcd_project_stat (
    tcd_project_id INT NOT NULL,
    project_category_id INT,
    stat_date TIMESTAMP,
    cost DECIMAL(10, 2),
    duration DECIMAL(10, 2),
    fulfillment INT,
    total_project INT,
    create_user VARCHAR(64),
    create_date TIMESTAMP,
    modify_user VARCHAR(64),
    modify_date TIMESTAMP,
    PRIMARY KEY (tcd_project_id, project_category_id, stat_date)
);


CREATE TABLE client_project_dim (
    client_project_id INTEGER NOT NULL,
    client_id DECIMAL(12, 0) NOT NULL,
    client_name VARCHAR(64) NOT NULL,
    client_create_date TIMESTAMP NOT NULL,
    client_modification_date TIMESTAMP NOT NULL,
    billing_project_id DECIMAL(12, 0) NOT NULL,
    project_name VARCHAR(64) NOT NULL,
    project_create_date TIMESTAMP NOT NULL,
    project_modification_date TIMESTAMP NOT NULL,
    billing_account_code VARCHAR(64) NOT NULL,
	cmc_account_id VARCHAR(100),
	customer_number VARCHAR(100),
	subscription_number  VARCHAR(64)
);

create table direct_project_dim
  (
    direct_project_id integer not null,
    name varchar(200) not null,
    description VARCHAR(10000),
    billing_project_id DECIMAL(12, 0),
    project_status_id INT default 1 not null,
    project_create_date TIMESTAMP NOT NULL,
    project_modification_date TIMESTAMP NOT NULL
  );

CREATE TABLE weekly_contest_stats (
    client_project_id INTEGER NOT NULL,
    tc_direct_project_id DECIMAL(12, 0) NOT NULL,
    project_category_id DECIMAL(12, 0) NOT NULL,
    week DECIMAL(4, 0) NOT NULL,
    month DECIMAL(2, 0) NOT NULL,
    year DECIMAL(5, 0) NOT NULL,
    avg_contest_fees DECIMAL(10, 2) NOT NULL,
    avg_member_fees DECIMAL(10, 2) NOT NULL,
    avg_duration DECIMAL(10, 2) NOT NULL,
    avg_fulfillment DECIMAL(10, 2) NOT NULL,
    total_completed_contests DECIMAL(8, 0) NOT NULL,
    total_failed_contests DECIMAL(8, 0) NOT NULL
);

CREATE TABLE monthly_contest_stats (
    client_project_id INTEGER NOT NULL,
    tc_direct_project_id DECIMAL(12, 0) NOT NULL,
    project_category_id DECIMAL(12, 0) NOT NULL,
    month DECIMAL(2, 0) NOT NULL,
    year DECIMAL(5, 0) NOT NULL,
    avg_contest_fees DECIMAL(10, 2) NOT NULL,
    avg_member_fees DECIMAL(10, 2) NOT NULL,
    avg_duration DECIMAL(10, 2) NOT NULL,
    avg_fulfillment DECIMAL(10, 2) NOT NULL,
    total_completed_contests DECIMAL(8, 0) NOT NULL,
    total_failed_contests DECIMAL(8, 0) NOT NULL
);


CREATE TABLE user_achievement_rule (
	user_achievement_rule_id DECIMAL(12,0) NOT NULL,
    user_achievement_name VARCHAR(254) NOT NULL,
	user_achievement_rule_desc VARCHAR(254) NOT NULL,
	user_achievement_rule_sql_file VARCHAR(254),
	user_achievement_type_id DECIMAL(12,0) NOT NULL,
	is_automated BOOLEAN DEFAULT 't',
	db_schema VARCHAR(50) DEFAULT 'tcs_catalog',
    user_achievement_earned_sql_file VARCHAR(254),
	user_achievement_count_sql_file VARCHAR(254),
    user_achievement_count_query VARCHAR(2540)

);


CREATE TABLE user_achievement_type_lu (
	user_achievement_type_id DECIMAL(12,0) NOT NULL,
	user_achievement_type_desc VARCHAR(254) NOT NULL
);


CREATE TABLE user_achievement_xref (
	user_id DECIMAL(12,0) NOT NULL,
	user_achievement_rule_id DECIMAL(12,0) NOT NULL,
	create_date TIMESTAMP NOT NULL,
	auto_loaded BOOLEAN,
    is_earned_date_populated BOOLEAN DEFAULT 'f'
);



CREATE TABLE participation_metrics_report_copilot (
    contest_id DECIMAL(10,0) NOT NULL,
    copilot_id INT,
    country_code VARCHAR(3),
    country VARCHAR(100)
);



CREATE TABLE participation_metrics_report_member (
    contest_id DECIMAL(10,0) NOT NULL,
    registrant_id INT,
    is_submitter BOOLEAN,
    is_milestone_winner BOOLEAN,
    is_final_winner BOOLEAN,
	num_of_milestone_subs DECIMAL(8, 0),
	num_of_final_subs DECIMAL(8, 0),
	num_of_milestone_wins DECIMAL(8, 0),
	num_of_final_wins DECIMAL(8, 0),
    country_code VARCHAR(3),
    country VARCHAR(100)
);


create table user_permission_grant(
    user_permission_grant_id decimal(10) not null ,
    user_id decimal(10,0) not null ,
    resource_id decimal(10,0) not null ,
    permission_type_id decimal(10,0) not null ,
    is_studio smallint
);
ALTER TABLE user_permission_grant
ADD CONSTRAINT pk_user_permission_grant_id
PRIMARY KEY (user_permission_grant_id) ;


  CREATE TABLE jira_issue(
	jira_issue_id INT NOT NULL,
	ticket_id VARCHAR(255),
	reporter VARCHAR(255),
	assignee VARCHAR(255),
	summary VARCHAR(255),
	description VARCHAR(65535),
	tco_points INT,
	created TIMESTAMP,
	updated TIMESTAMP,
	due_date TIMESTAMP,
	resolution_date TIMESTAMP,
	votes INT,
	winner VARCHAR(255),
	payment_amount DECIMAL(15, 2),
	admin_fee DECIMAL(10, 2),
	contest_id INT,
	project_id INT,
	status VARCHAR(255),
	payment_status VARCHAR(255),
	issue_type VARCHAR(255)
);
ALTER TABLE jira_issue
ADD CONSTRAINT pk_jira_issue
PRIMARY KEY (jira_issue_id) ;


CREATE TABLE copilot_statistics (
    copilot_profile_id INT NOT NULL,
    user_id INT,
    projects_count INT,
    contests_count INT,
    reposts_count INT,
    failures_count INT,
    bug_races_count INT,
    current_projects_count INT,
    current_contests_count INT,
    fulfillment DECIMAL(5,2),
    submission_rate DECIMAL(5,2),
    total_earnings DECIMAL(10,2)
);


CREATE TABLE client_user_stats (
    client_user_stats_id INT not null,
    client_id INT not null,
    year INT not null,
    month INT not null,
    user_count INT not null
);

ALTER TABLE client_user_stats
ADD CONSTRAINT pk_client_user_stats
PRIMARY KEY (client_user_stats_id) ;


create table participation (
    user_id DECIMAL(10) not null,
    participation_type INT not null,
    participation_date DATE not null
);


CREATE TABLE design_project_result (
    project_id DECIMAL(12,0) NOT NULL,
    user_id DECIMAL(12,0),
    submission_id DECIMAL(12, 0),
    upload_id DECIMAL(12, 0),
    prize_id DECIMAL(12,0),
    prize_amount DECIMAL(10,2),
    placement DECIMAL(6,0),
    dr_points FLOAT,
    is_checkpoint DECIMAL(1, 0),
    client_selection DECIMAL(1, 0),
    submit_timestamp TIMESTAMP,
    review_complete_timestamp TIMESTAMP,
    inquire_timestamp TIMESTAMP,
    submit_ind DECIMAL(1, 0),
    valid_submission_ind DECIMAL(1, 0)
);

CREATE TABLE tcs_dw.payment
(
   payment_id            integer,
   payment_desc          varchar(100),
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
   created_time_id       integer
);


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


create view active_developers (user_id) as
   select x0.user_id
   from user_rating x0 ,project x1
   where ((((x0.last_rated_project_id = x1.project_id ) AND (x0.phase_id = 113. ) ) AND (x1.phase_id = x0.phase_id ) ) AND (x1.posting_date > (GETDATE() - INTERVAL '180 days' ) ) ) ;
create view active_designers (user_id) as
   select x0.user_id
   from user_rating x0 ,project x1
   where ((((x0.last_rated_project_id = x1.project_id ) AND (x0.phase_id = 112. ) ) AND (x1.phase_id = x0.phase_id ) ) AND (x1.posting_date > (GETDATE() - INTERVAL '180 days' ) ) ) ;

create view active_conceptualizers (user_id) as
   select x0.user_id
   from user_rating x0 ,project x1
   where ((((x0.last_rated_project_id = x1.project_id ) AND (x0.phase_id = 134. ) ) AND (x1.phase_id = x0.phase_id ) ) AND (x1.posting_date > (GETDATE() - INTERVAL '180 days' ) ) ) ;

create view active_specifiers (user_id) as
   select x0.user_id
   from user_rating x0 ,project x1
   where ((((x0.last_rated_project_id = x1.project_id ) AND (x0.phase_id = 117. ) ) AND (x1.phase_id = x0.phase_id ) ) AND (x1.posting_date > (GETDATE() - INTERVAL '180 days' ) ) ) ;

create view active_architects (user_id) as
   select x0.user_id
   from user_rating x0 ,project x1
   where ((((x0.last_rated_project_id = x1.project_id ) AND (x0.phase_id = 118. ) ) AND (x1.phase_id = x0.phase_id ) ) AND (x1.posting_date > (GETDATE() - INTERVAL '180 days' ) ) ) ;

create view active_assemblers (user_id) as
   select x0.user_id
   from user_rating x0 ,project x1
   where ((((x0.last_rated_project_id = x1.project_id ) AND (x0.phase_id = 125. ) ) AND (x1.phase_id = x0.phase_id ) ) AND (x1.posting_date > (GETDATE() - INTERVAL '180 days' ) ) ) ;

create view active_application_testers (user_id) as
   select x0.user_id
   from user_rating x0 ,project x1
   where ((((x0.last_rated_project_id = x1.project_id ) AND (x0.phase_id = 124. ) ) AND (x1.phase_id = x0.phase_id ) ) AND (x1.posting_date > (GETDATE() - INTERVAL '180 days' ) ) ) ;

create view active_test_scenarios_competitors (user_id) as
   select x0.user_id
   from user_rating x0 ,project x1
   where ((((x0.last_rated_project_id = x1.project_id ) AND (x0.phase_id = 137. ) ) AND (x1.phase_id = x0.phase_id ) ) AND (x1.posting_date > (GETDATE() - INTERVAL '180 days' ) ) ) ;

create view active_ui_prototypes_competitors (user_id) as
   select x0.user_id
   from user_rating x0 ,project x1
   where ((((x0.last_rated_project_id = x1.project_id ) AND (x0.phase_id = 130. ) ) AND (x1.phase_id = x0.phase_id ) ) AND (x1.posting_date > (GETDATE() - INTERVAL '180 days' ) ) ) ;

create view active_ria_builds_competitors (user_id) as
   select x0.user_id
   from user_rating x0 ,project x1
   where ((((x0.last_rated_project_id = x1.project_id ) AND (x0.phase_id = 135. ) ) AND (x1.phase_id = x0.phase_id ) ) AND (x1.posting_date > (GETDATE() - INTERVAL '180 days' ) ) ) ;

create view active_content_creation_competitors (user_id) as
   select x0.user_id
   from user_rating x0 ,project x1
   where ((((x0.last_rated_project_id = x1.project_id ) AND (x0.phase_id = 146. ) ) AND (x1.phase_id = x0.phase_id ) ) AND (x1.posting_date > (GETDATE() - INTERVAL '180 days' ) ) ) ;

create view active_reporting_competitors (user_id) as
   select x0.user_id
   from user_rating x0 ,project x1
   where ((((x0.last_rated_project_id = x1.project_id ) AND (x0.phase_id = 147. ) ) AND (x1.phase_id = x0.phase_id ) ) AND (x1.posting_date > (GETDATE() - INTERVAL '180 days' ) ) ) ;
