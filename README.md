# Overview
This is a database loader utility that extracts, transforms, and loads (ETL) from a source database to a target database. It is currently used to move data from the Topcoder oltp/transactional database to the Topcoder Redshift data warehouse. This loader, as well as the Redshift database, is intended to eventually replace the existing Informix data warehouse and corresponding loaders. It is configurable so you can setup loader classes for specific data loads.  This makes it easy to split up loading tasks and run them on separate schedules.  For example, https://github.com/topcoder-platform/tcs-loader/blob/dev/scripts/loadpayments.xml tells the loader to run only the TCLoadPayments class, which can encapsulate the loader logic for payments.

Each individual loader needs to have a numeric log_type.  The log_type is used to identify a specific loader and load times are logged for each log_type when they run.  This allows the loaders to understand when they performed the last load which is important for loaders that do incremental loads.

# Deployment

## Preparing your environment

### Informix
You will need to have the Informix docker container up and running and open
port 2021. On a host with docker installed you can issue the following commands

    sudo docker pull appiriodevops/informix:1b3d4ef
    sudo docker run -p 2021:2021 -it appiriodevops/informix:1b3d4ef


The submission contains the precompiled JAR, so you can skip to the section about setting
up Redshift. Follow the steps in the next section to build the loader from source.

### Prepare the build environment

Make sure Java and Maven are installed on your system, e.g. in Ubuntu/Debian
make sure you have run:

    sudo apt-get install openjdk-8-jdk maven

### Amazon Redshift JDBC driver
The JAR for Redshift should be included in the submission file (or you can
download it from Amazon). Unfortunately Amazon doesn't distribute it on maven,
so you'll have to install it manually. In a terminal inside the unzipped
submission run the following command:

    mvn install:install-file -Dfile=./RedshiftJDBC42-1.1.17.1017.jar \
        -DgroupId=com.amazon -DartifactId=redshift.jdbc42 \
        -Dversion=1.1.17.1017 -Dpackaging=jar -DgeneratePom=true

This will install the JAR in your local repository and make it available to maven.

### Setting up Amazon Redshift
If you have a cluster set-up, you can skip this section. Just log in to the
console and have your cluster's info ready.

NOTE: if your master user is not named redshift, you will get some errors while loading the
privileges schema. There shouldn't be any further errors though so you can ignore them.

If you don't have one already, go to the AWS console at
http://console.aws.amazon.com/redshift/home and start a Redshift cluster. Use
the default settings except the following:

- Database name: tcsdw    # NOTE: underscores are not allowed
- Database port: 5439
- Master username: redshift
- Master password: (type one and write it down because we'll need it soon)

Continue by leaving everything else in the default settings and launch the cluster.
It should be prepared in 2-3 minutes.

#### Verifying your cluster works with psql
The psql command line client is largely compatible with Redshift and you can
connect directly to your cluster. Make sure psql is installed on your system,
e.g. in Ubuntu/Debian install the psql packages:

    sudo apt-get install postgresql-client

Find your cluster's endpoint from the AWS console. It should be a URL of the form
`xxxxx.xxxx.xxxx.redshift.amazonaws.com`. Connect to it using psql with the
following command:

    psql -h <endpoint_url> -p 5439 -U redshift -d tcsdw

You should be given a password prompt and after typing the master password you
should get a `tcsdw#` prompt.

### Loading the schema & test data

The schema files are inside `./redshift_schema` and there is a convenience script
that can load them to Redshift via psql.

NOTE: Make sure you have psql installed or otherwise the script below will fail.
Alternatively, you can run the SQL files in redshift_schema/ with any other DB
tool you want, as long as they are executed in the correct order.

Run it like this:

    ./scripts/redshift_create_schema.sh -P <master_password> -h <redshift_endpoint>

If you're using non-default settings, you can enter more options:

    ./scripts/redshift_create_schema.sh -U <master_user> -P <master_password> \
        -h <redshift_endpoint> -p <port> -d <dbname> -s <schema_directory>

The script will load the the converted tcs_dw schema for Redshift, along with
test data and a few other tables (coder, calendar, etc.) for testing convenience.

### Running TCSLoad

First, edit the XML configuration file `loadredshift.xml` inside the scripts directory. Change
the targetdb node to match your cluster. The format is:

    <targetdb>jdbc:redshift://<endpoint>:<port>/<dbname>;PWD=<password>;UID=<user></targetdb>

with the default settings in this README:

    <targetdb>jdbc:redshift://<endpoint>:5439/tcsdw;PWD=<password>;UID=redshift</targetdb>

i.e. the JDBC url shown in the console with the PWD and UID variables appended.

Next, prepare the package by running:

    mvn install
    mvn package

maven should pull and install all dependencies and package the project into a
JAR in the target directory. Now you can run:

    bash ./scripts/loadredshiftscript.sh

You should be seeing a long list of log messages. The whole process takes around
5-10 minutes.

## Verification

Almost all doLoadXXX methods work these two:

- doLoadDRTrackContests
- doLoadDRTrackResults

Both are looking for a class DRv2July08TopNCalculator which was not included in
the original files

As few tables have any actual data in the Informix docker container, you can
evaluate the loader by checking the following doLoadXXX methods. You can use
a GUI tool like SQL Workbench to check the Redshift tables or you can use psql:

- doLoadProjects
  Changes the project table. List a sample of rows with:

    PGPASSWORD=<master_pass> psql -h <endpoint> -p <port> -U redshift -d tcsdw \
        -c "SELECT project_id, category_desc, last_modification_date FROM project"

- doLoadClientProjectDim
  Changes the client_project_dim table. List a sample of rows with:

    PGPASSWORD=<master_pass> psql -h <endpoint> -p <port> -U redshift -d tcsdw \
        -c "SELECT client_name, project_name, billing_account_code FROM client_project_dim"

- doLoadScoreCardTemplate
  Changes the scorecard_template table. List a sample of rows with:

    PGPASSWORD=<master_pass> psql -h <endpoint> -p <port> -U redshift -d tcsdw \
        -c "SELECT * FROM scorecard_template"

- doLoadScoreCardQuestion
  Changes the scorecard_question table. List a sample of rows with:

    PGPASSWORD=<master_pass> psql -h <endpoint> -p <port> -U redshift -d tcsdw \
        -c "SELECT question_desc, question_weight FROM scorecard_question"


## Schema conversion

- All tables were converted to Redshift. A long list of the modifications made
to fit Redshift are on the beginning of `01_tcs_dw_redshift_main_schema.sql`.
There were a few minor issues with the data conversion but generally everything
has a 1-1 correspondence with Informix.

- Primary & foreign key constraints were kept. They are not actually enforced
by Redshift but they are used by the query planner.

- GRANTS & REVOKES were kept. A dummy user with the name 'coder' is created when
`02_tcs_dw_redshift_privileges.sql` is executed since there are some privileges
assigned to that username.

- Indexes were not kept; while sortkeys and distkeys exist in Redshift for a similar
purpose, they're not directly equivalent to indexes.

- Triggers and sequences are not supported by Redshift and were removed.

## Code description
All the loading functionality is contained in the TCLoadTCSRedshift class. It
is modified from TCLoadTCS to match the query syntax of Redshift, different
behavior of the JDBC driver, etc. But the two files are mostly identical and the
vast majority of the changes do not affect the logic or data in any significant
way.

## Connect Postgres Load to Redshift
A new class TCLoadConnectProjects is added in the tcspostgresredshift folder.
Its responsible for loading Connect Projects and Connect Members in the Redshift

Script file : /scripts/loadconnectdatascript.sh
Xml Config:   /scripts/loadConnectdata.xml

Run : bash ./scripts/loadconnectdatascript.sh

