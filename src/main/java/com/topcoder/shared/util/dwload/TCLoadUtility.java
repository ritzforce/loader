package com.topcoder.shared.util.dwload;

/**
 * TCLoadUtility.java
 *
 * This is the load utility class for TopCoder loads. Using this class, you
 * can perform any of the loads identified by classes derived from TCLoad.
 *
 * TODO: Add explanation of command line options/XML files here
 *
 * @author Christopher Hopkins [TCid: darkstalker] (chrism_hopkins@yahoo.com)
 * @version $Revision: 68374 $
 *
 */

import com.topcoder.shared.util.DBMS;
import com.topcoder.shared.util.logging.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

public class TCLoadUtility {
    private static Logger log = Logger.getLogger(TCLoadUtility.class);
    /**
     * This holds any error message that might occur when performing a particular
     * load. So, if a load fails, we can print something nice to the user.
     */
    private static StringBuffer sErrorMsg = new StringBuffer(128);

    /**
     * This variable holds the name of the JDBC driver we are using to connect
     * to the databases.
     */
    private static String sDriverName = "com.informix.jdbc.IfxDriver";

    /**
     * This variable holds the stage of the load process.
     */
    private static String stage = null;

    /**
     * This variable holds the start time of the load process.
     */
    private static java.sql.Timestamp startTime = null;

    /**
     * This variable holds the last log time of the load process.
     */
    private static java.sql.Timestamp lastLogTime = null;

    /**
     * The main method parses the command line options (or XML file when we
     * decide to go that route), determines the class name of the load to run,
     * parses any additional parameters for that load and runs the load.
     */
    public static void main(String[] args) {
        // First, parse the argument list and come up with a Hashtable of
        // arguments to this load. The only required argument is -load
        // "classname" which specifies which load to run. If we have a
        // -xmlfile argument as the first argument, we have been given an
        // XML file to load which specifies which loads to run and their
        // parameters. So, we need to parse that appropriately. Otherwise,
        // we do a normal, single load
        if (args.length > 1 && args[0].equals("-xmlfile")) {
            runXMLLoad(args[1]);
        } else {
            Hashtable params = parseArgs(args);

            checkDriver();

            String loadclass = (String) params.get("load");
            runTCLoad(loadclass, params);
        }
    }

    /**
     * This method runs a particular load(s) specified by the XML file
     * passed on the command line.
     */
    private static void runXMLLoad(String xmlFileName) {
        try {
            FileInputStream f = new FileInputStream(xmlFileName);
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder dombuild = dbf.newDocumentBuilder();
            Document doc = dombuild.parse(f);

            Element root = doc.getDocumentElement();
            NodeList nl = root.getChildNodes();

            String sourceDBURL = null, targetDBURL = null;
            Node node;
            int i = 1;

            // Check to see if we have a sourceDBURL or targetDBURL prior to loadlist
            // NOTE: There is a #text node after every child node in the Document so
            // we need to skip over those to get the right children.
            if (i < nl.getLength()) {
                node = nl.item(i);
                if (node.getNodeName().equals("driver")) {
                    sDriverName = node.getFirstChild().getNodeValue();
                    i += 2;
                }
            }

            if (i < nl.getLength()) {
                node = nl.item(i);
                if (node.getNodeName().equals("sourcedb")) {
                    sourceDBURL = node.getFirstChild().getNodeValue();
                    i += 2;
                }
            }

            if (i < nl.getLength()) {
                node = nl.item(i);
                if (node.getNodeName().equals("targetdb")) {
                    targetDBURL = node.getFirstChild().getNodeValue();
                    i += 2;
                }
            }

            checkDriver();

            // Build new Hashtable for this load
            Hashtable params = new Hashtable();
            params.put("sourcedb", sourceDBURL);
            params.put("targetdb", targetDBURL);

            for (; i < nl.getLength(); i += 2) {
                Node n = nl.item(i);
                fillParams(params, n);
            }

            // Get start timestamp
            startTime = new java.sql.Timestamp(System.currentTimeMillis());

            // Run Pre Loader
            stage = "PRE";
            runTCLoad((String) params.get("preload"), params);

            // Run Individual Loaders
            stage = "LOAD";
            for (String className : (Set<String>) params.get("load")) {
                runTCLoad(className, params);
            }

            // Run Post Loaders
            stage = "POST";
            runTCLoad((String) params.get("postload"), params);

        } catch (Exception ex) {
            ex.printStackTrace();
            sErrorMsg.setLength(0);
            sErrorMsg.append("Load of XML file failed:\n");
            sErrorMsg.append(ex.getMessage());
            fatal_error(ex);
        }
    }

    /**
     * This method fills a Hashtable with the load class as well as any
     * parameters passed to the load. We then pass that off to
     * runTCLoad.
     */
    private static void fillParams(Hashtable params, Node node) throws Exception {
        if (node != null) {

            if (node.getNodeName().equals("preload")) {
                String className = node.getChildNodes().item(1).getFirstChild().getNodeValue();
                params.put("preload", className);
            }

            if (node.getNodeName().equals("postload")) {
                String className = node.getChildNodes().item(1).getFirstChild().getNodeValue();
                params.put("postload", className);
            }

            if (node.getNodeName().equals("load")) {
                NodeList loadChildren = node.getChildNodes();
                for (int i = 1; i < loadChildren.getLength(); i += 2) {
                    Node child = loadChildren.item(i);

                    // CHILD NODE: classList
                    if (child.getNodeName().equals("classList")) {
                        NodeList classList = child.getChildNodes();
                        Set<String> classes = new HashSet<>();

                        for (int j = 1; j < classList.getLength(); j += 2) {
                            if (!classList.item(j).getNodeName().equals("classname"))
                                continue;
                            String className = classList.item(j).getFirstChild().getNodeValue();
                            classes.add(className);
                        }
                        params.put("load", classes);
                    }

                    // CHILD NODE: parameterList
                    if (child.getNodeName().equals("parameterList")) {
                        NodeList classList = child.getChildNodes();
                        for (int j = 1; j < classList.getLength(); j += 2) {
                            NamedNodeMap attr = classList.item(j).getAttributes();
                            params.put(attr.getNamedItem("name").getNodeValue(),
                                    attr.getNamedItem("value").getNodeValue());
                        }
                    }
                }
            }

        }

    }

    /**
     * This method runs a particular load specified by loadclass and
     * with parameters specified in the params Hashtable.
     */
    private static void runTCLoad(String loadclass, Hashtable params) {
        if (loadclass == null) {
            sErrorMsg.setLength(0);
            sErrorMsg.append("Please specify a load to run using the -load option.");
            fatal_error(false);
        }

        Class loadme = null;
        try {
            loadme = Class.forName(loadclass);
        } catch (Exception ex) {
            sErrorMsg.setLength(0);
            sErrorMsg.append("Unable to load class for load: ");
            sErrorMsg.append(loadclass);
            sErrorMsg.append(". Cannot continue.\n");
            sErrorMsg.append(ex.getMessage());
            fatal_error(ex);
        }

        Object ob = null;
        try {
            ob = loadme.newInstance();
            if (ob == null)
                throw new Exception("Object is null after newInstance call.");
        } catch (Exception ex) {
            sErrorMsg.setLength(0);
            sErrorMsg.append("Unable to create new instance of class for load: ");
            sErrorMsg.append(loadclass);
            sErrorMsg.append(". Cannot continue.\n");
            sErrorMsg.append(ex.getMessage());
            fatal_error(ex);
        }

        if (!(ob instanceof TCLoad)) {
            sErrorMsg.setLength(0);
            sErrorMsg.append(loadclass + " is not an instance of TCLoad. You must ");
            sErrorMsg.append("extend TCLoad to create a TopCoder database load.");
            fatal_error(false);
        }

        TCLoad load = (TCLoad) ob;
        if (!load.setParameters(params)) {
            sErrorMsg.setLength(0);
            sErrorMsg.append(load.getReasonFailed());
            fatal_error(false);
        }

        setDatabases(load, params);
        try {
            doLoad(load);
        } catch (Exception e) {
            fatal_error(e);
        }

    }
    
    public static void doLoad(TCLoad tcload, String sourceDB, String targetDB) throws Exception {
        log.info("Creating source database connection...");
        Connection conn = DBMS.getConnection(sourceDB);
        PreparedStatement ps = conn.prepareStatement("set lock mode to wait 5");
        ps.execute();
        ps.close();
        tcload.setSourceConnection(conn);
        log.info("Success!");

        log.info("Creating target database connection...");
        Connection conn1 = DBMS.getConnection(targetDB);
        PreparedStatement ps1 = conn1.prepareStatement("set lock mode to wait 5");
        ps1.execute();
        ps1.close();
        tcload.setTargetConnection(conn1);
        log.info("Success!");
    
        try {
            tcload.performLoad();
        } catch (Exception e) {
            sErrorMsg.setLength(0);
            sErrorMsg.append(tcload.getReasonFailed());
            closeLoad(tcload);
            throw e;

        }
        closeLoad(tcload);
    }

    public static void doLoad(TCLoad tcload) throws Exception {
        try {
            log.info("Creating source database connection...");
            System.out.println(tcload.buildSourceDBConn());
            log.info("Success!");
        } catch (SQLException sqle) {
            sErrorMsg.setLength(0);
            sErrorMsg.append("Creation of source DB connection failed. ");
            sErrorMsg.append("Cannot continue.\n");
            sErrorMsg.append(sqle.getMessage());
            throw sqle;
        }

        try {
            log.info("Creating target database connection...");
            System.out.println(tcload.buildTargetDBConn());
            log.info("Success!");
        } catch (SQLException sqle2) {
            sErrorMsg.setLength(0);
            sErrorMsg.append("Creation of target DB connection failed. ");
            sErrorMsg.append("Cannot continue.\n");
            sErrorMsg.append(sqle2.getMessage());
            throw sqle2;
        }

        try {
            if (stage.equals("POST")) {
                tcload.setfStartTime(startTime);
            }
            else if (stage.equals("LOAD")) {
                tcload.setfLastLogTime(lastLogTime);
            }

            tcload.performLoad();

            if (stage.equals("PRE")) {
                lastLogTime = tcload.getfLastLogTime();
            }

        } catch (Exception e) {
            sErrorMsg.setLength(0);
            sErrorMsg.append(tcload.getReasonFailed());
            closeLoad(tcload);
            throw e;

        }
        closeLoad(tcload);
    }

    /**
     * This method converts an array of Strings into a Hashtable of
     * arguments. The arguments form keys seperated by a -. So, an
     * argument list of "-test one -test2 two" will create a Hashtable
     * with two keys, "test" and "test2" with corresponding values of
     * "one" and "two". The load is then passed the Hashtable and can
     * retrieve the arguments by name.
     */
    protected static Hashtable parseArgs(String[] args) {
        Hashtable hash = new Hashtable();
        for (int i = 0; i < args.length - 1; i += 2) {
            if (!args[i].startsWith("-")) {
                sErrorMsg.setLength(0);
                sErrorMsg.append("Argument " + (i + 1) + " (" + args[i] +
                        ") should start with a -.");
                fatal_error(true);
            }

            String key = args[i].substring(1);
            String value = args[i + 1];
            hash.put(key, value);
        }

        String tmp;
        tmp = (String) hash.get("driver");
        if (tmp != null) {
            sDriverName = tmp;
        }

        return hash;
    }

    protected static void closeLoad(TCLoad tcload) {
        try {
            tcload.closeDBConnections();
        } catch (SQLException sqle) {
            DBMS.printSqlException(true, sqle);
        }
    }

    protected static void setUsageError(String msg) {
        sErrorMsg.setLength(0);
        sErrorMsg.append(msg);
        sErrorMsg.append("TCLoadUtility parameters:\n");
        sErrorMsg.append("   -load class   : Classname of load to run.\n");
        sErrorMsg.append("   -sourcedb URL : URL of source database.\n");
        sErrorMsg.append("   -targetdb URL : URL of target database.\n");
        fatal_error(true);
    }

    protected static void setDatabases(TCLoad load, Hashtable params) {
        String tmp = (String) params.get("sourcedb");
        if (tmp == null)
            setUsageError("Please specify a source database.\n");

        load.setSourceDBURL(tmp);

        tmp = (String) params.get("targetdb");
        if (tmp == null)
            setUsageError("Please specify a target database.\n");

        load.setTargetDBURL(tmp);
    }

    private static void fatal_error(boolean exit) {
        log.error("*******************************************");
        log.error("FAILURE: " + sErrorMsg.toString());
        log.error("*******************************************");
        if (exit) System.exit(-1);
    }

    private static void fatal_error(Exception e) {
        log.error("*******************************************");
        log.error("FAILURE: ", e);
        log.error("*******************************************");
        // System.exit(-1);
    }

    /**
     * This method performs a Class.forName on the driver used for this
     * load. If it fails, the driver is not available and the load
     * fails.
     */
    private static void checkDriver() {
        try {
            Class.forName(sDriverName);
        } catch (Exception ex) {
            sErrorMsg.setLength(0);
            sErrorMsg.append("Unable to load driver ");
            sErrorMsg.append(sDriverName);
            sErrorMsg.append(". Cannot continue.");
            fatal_error(true);
        }
    }
}
