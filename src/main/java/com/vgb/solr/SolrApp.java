package com.vgb.solr;


import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.common.SolrInputDocument;

import java.net.MalformedURLException;

import static org.apache.solr.client.solrj.request.CoreAdminRequest.getStatus;

public class SolrApp {
    private static String url = "http://localhost:8088/solr";
    private static String core = "test_core";
    private static String instanceDir = "cores";
    private static String FWD_SLASH = "/";
    private static final Logger log = Logger.getLogger(SolrApp.class);

    public static void main(String[] args) throws Exception {
        reset(core);

        final SolrServer updateServer = buildCommonsServer(core);
        final String documentId = "1";

        //create main doc
        final SolrInputDocument parentDoc = new SolrInputDocument();
        parentDoc.addField("id", documentId);
        parentDoc.addField("content_type", "parentDocument");
        parentDoc.addField("text", "eric clapton was the guitarist for the rock band, cream.");

        //create child doc
        final SolrInputDocument childDoc = new SolrInputDocument();
        childDoc.addField("id", documentId + "-A");
        childDoc.addField("ATTRIBUTES.STATE", "LA");
        childDoc.addField("ATTRIBUTES.STATE", "TX");

        //associate parent and child doc
        parentDoc.addChildDocument(childDoc);

        //add parent doc (and child) to SOLR
        updateServer.add(parentDoc);
        updateServer.commit();

//        //Question: is it also possible to update the child doc separately ?
//        childDoc.addField("ATTRIBUTES.STATE", "OK");
//        childDoc.addField("ATTRIBUTES.PARTY", "GOP");
//        updateServer.add(childDoc);
//        updateServer.commit();

        //run some queries
        SolrQuery parameters = new SolrQuery();
        parameters.set("q", "*:*");
        System.out.println("Query for something matching any doc: " + updateServer.query(parameters));

        parameters = new SolrQuery();
        parameters.set("q", "text:eric");
        System.out.println("Query for something matching only the parent doc: " + updateServer.query(parameters));

        parameters = new SolrQuery();
        parameters.set("q", "ATTRIBUTES.STATE:TX");
        System.out.println("Query for something matching only the child doc: " + updateServer.query(parameters));


        parameters = new SolrQuery();
        parameters.set("q", "{!parent which=\"content_type:parentDocument\"}ATTRIBUTES.STATE:TX");
        System.out.println("Query to retrieve the parent using Block Join for something matching only the child doc: " + updateServer.query(parameters));

        //terminate the server
        updateServer.shutdown();


    }


    private static int reset(String coreName)
            throws Exception {

        SolrServer adminServer = buildCommonsAdminServer();
        boolean coreExists = pingCore(coreName, adminServer);
        if (coreExists) {
            return truncate(coreName);
        }

        return create(coreName);
    }

    private static int create(String coreName)
            throws Exception {
        CoreAdminRequest.Create req = new CoreAdminRequest.Create();
        req.setCoreName(coreName);
        req.setInstanceDir(instanceDir);
        req.setDataDir(coreName);
        req.setIsTransient(false);
        req.setIsLoadOnStartup(true);
        SolrServer adminServer = buildCommonsAdminServer();

        boolean coreExists = pingCore(coreName, adminServer);
        if (coreExists) {
            return 1;
        }
        return req.process(adminServer).getStatus();
    }

    private static SolrServer buildCommonsAdminServer()
            throws MalformedURLException {
        return new HttpSolrServer(url);
    }

    private static boolean pingCore(String coreName, SolrServer adminServer) {

        try {
            CoreAdminResponse statusResponse = getStatus(coreName, adminServer);
            return statusResponse.getCoreStatus(coreName) != null && statusResponse.getCoreStatus(coreName).size() > 0;
        } catch (Exception e) {
            log.info("Cannot ping core: " + coreName + " : " + e.getMessage());
            return false;
        }
    }

    private static int truncate(String coreName)
            throws Exception {
        SolrServer adminServer = buildCommonsAdminServer();

        boolean coreExists = pingCore(coreName, adminServer);
        if (!coreExists) {
            return 1;
        }
        SolrServer coreServer = buildCommonsServer(coreName);
        int retVal = coreServer.deleteByQuery("*:*").getStatus();
        coreServer.commit();
        return retVal;
    }

    private static SolrServer buildCommonsServer(String coreName)
            throws MalformedURLException {
        final SolrServer solrServer = new HttpSolrServer(buildServerURL(coreName));
        //solrServer.setParser(new BinaryResponseParser());
        return solrServer;
    }

    private static String buildServerURL(String coreName) {
        return url + FWD_SLASH + coreName;
    }
}
