# Description 
This project demonstrates how to use SolrJ to create parent and child docs, as well as how to use the Block Join query parser to retrieve the parent based on queries matching the child document.

In order to execute the SolrApp.main() method, you will need to create a SOLR (v4.8 or greater)server (mine currently runs on port 8088) and ensure that the schema.xml contains id, text and ATTRIBUTEs.* (wildcard) fields.
