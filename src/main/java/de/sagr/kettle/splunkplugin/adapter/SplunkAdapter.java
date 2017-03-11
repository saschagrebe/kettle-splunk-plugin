package de.sagr.kettle.splunkplugin.adapter;

import com.splunk.*;
import org.pentaho.di.core.exception.KettleException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by grebe on 11.03.2017.
 */
public class SplunkAdapter {

    // splunk service
    private Service splunkService;

    public void init(final String host, final int port, final String username, final String password) {
        final ServiceArgs splunkCredentials = new ServiceArgs();
        splunkCredentials.setHost(host);
        splunkCredentials.setPort(port);
        splunkCredentials.setUsername(username);
        splunkCredentials.setPassword(password);
        splunkCredentials.setSSLSecurityProtocol(SSLSecurityProtocol.TLSv1_2);

        // connect to splunk service
        splunkService = Service.connect(splunkCredentials);
        if (splunkService == null) {
            throw new RuntimeException("Could not connect to splunk server!");
        }
    }

    public void search(final String searchQuery, final String[] fieldNames, final SearchResultEventProcessor eventProcessor) throws KettleException {
        final String actualQuery = SplunkQuery.search(searchQuery).build();

        final JobExportArgs exportArgs = new JobExportArgs();
        // set the fields to export
        exportArgs.put("f", fieldNames);

        // search splunk an process each event
        try (final InputStream exportSearch = splunkService.export(actualQuery, exportArgs)) {
            // Display results using the SDK's multi-results reader for XML
            final MultiResultsReaderXml multiResultsReader = new MultiResultsReaderXml(exportSearch);

            for (SearchResults searchResults : multiResultsReader) {
                for (Event event : searchResults) {
                    eventProcessor.processEvent(event);
                }
            }
            multiResultsReader.close();

        } catch(IOException e) {
            throw new KettleException(e);
        }
    }

    public Set<String> getSplunkFields(final String searchQuery) {
        // search for the given query and get the first 100 results as a table to retrieve all possible fields
        final String fieldQuery = SplunkQuery.search(searchQuery)
                .head(100)
                .table()
                .build();

        // search splunk an process each event
        try (final InputStream exportSearch = splunkService.export(fieldQuery)) {
            // Display results using the SDK's multi-results reader for XML
            final MultiResultsReaderXml multiResultsReader = new MultiResultsReaderXml(exportSearch);

            final Set<String> fieldNames = new TreeSet<>();
            for (SearchResults searchResults : multiResultsReader) {
                fieldNames.addAll(searchResults.getFields());
            }
            multiResultsReader.close();

            return fieldNames;
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

}
