package de.sagr.kettle.splunkplugin.adapter;

import com.splunk.Event;
import org.pentaho.di.core.exception.KettleException;

/**
 * Created by grebe on 11.03.2017.
 */
public interface SearchResultEventProcessor {

    void processEvent(Event event) throws KettleException;

}
