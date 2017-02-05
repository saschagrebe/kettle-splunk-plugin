package de.sagr.kettle.splunkplugin.lookup;

import com.splunk.*;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import java.io.IOException;
import java.io.InputStream;

public class LookupStep extends BaseStep implements StepInterface {

    private LookupStepData data;

    private LookupStepMeta meta;

    public LookupStep(final StepMeta s, final StepDataInterface stepDataInterface, final int c, final TransMeta t, final Trans dis) {
        super(s, stepDataInterface, c, t, dis);
    }

    @Override
    public boolean processRow(final StepMetaInterface smi, final StepDataInterface sdi) throws KettleException {

        meta = (LookupStepMeta) smi;
        data = (LookupStepData) sdi;

        if (first) {
            first = false;

            // determine output field structure
            data.outputRowMeta = (RowMetaInterface) getInputRowMeta().clone();
            meta.getFields(data.outputRowMeta, getStepname(), null, null, this);

            // stores default values in correct format
            data.defaultObjects = new Object[meta.getFieldNames().length];

            // stores the indices where to look for the key fields in the input rows
            data.fieldIndex = new int[meta.getFieldNames().length];
            data.conversionMeta = new ValueMetaInterface[meta.getFieldNames().length];

            for (int i = 0; i < meta.getFieldNames().length; i++) {

                // get output and from-string conversion format for each field
                final ValueMetaInterface returnMeta = data.outputRowMeta.getValueMeta(i);

                final ValueMetaInterface conversionMeta = returnMeta.clone();
                conversionMeta.setType(ValueMetaInterface.TYPE_STRING);
                data.conversionMeta[i] = conversionMeta;

                // calculate default values
                if (!Const.isEmpty(meta.getOutputDefault()[i])) {
                    data.defaultObjects[i] = returnMeta.convertData(data.conversionMeta[i], meta.getOutputDefault()[i]);

                } else {
                    data.defaultObjects[i] = null;

                }

                // calc key field indices
                data.fieldIndex[i] = data.outputRowMeta.indexOfValue(meta.getFieldNames()[i]);
                if (data.fieldIndex[i] < 0) {
                    throw new KettleStepException(getMessage("Step.Error.UnableFindField", meta.getFieldNames()[i], "" + (i + 1)));
                }

            }

        }

        // search splunk an process each event
        try (final InputStream exportSearch = data.splunkService.export(meta.getSplunkSearchQuery())) {
            // Display results using the SDK's multi-results reader for XML
            final MultiResultsReaderXml multiResultsReader = new MultiResultsReaderXml(exportSearch);

            for (SearchResults searchResults : multiResultsReader) {
                for (Event event : searchResults) {
                    // generate output row, make it correct size
                    final Object[] outputRow = new Object[data.outputRowMeta.size()];

                    // fill the output fields with look up data
                    for (int i = 0; i < meta.getFieldNames().length; i++) {
                        final String nextFieldName = meta.getFieldNames()[i];
                        final String splunkValue = event.get(nextFieldName);

                        // if nothing is there, return the default
                        if (splunkValue == null) {
                            outputRow[i] = data.defaultObjects[i];
                        }
                        // else convert the value to desired format
                        else {
                            outputRow[i] = data.outputRowMeta.getValueMeta(i).convertData(data.conversionMeta[i], splunkValue);
                        }
                    }

                    // copy row to possible alternate rowset(s)
                    putRow(data.outputRowMeta, outputRow);
                }
            }
            multiResultsReader.close();

        } catch(IOException e) {
            throw new KettleException(e);
        }

        // input step process rows is called once
        setOutputDone();
        return false;
    }

    @Override
    public boolean init(final StepMetaInterface smi, final StepDataInterface sdi) {
        meta = (LookupStepMeta) smi;
        data = (LookupStepData) sdi;

        final ServiceArgs splunkCredentials = new ServiceArgs();
        splunkCredentials.setHost(meta.getSplunkHost());
        splunkCredentials.setPort(Integer.parseInt(meta.getSplunkPort()));
        splunkCredentials.setUsername(meta.getSplunkUsername());
        splunkCredentials.setPassword(meta.getSplunkPassword());

        // connect to splunk service
        data.splunkService = Service.connect(splunkCredentials);
        if (data.splunkService == null) {
            return false;
        }

        return super.init(smi, sdi);
    }

    @Override
    public void dispose(final StepMetaInterface smi, final StepDataInterface sdi) {
        meta = (LookupStepMeta) smi;
        data = (LookupStepData) sdi;

        data.splunkService = null;

        super.dispose(smi, sdi);
    }

    private String getMessage(final String key, final Object... param) {
        return BaseMessages.getString(LookupStep.class, key, param);
    }
}
