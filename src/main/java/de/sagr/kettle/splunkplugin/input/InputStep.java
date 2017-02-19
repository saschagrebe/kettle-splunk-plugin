package de.sagr.kettle.splunkplugin.input;

import com.splunk.*;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import java.io.IOException;
import java.io.InputStream;

public class InputStep extends BaseStep implements StepInterface {

    private InputStepData data;

    private InputStepMeta meta;

    public InputStep(final StepMeta s, final StepDataInterface stepDataInterface, final int c, final TransMeta t, final Trans dis) {
        super(s, stepDataInterface, c, t, dis);
    }

    @Override
    public boolean processRow(final StepMetaInterface smi, final StepDataInterface sdi) throws KettleException {

        meta = (InputStepMeta) smi;
        data = (InputStepData) sdi;

        if (first) {
            first = false;

            // determine output field structure
            data.outputRowMeta = new RowMeta();
            meta.getFields(data.outputRowMeta, getStepname(), null, null, this, null, null);
            logRowlevel(data.outputRowMeta.toStringMeta());

            // stores default values in correct format
            data.defaultObjects = new Object[meta.getInputFields().length];

            // stores the indices where to look for the key fields in the input rows
            data.conversionMeta = new ValueMetaInterface[meta.getInputFields().length];

            for (int i = 0; i < meta.getInputFields().length; i++) {

                // get output and from-string conversion format for each field
                final ValueMetaInterface returnMeta = data.outputRowMeta.getValueMeta(i);

                final ValueMetaInterface conversionMeta = returnMeta.clone();
                conversionMeta.setType(ValueMetaInterface.TYPE_STRING);
                data.conversionMeta[i] = conversionMeta;

                // calculate default values
                final InputField nextField = meta.getInputFields()[i];
                if (!Utils.isEmpty(nextField.getDefaultValue()) && returnMeta.getType() != 0) {
                    data.defaultObjects[i] = returnMeta.convertData(conversionMeta, nextField.getDefaultValue());

                } else {
                    data.defaultObjects[i] = null;

                }
            }

            logRowlevel(data.conversionMeta.toString());
        }

        // search splunk an process each event
        try (final InputStream exportSearch = data.splunkService.export("search " + meta.getSplunkSearchQuery())) {
            // Display results using the SDK's multi-results reader for XML
            final MultiResultsReaderXml multiResultsReader = new MultiResultsReaderXml(exportSearch);

            for (SearchResults searchResults : multiResultsReader) {
                for (Event event : searchResults) {
                    // generate output row, make it correct size
                    final Object[] outputRow = new Object[data.outputRowMeta.size()];

                    // fill the output fields with look up data
                    for (int i = 0; i < meta.getInputFields().length; i++) {
                        final InputField nextField = meta.getInputFields()[i];
                        final String nextFieldName = nextField.getName();
                        final String splunkValue = event.get(nextFieldName);

                        // if nothing is there, return the default
                        if (splunkValue == null) {
                            outputRow[i] = data.defaultObjects[i];
                        }
                        // else convert the value to desired format
                        else {
                            final ValueMetaInterface valueMeta = data.outputRowMeta.getValueMeta(i);
                            logRowlevel(valueMeta.toStringMeta());
                            outputRow[i] = valueMeta.convertData(data.conversionMeta[i], splunkValue);
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
        meta = (InputStepMeta) smi;
        data = (InputStepData) sdi;

        final ServiceArgs splunkCredentials = new ServiceArgs();
        splunkCredentials.setHost(meta.getSplunkHost());
        splunkCredentials.setPort(Integer.parseInt(meta.getSplunkPort()));
        splunkCredentials.setUsername(meta.getSplunkUsername());
        splunkCredentials.setPassword(meta.getSplunkPassword());
        splunkCredentials.setSSLSecurityProtocol(SSLSecurityProtocol.TLSv1_2);

        // connect to splunk service
        data.splunkService = Service.connect(splunkCredentials);
        if (data.splunkService == null) {
            return false;
        }

        return super.init(smi, sdi);
    }

    @Override
    public void dispose(final StepMetaInterface smi, final StepDataInterface sdi) {
        meta = (InputStepMeta) smi;
        data = (InputStepData) sdi;

        data.splunkService = null;

        super.dispose(smi, sdi);
    }

    private String getMessage(final String key, final Object... param) {
        return BaseMessages.getString(InputStep.class, key, param);
    }
}
