package de.sagr.kettle.splunkplugin.input;

import de.sagr.kettle.splunkplugin.adapter.SplunkAdapter;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

public class InputStep extends BaseStep {

    public InputStep(final StepMeta s, final StepDataInterface stepDataInterface, final int c, final TransMeta t, final Trans dis) {
        super(s, stepDataInterface, c, t, dis);
    }

    @Override
    public boolean processRow(final StepMetaInterface smi, final StepDataInterface sdi) throws KettleException {
        final InputStepMeta meta = (InputStepMeta) smi;
        final InputStepData data = (InputStepData) sdi;

        if (first) {
            first = false;
            initDefaultData(meta, data);
        }

        final InputStepSearchEventProcessor processor = new InputStepSearchEventProcessor(this, meta, data);
        data.adapter.search(meta.getSplunkSearchQuery(), meta.getInputFieldNames(), processor);

        // input step process rows is called once
        setOutputDone();
        return false;
    }

    private void initDefaultData(final InputStepMeta meta, final InputStepData data) throws KettleException {
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

    @Override
    public boolean init(final StepMetaInterface smi, final StepDataInterface sdi) {
        final InputStepMeta meta = (InputStepMeta) smi;
        final InputStepData data = (InputStepData) sdi;

        // connect to splunk service
        data.adapter = new SplunkAdapter();
        data.adapter.init(meta.getSplunkHost(), meta.getSplunkPort(), meta.getSplunkUsername(), meta.getSplunkPassword());

        return super.init(smi, sdi);
    }

    @Override
    public void dispose(final StepMetaInterface smi, final StepDataInterface sdi) {
        final InputStepData data = (InputStepData) sdi;

        data.adapter = null;

        super.dispose(smi, sdi);
    }

}
