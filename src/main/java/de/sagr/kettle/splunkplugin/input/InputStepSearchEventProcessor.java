package de.sagr.kettle.splunkplugin.input;

import com.splunk.Event;
import de.sagr.kettle.splunkplugin.adapter.SearchResultEventProcessor;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.StringUtil;

import java.util.regex.Matcher;

/**
 * Created by grebe on 11.03.2017.
 */
public class InputStepSearchEventProcessor implements SearchResultEventProcessor {

    private final InputStep inputStep;

    private final InputStepMeta meta;

    private final InputStepData data;

    public InputStepSearchEventProcessor(final InputStep inputStep, final InputStepMeta meta, final InputStepData data) {
        this.inputStep = inputStep;
        this.meta = meta;
        this.data = data;
    }

    @Override
    public void processEvent(Event event) throws KettleException {
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
                outputRow[i] = convertData(i, nextField, splunkValue);
            }
        }

        // copy row to possible alternate rowset(s)
        inputStep.putRow(data.outputRowMeta, outputRow);
    }

    private Object convertData(final int i, final InputField nextField, final String splunkValue) throws KettleException {
        String value;
        if (!StringUtil.isEmpty(nextField.getRegExp())) {
            final Matcher matcher = nextField.getPattern().matcher(splunkValue);
            if (matcher.find()) {
                value = matcher.group();
            } else {
                value = splunkValue;
            }
        } else {
            value = splunkValue;
        }

        final ValueMetaInterface valueMeta = data.outputRowMeta.getValueMeta(i);
        inputStep.logRowlevel(valueMeta.toStringMeta());
        return valueMeta.convertData(data.conversionMeta[i], value);
    }

}
