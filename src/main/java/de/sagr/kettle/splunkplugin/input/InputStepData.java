package de.sagr.kettle.splunkplugin.input;

import de.sagr.kettle.splunkplugin.adapter.SplunkAdapter;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class InputStepData extends BaseStepData implements StepDataInterface {

	public RowMetaInterface outputRowMeta;
	
	// precomputed default objects
	public Object[] defaultObjects;

	// splunk adapter
	public SplunkAdapter adapter;

	// meta info for a string conversion 
	public ValueMetaInterface[] conversionMeta;

	public InputStepData()
	{
		super();
	}
}
	
