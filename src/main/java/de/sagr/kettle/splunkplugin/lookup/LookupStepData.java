package de.sagr.kettle.splunkplugin.lookup;

import com.splunk.Service;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class LookupStepData extends BaseStepData implements StepDataInterface {

	public RowMetaInterface outputRowMeta;
	
	// precomputed default objects
	public Object[] defaultObjects;

	// where the key field indexes are
	public int[] fieldIndex;

	// splunk service
	public Service splunkService;

	// meta info for a string conversion 
	public ValueMetaInterface[] conversionMeta;
	
    public LookupStepData()
	{
		super();
	}
}
	
