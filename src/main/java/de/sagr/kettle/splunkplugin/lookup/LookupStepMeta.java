package de.sagr.kettle.splunkplugin.lookup;

import java.util.List;
import java.util.Map;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.*;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.*;
import org.pentaho.di.core.row.*;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.*;
import org.pentaho.di.trans.*;
import org.pentaho.di.trans.step.*;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

@Step( id = "SplunkInput", image = "splunk-icon.png", name = "Splunk Input",
        description = "Execute a splunk search query and process the result", documentationUrl = "http://example.com/", categoryDescription = "Big Data" )
public class LookupStepMeta extends BaseStepMeta implements StepMetaInterface {

    private String splunkHost;
    private String splunkPort;
    private String splunkUsername;
    private String splunkPassword;
    private String splunkSearchQuery;

    private String fieldNames[];
    private String outputDefault[];
    private int outputType[];
    private String outputFormat[];
    private String outputCurrency[];
    private String outputDecimal[];
    private String outputGroup[];
    private int outputLength[];
    private int outputPrecision[];

    public LookupStepMeta() {
        super();
    }

    // getters and setters for the step settings

    public String getSplunkHost() {
        return splunkHost;
    }

    public void setSplunkHost(String splunkHost) {
        this.splunkHost = splunkHost;
    }

    public String getSplunkPort() {
        return splunkPort;
    }

    public void setSplunkPort(String splunkPort) {
        this.splunkPort = splunkPort;
    }

    public String getSplunkUsername() {
        return splunkUsername;
    }

    public void setSplunkUsername(String splunkUsername) {
        this.splunkUsername = splunkUsername;
    }

    public String getSplunkPassword() {
        return splunkPassword;
    }

    public void setSplunkPassword(String splunkPassword) {
        this.splunkPassword = splunkPassword;
    }

    public String getSplunkSearchQuery() {
        return splunkSearchQuery;
    }

    public void setSplunkSearchQuery(String splunkSearchQuery) {
        this.splunkSearchQuery = splunkSearchQuery;
    }

    public String[] getFieldNames() {
        return fieldNames;
    }

    public void setFieldNames(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    public String[] getOutputDefault() {
        return outputDefault;
    }

    public void setOutputDefault(String[] outputDefault) {
        this.outputDefault = outputDefault;
    }

    public int[] getOutputType() {
        return outputType;
    }

    public void setOutputType(int[] outputType) {
        this.outputType = outputType;
    }

    public String[] getOutputFormat() {
        return outputFormat;
    }

    public void setOutputFormat(String[] outputFormat) {
        this.outputFormat = outputFormat;
    }

    public String[] getOutputCurrency() {
        return outputCurrency;
    }

    public void setOutputCurrency(String[] outputCurrency) {
        this.outputCurrency = outputCurrency;
    }

    public String[] getOutputDecimal() {
        return outputDecimal;
    }

    public void setOutputDecimal(String[] outputDecimal) {
        this.outputDecimal = outputDecimal;
    }

    public String[] getOutputGroup() {
        return outputGroup;
    }

    public void setOutputGroup(String[] outputGroup) {
        this.outputGroup = outputGroup;
    }

    public int[] getOutputLength() {
        return outputLength;
    }

    public void setOutputLength(int[] outputLength) {
        this.outputLength = outputLength;
    }

    public int[] getOutputPrecision() {
        return outputPrecision;
    }

    public void setOutputPrecision(int[] outputPrecision) {
        this.outputPrecision = outputPrecision;
    }

    // set sensible defaults for a new step
    @Override
    public void setDefault() {
        splunkHost = "localhost";
        splunkPort = "8089";
        splunkUsername = "";
        splunkPassword = "";
        splunkSearchQuery = "";

        // default is to have no key lookup settings
        allocate(0);
    }

    // helper method to allocate the arrays
    public void allocate(int nrkeys) {
        fieldNames = new String[nrkeys];
        outputDefault = new String[nrkeys];
        outputType = new int[nrkeys];
        outputFormat = new String[nrkeys];
        outputDecimal = new String[nrkeys];
        outputGroup = new String[nrkeys];

        outputLength = new int[nrkeys];

        outputPrecision = new int[nrkeys];
        outputCurrency = new String[nrkeys];

    }

    @Override
    public void getFields(RowMetaInterface inputRowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore) throws KettleStepException {
        if (fieldNames == null) {
            return;
        }

        // append the outputFields to the output
        for (int i = 0; i < fieldNames.length; i++) {
            final ValueMetaInterface valueMeta = new ValueMetaBase(fieldNames[i], outputType[i]);
            valueMeta.setLength(outputLength[i]);
            valueMeta.setPrecision(outputPrecision[i]);
            valueMeta.setCurrencySymbol(outputCurrency[i]);
            valueMeta.setConversionMask(outputFormat[i]);
            valueMeta.setDecimalSymbol(outputDecimal[i]);
            valueMeta.setGroupingSymbol(outputGroup[i]);

            valueMeta.setOrigin(origin);
            inputRowMeta.addValueMeta(valueMeta);
        }
    }

    @Override
    public Object clone() {
        // field by field copy is default
        final LookupStepMeta retval = (LookupStepMeta) super.clone();

        // add proper deep copy for the collections
        final int nrKeys = fieldNames.length;

        retval.allocate(nrKeys);

        for (int i = 0; i < nrKeys; i++) {
            retval.fieldNames[i] = fieldNames[i];
            retval.outputDefault[i] = outputDefault[i];
            retval.outputType[i] = outputType[i];
            retval.outputCurrency[i] = outputCurrency[i];
            retval.outputDecimal[i] = outputDecimal[i];
            retval.outputFormat[i] = outputFormat[i];
            retval.outputGroup[i] = outputGroup[i];
            retval.outputLength[i] = outputLength[i];
            retval.outputPrecision[i] = outputPrecision[i];
        }

        return retval;
    }

    @Override
    public String getXML() throws KettleValueException {

        StringBuffer retval = new StringBuffer(150);

        retval.append("    ").append(XMLHandler.addTagValue("host", splunkHost));
        retval.append("    ").append(XMLHandler.addTagValue("port", splunkPort));
        retval.append("    ").append(XMLHandler.addTagValue("username", splunkUsername));
        retval.append("    ").append(XMLHandler.addTagValue("password", splunkPassword));
        retval.append("    ").append(XMLHandler.addTagValue("searchQuery", splunkSearchQuery));

        for (int i = 0; i < fieldNames.length; i++) {
            retval.append("      <lookup>").append(Const.CR);
            retval.append("        ").append(XMLHandler.addTagValue("fieldName", fieldNames[i]));
            retval.append("        ").append(XMLHandler.addTagValue("default", outputDefault[i]));
            retval.append("        ").append(XMLHandler.addTagValue("type", ValueMetaBase.getTypeDesc(outputType[i])));
            retval.append("        ").append(XMLHandler.addTagValue("format", outputFormat[i]));
            retval.append("        ").append(XMLHandler.addTagValue("decimal", outputDecimal[i]));
            retval.append("        ").append(XMLHandler.addTagValue("group", outputGroup[i]));
            retval.append("        ").append(XMLHandler.addTagValue("length", outputLength[i]));
            retval.append("        ").append(XMLHandler.addTagValue("precision", outputPrecision[i]));
            retval.append("        ").append(XMLHandler.addTagValue("currency", outputCurrency[i]));

            retval.append("      </lookup>").append(Const.CR);
        }

        return retval.toString();
    }

    @Override
    public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {
        try {
            splunkHost = XMLHandler.getTagValue(stepnode, "host");
            splunkPort = XMLHandler.getTagValue(stepnode, "port");
            splunkUsername = XMLHandler.getTagValue(stepnode, "username");
            splunkPassword = XMLHandler.getTagValue(stepnode, "password");
            splunkSearchQuery = XMLHandler.getTagValue(stepnode, "searchQuery");

            int nrKeys = XMLHandler.countNodes(stepnode, "lookup");
            allocate(nrKeys);

            for (int i = 0; i < nrKeys; i++) {
                Node knode = XMLHandler.getSubNodeByNr(stepnode, "lookup", i);

                fieldNames[i] = XMLHandler.getTagValue(knode, "fieldName");
                outputDefault[i] = XMLHandler.getTagValue(knode, "default");
                outputType[i] = ValueMetaBase.getType(XMLHandler.getTagValue(knode, "type"));
                outputFormat[i] = XMLHandler.getTagValue(knode, "format");
                outputDecimal[i] = XMLHandler.getTagValue(knode, "decimal");
                outputGroup[i] = XMLHandler.getTagValue(knode, "group");
                outputLength[i] = Const.toInt(XMLHandler.getTagValue(knode, "length"), -1);
                outputPrecision[i] = Const.toInt(XMLHandler.getTagValue(knode, "precision"), -1);
                outputCurrency[i] = XMLHandler.getTagValue(knode, "currency");

                if (outputType[i] < 0) {
                    outputType[i] = ValueMetaInterface.TYPE_STRING;
                }

            }

        } catch (Exception e) {
            throw new KettleXMLException("Template Plugin Unable to read step info from XML node", e);
        }

    }

    @Override
    public void readRep(Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases) throws KettleException {
        try {
            splunkHost = rep.getStepAttributeString(id_step, "host");
            splunkPort = rep.getStepAttributeString(id_step, "port");
            splunkUsername = rep.getStepAttributeString(id_step, "username");
            splunkPassword = rep.getStepAttributeString(id_step, "password");
            splunkSearchQuery = rep.getStepAttributeString(id_step, "searchQuery");

            int nrKeys = rep.countNrStepAttributes(id_step, "lookup_keyfield");
            allocate(nrKeys);

            for (int i = 0; i < nrKeys; i++) {
                fieldNames[i] = rep.getStepAttributeString(id_step, i, "lookup_field");
                outputDefault[i] = rep.getStepAttributeString(id_step, i, "lookup_default");
                outputType[i] = ValueMetaBase.getType(rep.getStepAttributeString(id_step, i, "lookup_type"));
                outputFormat[i] = rep.getStepAttributeString(id_step, i, "lookup_format");
                outputDecimal[i] = rep.getStepAttributeString(id_step, i, "lookup_decimal");
                outputGroup[i] = rep.getStepAttributeString(id_step, i, "lookup_group");
                outputLength[i] = Const.toInt(rep.getStepAttributeString(id_step, i, "lookup_length"), -1);
                outputPrecision[i] = Const.toInt(rep.getStepAttributeString(id_step, i, "lookup_precision"), -1);
                outputCurrency[i] = rep.getStepAttributeString(id_step, i, "lookup_currency");

            }

        } catch (Exception e) {
            throw new KettleException(getMessage("Step.Exception.UnexpectedErrorInReadingStepInfo"), e);
        }
    }

    @Override
    public void saveRep(Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step) throws KettleException {
        try {
            rep.saveStepAttribute(id_transformation, id_step, "host", splunkHost);
            rep.saveStepAttribute(id_transformation, id_step, "port", splunkPort);
            rep.saveStepAttribute(id_transformation, id_step, "username", splunkUsername);
            rep.saveStepAttribute(id_transformation, id_step, "password", splunkPassword);
            rep.saveStepAttribute(id_transformation, id_step, "searchQuery", splunkSearchQuery);

            for (int i = 0; i < fieldNames.length; i++) {
                rep.saveStepAttribute(id_transformation, id_step, i, "lookup_field", fieldNames[i]);
                rep.saveStepAttribute(id_transformation, id_step, i, "lookup_default", outputDefault[i]);
                rep.saveStepAttribute(id_transformation, id_step, i, "lookup_type", ValueMetaBase.getTypeDesc(outputType[i]));
                rep.saveStepAttribute(id_transformation, id_step, i, "lookup_format", outputFormat[i]);
                rep.saveStepAttribute(id_transformation, id_step, i, "lookup_decimal", outputDecimal[i]);
                rep.saveStepAttribute(id_transformation, id_step, i, "lookup_group", outputGroup[i]);
                rep.saveStepAttribute(id_transformation, id_step, i, "lookup_length", outputLength[i]);
                rep.saveStepAttribute(id_transformation, id_step, i, "lookup_precision", outputPrecision[i]);
                rep.saveStepAttribute(id_transformation, id_step, i, "lookup_currency", outputCurrency[i]);

            }

        } catch (Exception e) {
            throw new KettleException(getMessage("Step.Exception.UnableToSaveStepInfoToRepository") + id_step, e);
        }
    }

    @Override
    public void check(List<CheckResultInterface> remarks, TransMeta transmeta, StepMeta stepMeta, RowMetaInterface prev, String input[], String output[], RowMetaInterface info) {
        CheckResult cr;

        // See if we have input streams leading to this step!
        if (input.length > 0) {
            cr = new CheckResult(CheckResult.TYPE_RESULT_OK, getMessage("Step.Check.StepIsReceivingInfoFromOtherSteps"), stepMeta);
            remarks.add(cr);
        } else {
            cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, getMessage("Step.Check.NoInputReceivedFromOtherSteps"), stepMeta);
            remarks.add(cr);
        }

        // also check that each expected key fields are acually coming
        if (prev != null && prev.size() > 0) {
            boolean first = true;
            String error_message = "";
            boolean error_found = false;

            for (int i = 0; i < fieldNames.length; i++) {
                ValueMetaInterface v = prev.searchValueMeta(fieldNames[i]);
                if (v == null) {
                    if (first) {
                        first = false;
                        error_message += getMessage("Step.Check.MissingFieldsNotFoundInInput") + Const.CR;
                    }
                    error_found = true;
                    error_message += "\t\t" + fieldNames[i] + Const.CR;
                }
            }
            if (error_found) {
                cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
            } else {
                cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, getMessage("Step.Check.AllFieldsFoundInInput"), stepMeta);
            }
            remarks.add(cr);
        } else {
            String error_message = getMessage("Step.Check.CouldNotReadFromPreviousSteps") + Const.CR;
            cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
            remarks.add(cr);
        }
    }

    public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name) {
        return new LookupStepDialog(shell, meta, transMeta, name);
    }

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta, Trans disp) {
        return new LookupStep(stepMeta, stepDataInterface, cnr, transMeta, disp);
    }

    @Override
    public StepDataInterface getStepData() {
        return new LookupStepData();
    }

    private String getMessage(final String key, final Object... param) {
        return BaseMessages.getString(LookupStepMeta.class, key, param);
    }
}
