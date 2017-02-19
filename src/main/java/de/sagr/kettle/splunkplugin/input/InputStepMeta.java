package de.sagr.kettle.splunkplugin.input;

import java.util.List;

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
public class InputStepMeta extends BaseStepMeta implements StepMetaInterface {

    private String splunkHost;
    private String splunkPort;
    private String splunkUsername;
    private String splunkPassword;
    private String splunkSearchQuery;

    private InputField[] inputFields;

    public InputStepMeta() {
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

    public InputField[] getInputFields() {
        return inputFields;
    }

    public void setInputFields(InputField[] inputFields) {
        this.inputFields = inputFields;
    }

    // set sensible defaults for a new step
    @Override
    public void setDefault() {
        splunkHost = "localhost";
        splunkPort = "8089";
        splunkUsername = "";
        splunkPassword = "";
        splunkSearchQuery = "";

        // default is to have no key input settings
        allocate(0);
    }

    // helper method to allocate the arrays
    public void allocate(int nrkeys) {
        this.inputFields = new InputField[nrkeys];
    }

    @Override
    public void getFields(RowMetaInterface inputRowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore) throws KettleStepException {
        if (inputFields == null) {
            return;
        }

        // append the outputFields to the output
        for (InputField nextField : inputFields) {
            final ValueMetaInterface valueMeta = nextField.toValueMeta(origin);
            inputRowMeta.addValueMeta(valueMeta);
        }
    }

    @Override
    public Object clone() {
        // field by field copy is default
        final InputStepMeta retval = (InputStepMeta) super.clone();

        // add proper deep copy for the collections
        final int nrKeys = inputFields.length;

        retval.allocate(nrKeys);

        for (int i = 0; i < nrKeys; i++) {
            retval.inputFields[i] = inputFields[i].clone();
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

        for (InputField nextField : inputFields) {
            retval.append(nextField.toXML());
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

            int nrKeys = XMLHandler.countNodes(stepnode, "input");
            allocate(nrKeys);

            for (int i = 0; i < nrKeys; i++) {
                final Node inputNode = XMLHandler.getSubNodeByNr(stepnode, "input", i);
                this.inputFields[i] = new InputField(inputNode);
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
                final InputField field = new InputField();
                field.setName(rep.getStepAttributeString(id_step, i, "lookup_field"));
                field.setDefaultValue(rep.getStepAttributeString(id_step, i, "lookup_default"));
                field.setType(ValueMetaBase.getType(rep.getStepAttributeString(id_step, i, "lookup_type")));
                field.setFormat(rep.getStepAttributeString(id_step, i, "lookup_format"));
                field.setDecimal(rep.getStepAttributeString(id_step, i, "lookup_decimal"));
                field.setGroup(rep.getStepAttributeString(id_step, i, "lookup_group"));
                field.setLength(Const.toInt(rep.getStepAttributeString(id_step, i, "lookup_length"), -1));
                field.setPrecision(Const.toInt(rep.getStepAttributeString(id_step, i, "lookup_precision"), -1));
                field.setCurrency(rep.getStepAttributeString(id_step, i, "lookup_currency"));

                this.inputFields[i] = field;

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

            for (int i = 0; i < inputFields.length; i++) {
                final InputField nextField = inputFields[i];
                rep.saveStepAttribute(id_transformation, id_step, i, "lookup_field", nextField.getName());
                rep.saveStepAttribute(id_transformation, id_step, i, "lookup_default", nextField.getDefaultValue());
                rep.saveStepAttribute(id_transformation, id_step, i, "lookup_type", ValueMetaBase.getTypeDesc(nextField.getType()));
                rep.saveStepAttribute(id_transformation, id_step, i, "lookup_format", nextField.getFormat());
                rep.saveStepAttribute(id_transformation, id_step, i, "lookup_decimal", nextField.getDecimal());
                rep.saveStepAttribute(id_transformation, id_step, i, "lookup_group", nextField.getGroup());
                rep.saveStepAttribute(id_transformation, id_step, i, "lookup_length", nextField.getLength());
                rep.saveStepAttribute(id_transformation, id_step, i, "lookup_precision", nextField.getPrecision());
                rep.saveStepAttribute(id_transformation, id_step, i, "lookup_currency", nextField.getCurrency());

            }

        } catch (Exception e) {
            throw new KettleException(getMessage("Step.Exception.UnableToSaveStepInfoToRepository") + id_step, e);
        }
    }

    @Override
    public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space, Repository repository, IMetaStore metaStore) {
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

            for (InputField nextField : inputFields) {
                final ValueMetaInterface v = prev.searchValueMeta(nextField.getName());
                if (v == null) {
                    if (first) {
                        first = false;
                        error_message += getMessage("Step.Check.MissingFieldsNotFoundInInput") + Const.CR;
                    }
                    error_found = true;
                    error_message += "\t\t" + nextField.getName() + Const.CR;
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
        return new InputStepDialog(shell, meta, transMeta, name);
    }

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta, Trans disp) {
        return new InputStep(stepMeta, stepDataInterface, cnr, transMeta, disp);
    }

    @Override
    public StepDataInterface getStepData() {
        return new InputStepData();
    }

    private String getMessage(final String key, final Object... param) {
        return BaseMessages.getString(InputStepMeta.class, key, param);
    }
}
