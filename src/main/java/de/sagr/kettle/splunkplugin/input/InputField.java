package de.sagr.kettle.splunkplugin.input;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.xml.XMLHandler;
import org.w3c.dom.Node;

/**
 * Created by grebe on 19.02.2017.
 */
public class InputField {

    private String name;

    private String outputName;
    private String defaultValue;
    private int type;
    private String format;
    private String currency;
    private String decimal;
    private String group;
    private int length;
    private int precision;

    public InputField() {
        // default
    }

    public InputField(Node knode) {
        this.name = XMLHandler.getTagValue(knode, "fieldName");
        this.outputName = XMLHandler.getTagValue(knode, "outputName");
        this.defaultValue = XMLHandler.getTagValue(knode, "default");
        this.type = ValueMetaBase.getType(XMLHandler.getTagValue(knode, "type"));
        this.format = XMLHandler.getTagValue(knode, "format");
        this.decimal = XMLHandler.getTagValue(knode, "decimal");
        this.group = XMLHandler.getTagValue(knode, "group");
        this.length = Const.toInt(XMLHandler.getTagValue(knode, "length"), -1);
        this.precision = Const.toInt(XMLHandler.getTagValue(knode, "precision"), -1);
        this.currency = XMLHandler.getTagValue(knode, "currency");

        if (this.type < 0) {
            this.type = ValueMetaInterface.TYPE_STRING;
        }
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getDecimal() {
        return decimal;
    }

    public void setDecimal(String decimal) {
        this.decimal = decimal;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getOutputName() {
        return outputName;
    }

    public void setOutputName(String outputName) {
        this.outputName = outputName;
    }

    public ValueMetaInterface toValueMeta(String origin) {
        final ValueMetaInterface valueMeta = new ValueMetaBase(name, type);
        if (!StringUtil.isEmpty(outputName)) {
            valueMeta.setName(outputName);
        }
        valueMeta.setLength(length);
        valueMeta.setPrecision(precision);
        valueMeta.setCurrencySymbol(currency);
        valueMeta.setConversionMask(format);
        valueMeta.setDecimalSymbol(decimal);
        valueMeta.setGroupingSymbol(group);
        valueMeta.setOrigin(origin);

        return valueMeta;
    }

    public InputField clone() {
        final InputField clone = new InputField();
        clone.setName(name);
        clone.setOutputName(outputName);
        clone.setDefaultValue(defaultValue);
        clone.setType(type);
        clone.setCurrency(currency);
        clone.setDecimal(decimal);
        clone.setFormat(format);
        clone.setGroup(group);
        clone.setLength(length);
        clone.setPrecision(precision);

        return clone;
    }

    public String toXML() {
        final StringBuilder builder = new StringBuilder();
        builder.append("      <input>").append(Const.CR);
        builder.append("        ").append(XMLHandler.addTagValue("fieldName", name));
        builder.append("        ").append(XMLHandler.addTagValue("outputName", outputName));
        builder.append("        ").append(XMLHandler.addTagValue("default", defaultValue));
        builder.append("        ").append(XMLHandler.addTagValue("type", ValueMetaBase.getTypeDesc(type)));
        builder.append("        ").append(XMLHandler.addTagValue("format", format));
        builder.append("        ").append(XMLHandler.addTagValue("decimal", decimal));
        builder.append("        ").append(XMLHandler.addTagValue("group", group));
        builder.append("        ").append(XMLHandler.addTagValue("length", length));
        builder.append("        ").append(XMLHandler.addTagValue("precision", precision));
        builder.append("        ").append(XMLHandler.addTagValue("currency", currency));
        builder.append("      </input>").append(Const.CR);

        return builder.toString();
    }

}
