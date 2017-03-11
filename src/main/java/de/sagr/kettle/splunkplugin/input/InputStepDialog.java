package de.sagr.kettle.splunkplugin.input;

import java.util.Arrays;
import java.util.Set;

import com.sun.rowset.internal.InsertRow;
import de.sagr.kettle.splunkplugin.adapter.SplunkAdapter;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;

public class InputStepDialog extends BaseStepDialog implements StepDialogInterface {

    private InputStepMeta input;

    // connection settings widgets
    private Group gConnect;
    private Label wlHost;
    private TextVar wHost;
    private Label wlPort;
    private TextVar wPort;
    private Label wlUsername;
    private TextVar wUsername;
    private Label wlPassword;
    private TextVar wPassword;

    // search commands
    private Label wlSearchQuery;
    private TextVar wSearchQuery;

    // input fields settings widgets
    private Label wlFields;
    private TableView wFields;

    // the dropdown column which should contain previous fields from stream
    private ColumnInfo fieldColumn = null;

    // constructor
    public InputStepDialog(Shell parent, Object in, TransMeta transMeta, String sname) {
        super(parent, (BaseStepMeta) in, transMeta, sname);
        input = (InputStepMeta) in;
    }

    // builds and shows the dialog
    @Override
    public String open() {
        shell = new Shell(getParent(), SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
        props.setLook(shell);
        setShellImage(shell, input);

        final ModifyListener lsMod = new ModifyListener() {
            public void modifyText(ModifyEvent e) {
                input.setChanged();
            }
        };
        backupChanged = input.hasChanged();

        final FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;

        shell.setLayout(formLayout);
        shell.setText(getMessage("Dialog.Shell.Title"));

        final int middle = props.getMiddlePct();
        final int margin = Const.MARGIN;

        // layout form
        layoutStepName(lsMod, middle, margin);
        layoutSplunkConnection(lsMod, middle, margin);
        layoutKeyTable(lsMod, margin);
        layoutButtons(margin);
        addDefaultListeners();

        return openDialog();
    }

    private void layoutStepName(final ModifyListener lsMod, final int middle, final int margin) {
        // Stepname line
        wlStepname = new Label(shell, SWT.RIGHT);
        wlStepname.setText(getMessage("System.Label.StepName"));
        props.setLook(wlStepname);
        fdlStepname = new FormData();
        fdlStepname.left = new FormAttachment(0, 0);
        fdlStepname.right = new FormAttachment(middle, -margin);
        fdlStepname.top = new FormAttachment(0, margin);
        wlStepname.setLayoutData(fdlStepname);

        wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        wStepname.setText(stepname);
        props.setLook(wStepname);
        wStepname.addModifyListener(lsMod);
        fdStepname = new FormData();
        fdStepname.left = new FormAttachment(middle, 0);
        fdStepname.top = new FormAttachment(0, margin);
        fdStepname.right = new FormAttachment(100, 0);
        wStepname.setLayoutData(fdStepname);
    }

    public void layoutSplunkConnection(final ModifyListener lsMod, final int middle, final int margin) {
        gConnect = new Group(shell, SWT.SHADOW_ETCHED_IN);
        gConnect.setText(getMessage("Dialog.ConnectGroup.Label"));

        final FormLayout gConnectLayout = new FormLayout();
        gConnectLayout.marginWidth = 3;
        gConnectLayout.marginHeight = 3;
        gConnect.setLayout(gConnectLayout);
        props.setLook(gConnect);

        // Host
        wlHost = new Label(gConnect, SWT.RIGHT);
        wlHost.setText(getMessage("Dialog.ConnectGroup.Host.Label"));
        props.setLook(wlHost);

        final FormData fdlHost = new FormData();
        fdlHost.top = new FormAttachment(0, margin);
        fdlHost.left = new FormAttachment(0, 0);
        fdlHost.right = new FormAttachment(middle, -margin);
        wlHost.setLayoutData(fdlHost);
        wHost = new TextVar(transMeta, gConnect, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        wHost.addModifyListener(lsMod);
        wHost.setToolTipText(getMessage("Dialog.ConnectGroup.Host.Tooltip"));
        props.setLook(wHost);

        final FormData fdHost = new FormData();
        fdHost.top = new FormAttachment(0, margin);
        fdHost.left = new FormAttachment(middle, 0);
        fdHost.right = new FormAttachment(100, 0);
        wHost.setLayoutData(fdHost);

        //  Port
        wlPort = new Label(gConnect, SWT.RIGHT);
        wlPort.setText(getMessage("Dialog.ConnectGroup.Port.Label"));
        props.setLook(wlPort);

        final FormData fdlPort = new FormData();
        fdlPort.top = new FormAttachment(wHost, margin);
        fdlPort.left = new FormAttachment(0, 0);
        fdlPort.right = new FormAttachment(middle, -margin);
        wlPort.setLayoutData(fdlPort);
        wPort = new TextVar(transMeta, gConnect, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        wPort.addModifyListener(lsMod);
        wPort.setToolTipText(getMessage("Dialog.ConnectGroup.Port.Tooltip"));
        props.setLook(wPort);

        final FormData fdPort = new FormData();
        fdPort.top = new FormAttachment(wHost, margin);
        fdPort.left = new FormAttachment(middle, 0);
        fdPort.right = new FormAttachment(100, 0);
        wPort.setLayoutData(fdPort);

        // username
        wlUsername = new Label(gConnect, SWT.RIGHT);
        wlUsername.setText(getMessage("Dialog.ConnectGroup.Username.Label"));
        props.setLook(wlUsername);

        final FormData fdlUsername = new FormData();
        fdlUsername.top = new FormAttachment(wPort, margin);
        fdlUsername.left = new FormAttachment(0, 0);
        fdlUsername.right = new FormAttachment(middle, -margin);
        wlUsername.setLayoutData(fdlUsername);
        wUsername = new TextVar(transMeta, gConnect, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        wUsername.addModifyListener(lsMod);
        wUsername.setToolTipText(getMessage("Dialog.ConnectGroup.Username.Tooltip"));
        props.setLook(wUsername);

        final FormData fdUsername = new FormData();
        fdUsername.top = new FormAttachment(wPort, margin);
        fdUsername.left = new FormAttachment(middle, 0);
        fdUsername.right = new FormAttachment(100, 0);
        wUsername.setLayoutData(fdUsername);

        // password
        wlPassword = new Label(gConnect, SWT.RIGHT);
        wlPassword.setText(getMessage("Dialog.ConnectGroup.Password.Label"));
        props.setLook(wlPassword);

        final FormData fdlPassword = new FormData();
        fdlPassword.top = new FormAttachment(wUsername, margin);
        fdlPassword.left = new FormAttachment(0, 0);
        fdlPassword.right = new FormAttachment(middle, -margin);
        wlPassword.setLayoutData(fdlPassword);
        wPassword = new TextVar(transMeta, gConnect, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.PASSWORD);
        wPassword.addModifyListener(lsMod);
        wPassword.setToolTipText(getMessage("Dialog.ConnectGroup.Password.Tooltip"));
        props.setLook(wPassword);

        final FormData fdPassword = new FormData();
        fdPassword.top = new FormAttachment(wUsername, margin);
        fdPassword.left = new FormAttachment(middle, 0);
        fdPassword.right = new FormAttachment(100, 0);
        wPassword.setLayoutData(fdPassword);

        // search query
        wlSearchQuery = new Label(gConnect, SWT.RIGHT);
        wlSearchQuery.setText(getMessage("Dialog.ConnectGroup.SearchQuery.Label"));
        props.setLook(wlSearchQuery);

        final FormData fdlSearchQuery = new FormData();
        fdlSearchQuery.top = new FormAttachment(wPassword, margin);
        fdlSearchQuery.left = new FormAttachment(0, 0);
        fdlSearchQuery.right = new FormAttachment(middle, -margin);
        wlSearchQuery.setLayoutData(fdlSearchQuery);
        wSearchQuery = new TextVar(transMeta, gConnect, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        wSearchQuery.addModifyListener(lsMod);
        wSearchQuery.setToolTipText(getMessage("Dialog.ConnectGroup.SearchQuery.Tooltip"));
        props.setLook(wSearchQuery);

        final FormData fdSearchQuery = new FormData();
        fdSearchQuery.top = new FormAttachment(wPassword, margin);
        fdSearchQuery.left = new FormAttachment(middle, 0);
        fdSearchQuery.right = new FormAttachment(100, 0);
        wSearchQuery.setLayoutData(fdSearchQuery);

        // connect group
        final FormData fdConnect = new FormData();
        fdConnect.left = new FormAttachment(0, 0);
        fdConnect.right = new FormAttachment(100, 0);
        fdConnect.top = new FormAttachment(wStepname, margin);
        gConnect.setLayoutData(fdConnect);
    }

    private void layoutKeyTable(final ModifyListener lsMod, final int margin) {
        wlFields = new Label(shell, SWT.NONE);
        wlFields.setText(getMessage("Dialog.Fields.Label"));
        props.setLook(wlFields);

        final FormData fdlReturn = new FormData();
        fdlReturn.left = new FormAttachment(0, 0);
        fdlReturn.top = new FormAttachment(gConnect, margin);
        wlFields.setLayoutData(fdlReturn);

        final int keyWidgetRows;
        if (input.getInputFields() != null) {
            keyWidgetRows = input.getInputFields().length;
        } else {
            keyWidgetRows = 1;
        }

        final ColumnInfo[] ciKeys = new ColumnInfo[11];
        ciKeys[0] = new ColumnInfo(getMessage("Dialog.Fields.ColumnInfo.Name"), ColumnInfo.COLUMN_TYPE_CCOMBO, new String[]{}, false);
        ciKeys[1] = new ColumnInfo(getMessage("Dialog.Fields.ColumnInfo.OutputName"), ColumnInfo.COLUMN_TYPE_TEXT, false);
        ciKeys[2] = new ColumnInfo(getMessage("Dialog.Fields.ColumnInfo.Default"), ColumnInfo.COLUMN_TYPE_TEXT, false);
        ciKeys[3] = new ColumnInfo(getMessage("Dialog.Fields.ColumnInfo.Type"), ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaBase.getTypes());
        ciKeys[4] = new ColumnInfo(getMessage("Dialog.Fields.ColumnInfo.Format"), ColumnInfo.COLUMN_TYPE_FORMAT, 4);
        ciKeys[5] = new ColumnInfo(getMessage("Dialog.Fields.ColumnInfo.Length"), ColumnInfo.COLUMN_TYPE_TEXT, false);
        ciKeys[6] = new ColumnInfo(getMessage("Dialog.Fields.ColumnInfo.Precision"), ColumnInfo.COLUMN_TYPE_TEXT, false);
        ciKeys[7] = new ColumnInfo(getMessage("Dialog.Fields.ColumnInfo.Currency"), ColumnInfo.COLUMN_TYPE_TEXT, false);
        ciKeys[8] = new ColumnInfo(getMessage("Dialog.Fields.ColumnInfo.Decimal"), ColumnInfo.COLUMN_TYPE_TEXT, false);
        ciKeys[9] = new ColumnInfo(getMessage("Dialog.Fields.ColumnInfo.Group"), ColumnInfo.COLUMN_TYPE_TEXT, false);
        ciKeys[10] = new ColumnInfo(getMessage("Dialog.Fields.ColumnInfo.RegExp"), ColumnInfo.COLUMN_TYPE_TEXT, false);

        fieldColumn = ciKeys[0];

        wFields = new TableView(transMeta, shell,
                SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
                ciKeys,
                keyWidgetRows,
                lsMod,
                props
        );

        final FormData fdReturn = new FormData();
        fdReturn.left = new FormAttachment(0, 0);
        fdReturn.top = new FormAttachment(wlFields, margin);
        fdReturn.right = new FormAttachment(100, 0);
        fdReturn.bottom = new FormAttachment(100, -50);
        wFields.setLayoutData(fdReturn);
    }

    private void layoutButtons(final int margin) {
        wOK = new Button(shell, SWT.PUSH);
        wOK.setText(getMessage("System.Button.OK"));
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText(getMessage("System.Button.Cancel"));
        wGet = new Button(shell, SWT.PUSH);
        wGet.setText(getMessage("System.Button.GetFields"));

        BaseStepDialog.positionBottomButtons(shell, new Button[]{wOK, wCancel, wGet}, margin, wFields);

        // Add listeners
        lsCancel = new Listener() {
            @Override
            public void handleEvent(Event e) {
                cancel();
            }
        };
        lsOK = new Listener() {
            @Override
            public void handleEvent(Event e) {
                ok();
            }
        };
        lsGet = new Listener() {
            @Override
            public void handleEvent(Event event) {
                loadFieldsForQuery();
            }
        };

        wCancel.addListener(SWT.Selection, lsCancel);
        wOK.addListener(SWT.Selection, lsOK);
        wGet.addListener(SWT.Selection, lsGet);
    }

    private void addDefaultListeners() {
        lsDef = new SelectionAdapter() {
            public void widgetDefaultSelected(SelectionEvent e) {
                ok();
            }
        };

        wStepname.addSelectionListener(lsDef);
        wHost.addSelectionListener(lsDef);
        wPort.addSelectionListener(lsDef);
        wUsername.addSelectionListener(lsDef);
        wPassword.addSelectionListener(lsDef);
        wSearchQuery.addSelectionListener(lsDef);

        // Detect X or ALT-F4 or something that kills this window...
        shell.addShellListener(new ShellAdapter() {
            public void shellClosed(ShellEvent e) {
                cancel();
            }
        });

        // Set the shell size, based upon previous time...
        setSize();
    }

    private String openDialog() {
        getData();

        input.setChanged(backupChanged);

        shell.open();
        final Display display = getParent().getDisplay();
        while (!shell.isDisposed()) {
            if (!display.readAndDispatch())
                display.sleep();
        }
        return stepname;
    }

    // Collect data from the meta and place it in the dialog
    private void getData() {
        wStepname.selectAll();

        if (input.getSplunkHost() != null) {
            wHost.setText(input.getSplunkHost());
        }
        wPort.setText(String.valueOf(input.getSplunkPort()));

        if (input.getSplunkUsername() != null) {
            wUsername.setText(input.getSplunkUsername());
        }

        if (input.getSplunkPassword() != null) {
            wPassword.setText(input.getSplunkPassword());
        }

        if (input.getSplunkSearchQuery() != null) {
            wSearchQuery.setText(input.getSplunkSearchQuery());
        }

        if (input.getInputFields() != null) {
            getTableData();
        }

        wFields.setRowNums();
        wFields.optWidth(true);
    }

    private void getTableData() {
        for (int i = 0; i < input.getInputFields().length; i++) {
            final InputField nextField = input.getInputFields()[i];
            final TableItem item = wFields.table.getItem(i);

            if (nextField.getName() != null) {
                item.setText(1, nextField.getName());
            }

            if (nextField.getOutputName() != null) {
                item.setText(2, nextField.getOutputName());
            }

            if (nextField.getDefaultValue() != null) {
                item.setText(3, nextField.getDefaultValue());
            }

            item.setText(4, ValueMetaBase.getTypeDesc(nextField.getType()));

            if (nextField.getFormat() != null) {
                item.setText(5, nextField.getFormat());
            }
            item.setText(6, nextField.getLength() < 0 ? "" : "" + nextField.getLength());
            item.setText(7, nextField.getPrecision() < 0 ? "" : "" + nextField.getPrecision());

            if (nextField.getCurrency() != null) {
                item.setText(8, nextField.getCurrency());
            }

            if (nextField.getDecimal() != null) {
                item.setText(9, nextField.getDecimal());
            }

            if (nextField.getGroup() != null) {
                item.setText(10, nextField.getGroup());
            }

            if (nextField.getRegExp() != null) {
                item.setText(11, nextField.getRegExp());
            }
        }
    }

    private void cancel() {
        stepname = null;
        input.setChanged(backupChanged);
        dispose();
    }

    // let the meta know about the entered data
    private void ok() {
        stepname = wStepname.getText();

        input.setSplunkHost(wHost.getText());
        input.setSplunkPort(Integer.parseInt(wPort.getText()));
        input.setSplunkUsername(wUsername.getText());
        input.setSplunkPassword(wPassword.getText());
        input.setSplunkSearchQuery(wSearchQuery.getText());

        int nrKeys = wFields.nrNonEmpty();

        input.allocate(nrKeys);

        for (int i = 0; i < nrKeys; i++) {
            final InputField inputField = new InputField();
            final TableItem item = wFields.getNonEmpty(i);
            inputField.setName(item.getText(1));
            inputField.setOutputName(item.getText(2));
            inputField.setDefaultValue(item.getText(3));

            final int type = ValueMetaBase.getType(item.getText(4));
            if (type < 0) {
                // fix unknowns
                inputField.setType(ValueMetaInterface.TYPE_STRING);
            } else {
                inputField.setType(type);
            }

            inputField.setFormat(item.getText(5));
            inputField.setLength(Const.toInt(item.getText(6), -1));
            inputField.setPrecision(Const.toInt(item.getText(7), -1));
            inputField.setCurrency(item.getText(8));
            inputField.setDecimal(item.getText(9));
            inputField.setGroup(item.getText(10));
            inputField.setRegExp(item.getText(11));

            input.getInputFields()[i] = inputField;
        }

        dispose();
    }

    private void loadFieldsForQuery() {
        final SplunkAdapter adapter = new SplunkAdapter();
        adapter.init(input.getSplunkHost(), input.getSplunkPort(), input.getSplunkUsername(), input.getSplunkPassword());

        final Set<String> fieldNames = adapter.getSplunkFields(wSearchQuery.getText());
        final String[] fieldNamesArray = fieldNames.toArray(new String[fieldNames.size()]);
        fieldColumn.setComboValues(fieldNamesArray);
    }

    private String getMessage(final String key, final Object... param) {
        return BaseMessages.getString(InputStepDialog.class, key, param);
    }
}
