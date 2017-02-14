package de.sagr.kettle.splunkplugin.lookup;

import java.util.Arrays;

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
import org.pentaho.di.core.row.ValueMeta;
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

public class LookupStepDialog extends BaseStepDialog implements StepDialogInterface {

    private LookupStepMeta input;

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

    // lookup fields settings widgets
    private Label wlFields;
    private TableView wFields;

    // all fields from the previous steps, used for dropdown selection
    private RowMetaInterface prevFields = null;

    // the dropdown column which should contain previous fields from stream
    private ColumnInfo fieldColumn = null;

    // constructor
    public LookupStepDialog(Shell parent, Object in, TransMeta transMeta, String sname) {
        super(parent, (BaseStepMeta) in, transMeta, sname);
        input = (LookupStepMeta) in;
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
        if (input.getFieldNames() != null) {
            keyWidgetRows = input.getFieldNames().length;
        } else {
            keyWidgetRows = 1;
        }

        final ColumnInfo[] ciKeys = new ColumnInfo[9];
        ciKeys[0] = new ColumnInfo(getMessage("Dialog.Fields.ColumnInfo.Name"), ColumnInfo.COLUMN_TYPE_CCOMBO, new String[]{}, false);
        ciKeys[1] = new ColumnInfo(getMessage("Dialog.Fields.ColumnInfo.Default"), ColumnInfo.COLUMN_TYPE_TEXT, false);
        ciKeys[2] = new ColumnInfo(getMessage("Dialog.Fields.ColumnInfo.Type"), ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaBase.getTypes());
        ciKeys[3] = new ColumnInfo(getMessage("Dialog.Fields.ColumnInfo.Format"), ColumnInfo.COLUMN_TYPE_FORMAT, 4);
        ciKeys[4] = new ColumnInfo(getMessage("Dialog.Fields.ColumnInfo.Length"), ColumnInfo.COLUMN_TYPE_TEXT, false);
        ciKeys[5] = new ColumnInfo(getMessage("Dialog.Fields.ColumnInfo.Precision"), ColumnInfo.COLUMN_TYPE_TEXT, false);
        ciKeys[6] = new ColumnInfo(getMessage("Dialog.Fields.ColumnInfo.Currency"), ColumnInfo.COLUMN_TYPE_TEXT, false);
        ciKeys[7] = new ColumnInfo(getMessage("Dialog.Fields.ColumnInfo.Decimal"), ColumnInfo.COLUMN_TYPE_TEXT, false);
        ciKeys[8] = new ColumnInfo(getMessage("Dialog.Fields.ColumnInfo.Group"), ColumnInfo.COLUMN_TYPE_TEXT, false);

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

        BaseStepDialog.positionBottomButtons(shell, new Button[]{wOK, wCancel}, margin, wFields);

        // Add listeners
        lsCancel = new Listener() {
            public void handleEvent(Event e) {
                cancel();
            }
        };
        lsOK = new Listener() {
            public void handleEvent(Event e) {
                ok();
            }
        };

        wCancel.addListener(SWT.Selection, lsCancel);
        wOK.addListener(SWT.Selection, lsOK);
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
        setComboValues();

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

        if (input.getSplunkPort() != null) {
            wPort.setText(input.getSplunkPort());
        }

        if (input.getSplunkUsername() != null) {
            wUsername.setText(input.getSplunkUsername());
        }

        if (input.getSplunkPassword() != null) {
            wPassword.setText(input.getSplunkPassword());
        }

        if (input.getSplunkSearchQuery() != null) {
            wSearchQuery.setText(input.getSplunkSearchQuery());
        }

        if (input.getFieldNames() != null) {


            for (int i = 0; i < input.getFieldNames().length; i++) {

                TableItem item = wFields.table.getItem(i);

                if (input.getFieldNames()[i] != null) {
                    item.setText(1, input.getFieldNames()[i]);
                }

                if (input.getOutputField()[i] != null) {
                    item.setText(2, input.getOutputField()[i]);
                }

                if (input.getOutputDefault()[i] != null) {
                    item.setText(3, input.getOutputDefault()[i]);
                }

                item.setText(4, ValueMeta.getTypeDesc(input.getOutputType()[i]));

                if (input.getOutputFormat()[i] != null) {
                    item.setText(5, input.getOutputFormat()[i]);
                }
                item.setText(6, input.getOutputLength()[i] < 0 ? "" : "" + input.getOutputLength()[i]);
                item.setText(7, input.getOutputPrecision()[i] < 0 ? "" : "" + input.getOutputPrecision()[i]);

                if (input.getOutputCurrency()[i] != null) {
                    item.setText(8, input.getOutputCurrency()[i]);
                }

                if (input.getOutputDecimal()[i] != null) {
                    item.setText(9, input.getOutputDecimal()[i]);
                }

                if (input.getOutputGroup()[i] != null) {
                    item.setText(10, input.getOutputGroup()[i]);
                }

            }
        }

        wFields.setRowNums();
        wFields.optWidth(true);
    }

    // asynchronous filling of the combo boxes
    private void setComboValues() {
        Runnable fieldLoader = new Runnable() {
            public void run() {
                try {
                    prevFields = transMeta.getPrevStepFields(stepname);
                } catch (KettleException e) {
                    prevFields = new RowMeta();
                    String msg = getMessage("Dialog.DoMapping.UnableToFindInput");
                    logError(msg);
                }
                String[] prevStepFieldNames = prevFields.getFieldNames();
                Arrays.sort(prevStepFieldNames);
                fieldColumn.setComboValues(prevStepFieldNames);

            }
        };
        new Thread(fieldLoader).start();
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
        input.setSplunkPort(wPort.getText());
        input.setSplunkUsername(wUsername.getText());
        input.setSplunkPassword(wPassword.getText());
        input.setSplunkSearchQuery(wSearchQuery.getText());

        int nrKeys = wFields.nrNonEmpty();

        input.allocate(nrKeys);

        for (int i = 0; i < nrKeys; i++) {
            TableItem item = wFields.getNonEmpty(i);
            input.getFieldNames()[i] = item.getText(1);
            input.getOutputField()[i] = item.getText(2);

            input.getOutputDefault()[i] = item.getText(3);
            input.getOutputType()[i] = ValueMeta.getType(item.getText(4));

            // fix unknowns
            if (input.getOutputType()[i] < 0) {
                input.getOutputType()[i] = ValueMetaInterface.TYPE_STRING;
            }

            input.getOutputFormat()[i] = item.getText(5);
            input.getOutputLength()[i] = Const.toInt(item.getText(6), -1);
            input.getOutputPrecision()[i] = Const.toInt(item.getText(7), -1);
            input.getOutputCurrency()[i] = item.getText(8);
            input.getOutputDecimal()[i] = item.getText(9);
            input.getOutputGroup()[i] = item.getText(10);

        }

        dispose();
    }

    private String getMessage(final String key, final Object... param) {
        return BaseMessages.getString(LookupStepDialog.class, key, param);
    }
}
