package etl.installer.panel.action;

import com.izforge.izpack.api.data.InstallData;
import com.izforge.izpack.api.data.PanelActionConfiguration;
import com.izforge.izpack.api.exception.IzPackException;
import com.izforge.izpack.api.handler.AbstractUIHandler;
import com.izforge.izpack.data.PanelAction;
import com.izforge.izpack.panels.userinput.field.Choice;
import com.izforge.izpack.panels.userinput.field.ChoiceField;
import com.izforge.izpack.panels.userinput.field.ChoiceFieldConfig;
import com.izforge.izpack.panels.userinput.field.combo.ComboField;
import com.izforge.izpack.panels.userinput.gui.GUIField;
import com.izforge.izpack.panels.userinput.gui.combo.GUIComboField;
import etl.installer.core.InstallerHelper;

import java.lang.reflect.Field;
import java.util.List;

public class LoadServerIpAction implements PanelAction {

    public void executeAction(InstallData installData, AbstractUIHandler abstractUIHandler) {
        if (!InstallerHelper.isAutoInstall()) {
            try {
                Field f = abstractUIHandler.getClass().getDeclaredField("views");
                f.setAccessible(true);
                List<GUIField> views = (List<GUIField>) f.get(abstractUIHandler);
                views.stream()
                        .filter(view -> view instanceof GUIComboField)
                        .forEach(view -> reloadChoice(view));
            } catch (Exception e) {
                throw new IzPackException("An error occur while load server ip", e);
            }
        }

    }

    private void reloadChoice(GUIField view) {
        ComboField comboField = (ComboField) view.getField();
        List<Choice> choices = comboField.getChoices();
        choices.clear();
        try {
            Field config = ChoiceField.class.getDeclaredField("config");
            config.setAccessible(true);
            ChoiceFieldConfig choiceConfig = (ChoiceFieldConfig) config.get(comboField);
            choices.addAll(choiceConfig.getChoices());
        } catch (Exception e) {
            throw new IzPackException("An error occur while load server ip",e);
        }
    }

    public void initialize(PanelActionConfiguration panelActionConfiguration) {

    }
}
