package macrobase.conf;

/**
 * Created by pbailis on 2/16/16.
 */
public class MissingParameterException extends ConfigurationException {
    public MissingParameterException(String parameter) {
        super(String.format("Missing requested parameter %s", parameter));
    }
}
