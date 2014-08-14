package edu.arizona.cs.mrpkm.commandline;

/**
 *
 * @author iychoi
 */
public class HelpArgumentParser extends AArgumentParser {

    private final static String KEY_STRING = "h";
    
    private boolean value;
    
    public HelpArgumentParser() {
        this.value = false;
    }
    
    @Override
    public String getKey() {
        return KEY_STRING;
    }

    @Override
    public Class getValueType() {
        return Boolean.class;
    }

    @Override
    public void parse(String[] args) throws ArgumentParseException {
        if(args.length != 0) {
            throw new ArgumentParseException("given args length is not 0");
        }
        
        this.value = true;
    }

    @Override
    public boolean hasDefault() {
        return false;
    }
    
    @Override
    public boolean isMandatory() {
        return false;
    }

    @Override
    public int getValueLength() {
        return 0;
    }

    @Override
    public Boolean getValue() {
        return this.value;
    }

    @Override
    public String getHelpMessage() {
        return CommandLineArgumentParser.OPTION_PREFIX + KEY_STRING + " : show help";
    }
}
