package edu.arizona.cs.mrpkm.commandline;

/**
 *
 * @author iychoi
 */
public class SinglePathArgumentParser extends EmptyKeyArgumentParserBase {

    private String value;
    
    public SinglePathArgumentParser() {
        this.value = null;
    }
    
    @Override
    public Class getValueType() {
        return String.class;
    }

    @Override
    public void parse(String[] args) throws ArgumentParseException {
        if(args.length != 1) {
            throw new ArgumentParseException("given args length is not 1");
        }
        
        this.value = args[0];
    }

    @Override
    public int getValueLength() {
        return 1;
    }

    @Override
    public String getValue() {
        return this.value;
    }

    @Override
    public String getHelpMessage() {
        return "<Path>";
    }

    @Override
    public boolean isMandatory() {
        return true;
    }
}
