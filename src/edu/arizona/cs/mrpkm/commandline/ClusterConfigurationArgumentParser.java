package edu.arizona.cs.mrpkm.commandline;

import edu.arizona.cs.mrpkm.cluster.MRClusterConfigurationBase;
import edu.arizona.cs.mrpkm.cluster.MRClusterConfiguration_Default;

/**
 *
 * @author iychoi
 */
public class ClusterConfigurationArgumentParser extends ArgumentParserBase {

    private final static String KEY_STRING = "c";
    private MRClusterConfigurationBase value;
    
    public ClusterConfigurationArgumentParser() {
        this.value = new MRClusterConfiguration_Default();
    }
    
    @Override
    public String getKey() {
        return KEY_STRING;
    }

    @Override
    public Class getValueType() {
        return MRClusterConfigurationBase.class;
    }

    @Override
    public void parse(String[] args) throws ArgumentParseException {
        if(args.length != 1) {
            throw new ArgumentParseException("given args length is not 1");
        }
        
        try {
            String clusterConfigString = args[0];
            this.value = MRClusterConfigurationBase.findConfiguration(clusterConfigString);
        } catch (NumberFormatException ex) {
            throw new ArgumentParseException("given arg is not in correct data type");
        } catch (ClassNotFoundException ex) {
            throw new ArgumentParseException("given configuration is not registered");
        } catch (InstantiationException ex) {
            throw new ArgumentParseException("given configuration is not registered");
        } catch (IllegalAccessException ex) {
            throw new ArgumentParseException("given configuration is not registered");
        }
    }

    @Override
    public boolean hasDefault() {
        return true;
    }
    
    @Override
    public boolean isMandatory() {
        return true;
    }

    @Override
    public int getValueLength() {
        return 1;
    }

    @Override
    public MRClusterConfigurationBase getValue() {
        return this.value;
    }

    @Override
    public String getHelpMessage() {
        return CommandLineArgumentParser.OPTION_PREFIX + KEY_STRING + " : set cluster configuration";
    }
}
