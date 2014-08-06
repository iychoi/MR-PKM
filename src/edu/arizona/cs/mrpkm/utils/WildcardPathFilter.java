package edu.arizona.cs.mrpkm.utils;

import java.util.regex.Pattern;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 *
 * @author iychoi
 */
public class WildcardPathFilter implements PathFilter {

    private String nameExpression;
    private String pattern;
    
    public WildcardPathFilter(String nameExpression) {
        this.nameExpression = nameExpression;
        this.pattern = makePattern(nameExpression);
    }
    
    private String makePattern(String wildcardExpression) {
        String expression = wildcardExpression.replaceAll("*", ".*");
        String safeExpression = Pattern.quote(expression);
        return "^" + safeExpression + "$";
    }
    
    @Override
    public boolean accept(Path path) {
        if(path.getName().matches(this.pattern)) {
            return true;
        }
        return false;
    }
}
