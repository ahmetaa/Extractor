package kdtm.extractor;


import java.util.ArrayList;

/**
 * Created by sila on 16.06.2016.
 */
public class ExtractRules {
    public static void main(String[] args) {
        Rule rules = new kdtm.extractor.Rule();
        int c =0;
        ArrayList ruleList =  rules.getRules("/home/sila/projects/crawler/domains/rules");


        String href = "http%3A%2F%2Fwww.nurturia.com.tr%2Fbazaar%2Fe7ae55bd-88d9-48d5-8415-9cf400ca12d9%2F3%2Fikinci-el   ";
        for (int i = 0; i < ruleList.size(); i++) {
            rules = (kdtm.extractor.Rule) ruleList.get(i);
            if(href.contains(rules.source) || href.contains(rules.source.replace(":", "%3A").replace("/", "%2F"))) {
                for(String ig : rules.ignores) {
                    if(href.contains(ig))
                        c++;

                }
            }
        }

    }
}
