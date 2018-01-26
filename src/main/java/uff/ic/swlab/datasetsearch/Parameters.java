package uff.ic.swlab.datasetsearch;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.validator.routines.UrlValidator;
import org.apache.jena.riot.Lang;

public final class Parameters {

    public final Lang lang;
    public final String query;
    public final URL voidURL;
    public final String keywords;
    public final Integer offset;
    public final Integer limit;
    public final Methods method;

    public enum Methods {
        SELECT("select"), SCAN("scan");
        public final String label;

        Methods(String label) {
            this.label = label;
        }
    }

    private Parameters() {
        lang = null;
        query = null;
        voidURL = null;
        keywords = null;
        offset = 0;
        limit = 0;
        method = null;
    }

    public Parameters(HttpServletRequest request) throws UnsupportedEncodingException, MalformedURLException, Exception {
        lang = detectRequestedLang(request.getHeader("Accept"));

        query = URLDecoder.decode(request.getParameter("q"), "UTF-8");
        if ((new UrlValidator()).isValid(query)) {
            keywords = null;
            voidURL = new URL(query);
        } else {
            keywords = query;
            voidURL = null;
        }

        String offsetString = request.getParameter("offset");
        offset = offsetString != null ? Integer.parseInt(offsetString) : null;

        String limitString = request.getParameter("limit");
        limit = limitString != null ? Integer.parseInt(limitString) : null;
        String methodString = null;
        try{
            methodString = URLDecoder.decode(request.getParameter("method"), "UTF-8");
        }catch(Throwable e) {
            System.out.println(e);
        }
        if (isKeywordSearch()){
            method = null;
        }else if (isVoidSearch()){
            if (methodString != null)
                if (methodString.equals(Methods.SELECT.label))
                    method = Methods.SELECT;
                else if (methodString.equals(Methods.SCAN.label))
                    method = Methods.SCAN;
                else {
                    method = null;
                    throw new Exception("Unknown search method.");
                }
            else if (isHumanRequest())
                method = Methods.SELECT; // Infer SELECT method
            else if (isAppRequest())
                method = Methods.SCAN; // Infer SCAN method
            else {
                method = null;
                throw new Exception("Unknown request agent.");
            }
        }else
            throw new Exception("Unknown search type.");
    }

    private Lang detectRequestedLang(String accept) {
        Lang[] langs = {Lang.RDFXML, Lang.TURTLE, Lang.TTL, Lang.NTRIPLES, Lang.N3, Lang.NT,
            Lang.TRIG, Lang.TRIX, Lang.JSONLD, Lang.RDFJSON, Lang.RDFTHRIFT, Lang.NQUADS, Lang.NQ};
        for (Lang lang : langs)
            if (accept.toLowerCase().contains(lang.getHeaderString().toLowerCase()))
                return lang;
        return null;
    }

    public boolean isKeywordSearch() {
        return keywords != null;
    }

    public boolean isVoidSearch() {
        return voidURL != null;
    }

    public boolean isHumanRequest() {
        return lang == null;
    }

    public boolean isAppRequest() {
        return lang != null;
    }
}
