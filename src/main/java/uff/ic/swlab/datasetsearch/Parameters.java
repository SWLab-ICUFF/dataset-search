package uff.ic.swlab.datasetsearch;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.validator.routines.UrlValidator;
import org.apache.jena.riot.Lang;

public class Parameters {

    public final Lang lang;
    public final String query;
    public final URL voidURL;
    public final String keywords;
    public final Integer offset;
    public final Integer limit;
    public final String method;

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
        String path = request.getPathInfo();
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

        String methodString = URLDecoder.decode(request.getParameter("method"), "UTF-8");
        if (methodString == null)
            if (lang == null)
                method = "select";
            else
                method = "scan";
        else if (methodString.equals("select") || methodString.equals("scan"))
            method = methodString;
        else
            method = null;
        if (method == null)
            new Exception("Invalid search method.");

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

    public boolean isSearchForSelect() {
        return method.equals("select") || keywords != null || lang == null;
    }
}
