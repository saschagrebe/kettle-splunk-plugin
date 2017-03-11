package de.sagr.kettle.splunkplugin.adapter;

/**
 * Created by grebe on 11.03.2017.
 */
public final class SplunkQuery {

    private final StringBuilder query = new StringBuilder();

    public static SplunkQuery search(String query) {
        final SplunkQuery builder = new SplunkQuery();
        return builder.startSearch(query);
    }

    private SplunkQuery startSearch(String searchQuery) {
        query.append("search ").append(searchQuery);
        return this;
    }

    public SplunkQuery head(int firstResults) {
        query.append(" | head ").append(firstResults);
        return this;
    }

    public SplunkQuery table() {
        return table("*");
    }

    public SplunkQuery table(String... fields) {
        query.append(" | table ");
        for (int i = 0; i < fields.length; i++) {
            query.append(fields[i]);
            if ((i + 1) < fields.length) {
                query.append(", ");
            }
        }

        return this;
    }

    public String build() {
        return query.toString();
    }
}
