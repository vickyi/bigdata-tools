package com.aiplus.bi.alarm.mail;

public class HtmlTableBuilder {

    private static final String TAG_START_BEGIN = "<";

    private static final String TAG_STOP_BEGIN = "</";

    private static final String TAG_END = ">";

    private static final String BLANK_SPACE = " ";

    private StringBuilder table = new StringBuilder();

    public HtmlTableBuilder(int border, int cellSpacing, int cellPadding) {
        table.append(TAG_START_BEGIN)
                .append("table").append(BLANK_SPACE)
                .append("border=\"").append(border).append("\"").append(BLANK_SPACE)
                .append("cellspacing=\"").append(cellSpacing).append("\"").append(BLANK_SPACE)
                .append("cellpadding=\"").append(cellPadding).append("\"")
                .append(TAG_END);
    }

    public HtmlTableBuilder appendHeaders(String[] headers) {
        return appendRow(headers, true);
    }

    public HtmlTableBuilder appendRow(String[] cells) {
        return appendRow(cells, false);
    }

    public String build() {
        table.append(TAG_STOP_BEGIN).append("table").append(TAG_END);
        return table.toString();
    }

    public HtmlTableBuilder appendRow(String[] cells, boolean isHeader) {
        if (null == cells || 0 == cells.length) {
            return this;
        }
        String cellTag = isHeader ? "th" : "td";
        table.append(TAG_START_BEGIN).append("tr").append(TAG_END);
        for (String header : cells) {
            table.append(TAG_START_BEGIN).append(cellTag).append(TAG_END)
                    .append(header)
                    .append(TAG_STOP_BEGIN).append(cellTag).append(TAG_END);
        }
        table.append(TAG_STOP_BEGIN).append("tr").append(TAG_END);
        return this;
    }
}
