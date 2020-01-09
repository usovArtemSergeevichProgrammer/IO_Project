package util;
/**
 * <!DOCTYPE html> <html> <body>
 * 
 * <h2>Basic HTML Table</h2>
 * 
 * <table style="width:100%">
 * <tr>
 * <th>Firstname</th>
 * <th>Lastname</th>
 * <th>Age</th>
 * </tr>
 * <tr>
 * <td>Jill</td>
 * <td>Smith</td>
 * <td>50</td>
 * </tr>
 * <tr>
 * <td>Eve</td>
 * <td>Jackson</td>
 * <td>94</td>
 * </tr>
 * <tr>
 * <td>John</td>
 * <td>Doe</td>
 * <td>80</td>
 * </tr>
 * </table>
 * 
 * </body> </html>
 * 
 */
public class HTMLTableGenerator {
	private static StringBuffer table = new StringBuffer();
    private static final String BEGIN = "<html>";
    private static final String END = "</html>";
    private static final String TABLE_BEGIN = "<table border = \"1\">";
    private static final String TABLE_END = "</table>";
    private static final String HEADER_BEGIN = "<th>";
    private static final String HEADER_END = "</th>";
    private static final String ROW_BEGIN = "<tr>";
    private static final String ROW_END = "</tr>";
    private static final String COLUMT_BEGIN = "<td>";
    private static final String COLUMT_END = "</td>";
    private static int colums;

    public HTMLTableGenerator(int rows, int colums) {
        HTMLTableGenerator.colums = colums;
        table.append(BEGIN);
        table.append(TABLE_BEGIN);
        //
        table.append(TABLE_END);
        table.append(END);
        System.out.println("step I - pre-results : " + table.toString());
    }

    public void addHeaders(String... values) {
        if (values.length != colums) {
            System.err.println("Error colums length");
        } else {
            int lastIndex = table.lastIndexOf(TABLE_END);
            if (lastIndex > 0) {
                StringBuffer headers = new StringBuffer();
                headers.append(ROW_BEGIN);
                for (String value : values) {
                    headers.append(HEADER_BEGIN);
                    headers.append(value);
                    headers.append(HEADER_END);
                }
                headers.append(ROW_END);
                table.insert(lastIndex, headers.toString());
                System.out.println("step II - pre-results  addHeaders: " + table.toString());
            }
        }
    }

    public void addRowsData(String... values) {
        if (values.length != colums) {
            System.err.println("Error colums length");
        } else {
            int lastIndex = table.lastIndexOf(ROW_END);
            if (lastIndex > 0) {
                int index = lastIndex + ROW_END.length();
                StringBuffer rows = new StringBuffer();
                rows.append(ROW_BEGIN);
                for (String value : values) {
                    rows.append(COLUMT_BEGIN);
                    rows.append(value);
                    rows.append(COLUMT_END);
                }
                rows.append(ROW_END);
                table.insert(index, rows.toString());
               // System.out.println("step III - pre-results  addRowsData: " + table.toString());
            }
        }
    }
    
    public String build(){
        return table.toString();
    }
    
    public void buildAndClean(){
        build();
        table.setLength(0);
    }

    public static void main(String[] args) {
        HTMLTableGenerator table = new HTMLTableGenerator(3, 3);
        table.addHeaders("NAME", "LAST_NAME", "AGE");
        table.addRowsData("ALex", "Ivanov", "30");
        table.addRowsData("John", "Johnson", "40");

        System.out.println(table.build());
    }
}
