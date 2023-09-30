package br.com.spedison.log;

import java.text.DateFormat;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class LogFormat  extends Formatter {
    private static Locale loc = new Locale.Builder().setLanguage("pt").setRegion("BR").build();
    //private static DateFormat sdf = DateFormat.getDateInstance(DateFormat., loc);getDateInstance(DateFormat., loc);
    private static Format sdf = DateTimeFormatter.ofPattern("dd/MM/yyyy hh:mm:ss").toFormat();
    @Override
    public String format(LogRecord record) {
        LocalDateTime ldt =
                Instant.ofEpochMilli(record.getMillis()).atZone(ZoneId.systemDefault()).toLocalDateTime();
        return "[%s] - [%s]\t%s - %s\n".formatted(
                sdf.format(ldt),
                record.getLevel().toString(),
                record.getSourceClassName(),
                record.getMessage()
        );
    }
}