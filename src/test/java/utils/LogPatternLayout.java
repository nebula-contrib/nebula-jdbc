/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package utils;

import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;


public class LogPatternLayout extends PatternLayout {

    @Override
    public String format(LoggingEvent event) {
        Level level = event.getLevel();
        String prefix = "\033[33m";
        String suffix = "\033[0m";
        switch (level.toInt()) {
            case Level.TRACE_INT:
                prefix = "\033[30m";
                break;

            case Level.DEBUG_INT:
                prefix = "\033[34m";
                break;
            case Level.INFO_INT:
                prefix = "\033[32m";
                break;
            case Level.WARN_INT:
                prefix = "\033[33m";
                break;
            case Level.ERROR_INT:
                prefix = "\033[31m";
                break;
        }
        return prefix + super.format(event) + suffix;
    }

}
