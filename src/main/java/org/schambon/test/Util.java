package org.schambon.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {

    public static final Logger logger = LoggerFactory.getLogger(Util.class);

    static void exception() {
        logger.warn("About to throw random exception");
        throw new RuntimeException("Oops, something just happened!");
    }

    static void die() {
        logger.warn("About to die... it was a nice life");
        System.exit(-1);
    }
}
