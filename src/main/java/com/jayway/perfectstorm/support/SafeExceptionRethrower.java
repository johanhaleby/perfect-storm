package com.jayway.perfectstorm.support;

public class SafeExceptionRethrower {

    public static <T> T safeRethrow(Throwable t) {
        SafeExceptionRethrower.<RuntimeException>safeRethrow0(t);
        return null;
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void safeRethrow0(Throwable t) throws T {
        throw (T) t;
    }

}