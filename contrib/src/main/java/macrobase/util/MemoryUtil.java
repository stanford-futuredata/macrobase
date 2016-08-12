package macrobase.util;

import java.text.NumberFormat;

public class MemoryUtil {

    public static String checkMemoryUsage() {
        Runtime runtime = Runtime.getRuntime();

        NumberFormat format = NumberFormat.getInstance();

        StringBuilder sb = new StringBuilder();
        long maxMemory = runtime.maxMemory();
        long allocatedMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();

        sb.append("free memory: " + format.format(freeMemory / 1024) + "  ");
        sb.append("allocated memory: " + format.format(allocatedMemory / 1024) + "  ");
        sb.append("max memory: " + format.format(maxMemory / 1024) + "  ");
        sb.append("total free memory: " + format.format((freeMemory + (maxMemory - allocatedMemory)) / 1024) + "  ");
        return sb.toString();
    }

}
