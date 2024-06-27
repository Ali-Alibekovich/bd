package org.example.com.service;

import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Enumeration;
import java.util.Random;

public class Generator {
    private static final int MACHINE_IDENTIFIER;
    private static final short PROCESS_IDENTIFIER;
    private static final Random RANDOM = new SecureRandom();
    private static int counter = RANDOM.nextInt() & 0xFFFFFF;

    static {
        try {
            MACHINE_IDENTIFIER = createMachineIdentifier();
            PROCESS_IDENTIFIER = createProcessIdentifier();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String generate() {
        ByteBuffer buffer = ByteBuffer.allocate(12);

        // Время
        int timestamp = (int) Instant.now().getEpochSecond();
        buffer.putInt(timestamp);

        // Идентификатор машины
        buffer.put((byte) (MACHINE_IDENTIFIER >> 16));
        buffer.put((byte) (MACHINE_IDENTIFIER >> 8));
        buffer.put((byte) MACHINE_IDENTIFIER);

        // Идентификатор процесса
        buffer.putShort(PROCESS_IDENTIFIER);

        // Счетчик
        buffer.put((byte) (counter >> 16));
        buffer.put((byte) (counter >> 8));
        buffer.put((byte) counter);
        counter = (counter + 1) & 0xFFFFFF;

        return toHexString(buffer.array());
    }

    private static int createMachineIdentifier() throws Exception {
        int machinePiece;
        try {
            StringBuilder sb = new StringBuilder();
            Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
            while (e.hasMoreElements()) {
                NetworkInterface ni = e.nextElement();
                sb.append(ni.toString());
            }
            machinePiece = sb.toString().hashCode();
        } catch (Throwable t) {
            machinePiece = RANDOM.nextInt();
        }
        return machinePiece & 0xFFFFFF;
    }

    private static short createProcessIdentifier() {
        short processId;
        try {
            String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
            if (processName.contains("@")) {
                processId = (short) Integer.parseInt(processName.substring(0, processName.indexOf('@')));
            } else {
                processId = (short) processName.hashCode();
            }
        } catch (Throwable t) {
            processId = (short) RANDOM.nextInt();
        }
        return processId;
    }

    private static String toHexString(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();
        for (byte b : bytes) {
            String hex = Integer.toHexString(b & 0xFF);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }
}