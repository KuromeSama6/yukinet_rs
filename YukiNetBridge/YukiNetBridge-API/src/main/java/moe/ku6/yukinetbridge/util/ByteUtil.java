package moe.ku6.yukinetbridge.util;

import lombok.experimental.UtilityClass;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@UtilityClass
// all data are big-endian
public class ByteUtil {
    public static byte ReadByte(InputStream stream) {
        try {
            return (byte) stream.read();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void ReadBytes(InputStream stream, byte[] buffer, int offset, int length) {
        try {
            int readBytes = stream.read(buffer, offset, length);
            if (readBytes != length) {
                throw new RuntimeException("Failed to read expected number of bytes");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static short ReadWord(InputStream stream) {
        int high = ReadByte(stream) & 0xFF;
        int low = ReadByte(stream) & 0xFF;
        return (short) ((high << 8) | (low & 0xFF));
    }

    public static int ReadDWord(InputStream stream) {
        int b1 = ReadByte(stream) & 0xFF;
        int b2 = ReadByte(stream) & 0xFF;
        int b3 = ReadByte(stream) & 0xFF;
        int b4 = ReadByte(stream) & 0xFF;
        return (b1 << 24) | (b2 << 16) | (b3 << 8) | b4;
    }

    public static long ReadQWord(InputStream stream) {
        long b1 = ReadByte(stream) & 0xFFL;
        long b2 = ReadByte(stream) & 0xFFL;
        long b3 = ReadByte(stream) & 0xFFL;
        long b4 = ReadByte(stream) & 0xFFL;
        long b5 = ReadByte(stream) & 0xFFL;
        long b6 = ReadByte(stream) & 0xFFL;
        long b7 = ReadByte(stream) & 0xFFL;
        long b8 = ReadByte(stream) & 0xFFL;
        return (b1 << 56) | (b2 << 48) | (b3 << 40) | (b4 << 32) |
               (b5 << 24) | (b6 << 16) | (b7 << 8) | b8;
    }

    public static boolean ReadBool(InputStream stream) {
        byte value = ReadByte(stream);
        return value == 1;
    }

    public static float ReadFloat32(InputStream stream) {
        int intBits = ReadDWord(stream);
        return Float.intBitsToFloat(intBits);
    }

    public static double ReadFloat64(InputStream stream) {
        long longBits = ReadQWord(stream);
        return Double.longBitsToDouble(longBits);
    }

    public static String ReadString(InputStream stream) {
        int size = ReadDWord(stream);
        if (size < 0) {
            throw new RuntimeException("Invalid string size: " + size);
        }

        if (size == 0) return "";

        byte[] strBytes = new byte[size];
        try {
            int readBytes = stream.read(strBytes);
            if (readBytes != size) {
                throw new RuntimeException("Failed to read expected number of bytes for string");
            }
            return new String(strBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Reads a variable-length integer from the input stream. The most significant bit (MSB) of each byte indicates if there are more bytes to read.
     * The lower 7 bits of each byte contribute to the final integer value.
     * @param stream The input stream to read from.
     * @return The decoded variable-length integer.
     */
    public static long ReadVarInt(InputStream stream) {
        long value = 0;
        int position = 0;
        byte currentByte;

        do {
            currentByte = ReadByte(stream);
            value |= (long)(currentByte & 0x7F) << position;
            position += 7;

            if (position > 63) {
                throw new RuntimeException("Variable length quantity is too long");
            }
        } while ((currentByte & 0x80) != 0);

        return value;
    }

    public static void WriteByte(OutputStream buf, byte value) {
        try {
            buf.write(value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void WriteBytes(OutputStream buf, byte[] value) {
        try {
            buf.write(value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void WriteWord(OutputStream buf, short value) {
        WriteByte(buf, (byte) ((value >> 8) & 0xFF));
        WriteByte(buf, (byte) (value & 0xFF));
    }

    public static void WriteDWord(OutputStream buf, int value) {
        WriteByte(buf, (byte) ((value >> 24) & 0xFF));
        WriteByte(buf, (byte) ((value >> 16) & 0xFF));
        WriteByte(buf, (byte) ((value >> 8) & 0xFF));
        WriteByte(buf, (byte) (value & 0xFF));
    }

    public static void WriteQWord(OutputStream buf, long value) {
        WriteByte(buf, (byte) ((value >> 56) & 0xFF));
        WriteByte(buf, (byte) ((value >> 48) & 0xFF));
        WriteByte(buf, (byte) ((value >> 40) & 0xFF));
        WriteByte(buf, (byte) ((value >> 32) & 0xFF));
        WriteByte(buf, (byte) ((value >> 24) & 0xFF));
        WriteByte(buf, (byte) ((value >> 16) & 0xFF));
        WriteByte(buf, (byte) ((value >> 8) & 0xFF));
        WriteByte(buf, (byte) (value & 0xFF));
    }

    public static void WriteFloat32(OutputStream buf, float value) {
        int intBits = Float.floatToIntBits(value);
        WriteDWord(buf, intBits);
    }

    public static void WriteFloat64(OutputStream buf, double value) {
        long longBits = Double.doubleToLongBits(value);
        WriteQWord(buf, longBits);
    }

    public static void WriteString(OutputStream buf, String value) {
        if (value == null) {
            WriteDWord(buf, 0);
            return;
        }

        byte[] strBytes = value.getBytes();
        WriteDWord(buf, strBytes.length);
        try {
            buf.write(strBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void WriteVarInt(OutputStream stream, long value) {
        do {
            byte temp = (byte) (value & 0x7F);
            value >>>= 7;
            if (value != 0) {
                temp |= (byte)0x80;
            }
            WriteByte(stream, temp);
        } while (value != 0);
    }

    public static void Write(OutputStream stream, boolean value) {
        WriteByte(stream, value ? (byte)1 : (byte)0);
    }

    /**
     * Packs a byte array into a List of 64-bit Longs in big-endian order.
     * If the input length is not a multiple of 8, the last Long is padded with zeros.
     *
     * @return A List of Longs containing the packed bytes.
     */
    public static List<Long> PackBytes(byte[] bytes) {
        List<Long> longs = new ArrayList<>();
        if (bytes == null) {
            return longs;
        }

        // pad to a multiple of 8 bytes
        if (bytes.length % 8 != 0) {
            byte[] paddedBytes = new byte[(bytes.length + 7) / 8 * 8];
            System.arraycopy(bytes, 0, paddedBytes, 0, bytes.length);
            bytes = paddedBytes;
        }

        // Calculate the number of longs needed (ceiling division)
        int numLongs = (bytes.length + 7) / 8;

        // Process each 8-byte chunk
        for (int i = 0; i < numLongs; i++) {
            long value = 0;
            // Pack up to 8 bytes into the current long, in big-endian order
            for (int j = 0; j < 8 && (i * 8 + j) < bytes.length; j++) {
                // Shift left and add the byte (unsigned, using & 0xFF)
                value = (value << 8) | (bytes[i * 8 + j] & 0xFF);
            }
            longs.add(value);
        }

        return longs;
    }

    public static byte[] UnpackBytes(List<Long> longs) {
        List<Byte> byteList = new ArrayList<>();
        for (Long value : longs) {
            for (int i = 7; i >= 0; i--) {
                byte b = (byte) ((value >> (i * 8)) & 0xFF);
                byteList.add(b);
            }
        }
        // Convert List<Byte> to byte[]
        byte[] bytes = new byte[byteList.size()];
        for (int i = 0; i < byteList.size(); i++) {
            bytes[i] = byteList.get(i);
        }
        return bytes;
    }

    public static String FormatBytes(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X ", b));
        }
        return sb.toString().trim();
    }
}
