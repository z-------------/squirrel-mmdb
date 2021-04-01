MMDB <- {
    MetadataMarker = blob(14)
}

// "\xab\xcd\xefMaxMind.com"
MMDB.MetadataMarker[0]  = 171;
MMDB.MetadataMarker[1]  = 205;
MMDB.MetadataMarker[2]  = 239;
MMDB.MetadataMarker[3]  = 77;
MMDB.MetadataMarker[4]  = 97;
MMDB.MetadataMarker[5]  = 120;
MMDB.MetadataMarker[6]  = 77;
MMDB.MetadataMarker[7]  = 105;
MMDB.MetadataMarker[8]  = 110;
MMDB.MetadataMarker[9]  = 100;
MMDB.MetadataMarker[10] = 46;
MMDB.MetadataMarker[11] = 99;
MMDB.MetadataMarker[12] = 111;
MMDB.MetadataMarker[13] = 109;

/* public interface */

MMDB.OpenFile <- function(filename)
{
    MMDB.File <- file(filename, "r");
}

MMDB.ReadMetadata <- function()
{
    MMDB.File.seek(MMDB.getMetadataPos());
    return MMDB.decodeData();
}

/* find blob in file */

MMDB.getMetadataPos <- function()
{
    local metadataMarkerPos = MMDB.rFindBlob(MMDB.MetadataMarker);
    return metadataMarkerPos + MMDB.MetadataMarker.len();
}

MMDB.blobAreEqual <- function(a, b)
{
    local aLen = a.len();
    local bLen = b.len();

    if (aLen != bLen) return false;

    for (local i = 0; i < aLen; ++i)
    {
        if (a[i] != b[i])
        {
            return false;
        }
    }

    return true;
}

MMDB.findBlob <- function(str)
{
    local strLen = str.len();
    local i = 0;

    MMDB.File.seek(0);

    while (!MMDB.File.eos())
    {
        local data = MMDB.File.readblob(strLen);        
        if (MMDB.blobAreEqual(data, str))
        {
            return i;
        }
        MMDB.File.seek(++i);
    }

    return null;
}

MMDB.rFindBlob <- function(str)
{
    local strLen = str.len();
    local fileLen = MMDB.File.len();
    local i = strLen;

    MMDB.File.seek(-strLen, 'e');

    while (true)
    {
        local data = MMDB.File.readblob(strLen);        
        if (MMDB.blobAreEqual(data, str))
        {
            return fileLen - i;
        }

        if (i == fileLen) break;

        MMDB.File.seek(-(++i), 'e');
    }

    return null;
}

/* data decode */

MMDB.blobToString <- function(b)
{
    local result = "";
    for (local i = 0; i < b.len(); ++i)
    {
        result += b[i].tochar();
    }
    return result;
}

MMDB.advance <- function()
{
    MMDB.File.seek(1, 'c');
}

MMDB.readByte <- function()
{
    local pos = MMDB.File.tell();
    local byte = MMDB.File.readblob(1)[0];
    MMDB.File.seek(pos + 1);
    return byte;
}

MMDB.readNumber <- function(size)
{
    local pos = MMDB.File.tell();
    local result = 0;

    for (local i = size - 1; i >= 0; --i)
    {
        local n = MMDB.readByte();
        result += n * pow(256, i);
    }

    MMDB.File.seek(pos + size);
    return result;
}

MMDB.readControlByte <- function()
{
    local controlByte = MMDB.readByte();

    local dataFormat = controlByte >>> 5;
    local dataSize = controlByte & 31; // 0d31 = 0b00011111

    if (dataFormat == 0) // extended
    {
        dataFormat = 7 + MMDB.readByte();
    }

    if (dataSize >= 29)
    {
        throw "can't deal with long payload sizes";
    }

    return {
        f = dataFormat
        s = dataSize
    }
}

MMDB.decodeData <- function()
{
    print("decode data. pos = " + MMDB.File.tell() + "\n");

    local controlInfo = MMDB.readControlByte();
    local dataFormat = controlInfo.f;
    local dataSize = controlInfo.s;

    print("data format = " + dataFormat + "\n");
    print("data size = " + dataSize + "\n");

    switch (dataFormat)
    {
        // case 1:
        //     return MMDB.decodePointer(dataSize);
        //     break;
        case 2:
            return MMDB.decodeString(dataSize);
            break;
        case 3:
            return MMDB.decodeDouble(dataSize);
            break;
        // case 4:
        //     return MMDB.decodeBytes(dataSize);
        //     break;
        case 5:
            return MMDB.decodeU16(dataSize);
            break;
        case 6:
            return MMDB.decodeU32(dataSize);
            break;
        // case 8:
        //     return MMDB.decodeI32(dataSize);
        //     break;
        case 9:
            return MMDB.decodeU64(dataSize);
            break;
        // case 10:
        //     return MMDB.decodeU128(dataSize);
        //     break;
        case 7:
            return MMDB.decodeMap(dataSize);
            break;
        case 11:
            return MMDB.decodeArray(dataSize);
            break;
        // case 12:
        //     return MMDB.decodeDataCacheContainer(dataSize);
        //     break;
        case 13:
            return MMDB.decodeEndMarker(dataSize);
            break;
        // case 14:
        //     return MMDB.decodeBoolean(dataSize);
        //     break;
        // case 15:
        //     return MMDB.decodeFloat(dataSize);
        //     break;
        default:
            throw "can't deal with data format " + dataFormat;
    }
}

MMDB.decodeString <- function(size)
{
    print("string length = " + size + "\n");
    local pos = MMDB.File.tell();
    local b = MMDB.File.readblob(size);
    local result = MMDB.blobToString(b);
    MMDB.File.seek(pos + size);
    return result;
}

MMDB.decodeDouble <- function(size)
{
    print("double size = " + size + "\n");
    local pos = MMDB.File.tell();
    local result = MMDB.File.readn('d');
    MMDB.File.seek(pos + 8);
    return result;
}

MMDB.decodeU16 <- function(size)
{
    print("U16 size = " + size + "\n");
    if (size > 2)
    {
        throw "unexpected size = " + size + " for U16.";
    }
    return MMDB.readNumber(size);
}

MMDB.decodeU32 <- function(size)
{
    print("U32 size = " + size + "\n");
    if (size > 4)
    {
        throw "unexpected size = " + size + " for U32.";
    }
    return MMDB.readNumber(size);
}

MMDB.decodeU64 <- function(size)
{
    print("U64 size = " + size + "\n");
    if (size > 8)
    {
        throw "unexpected size = " + size + " for U64.";
    }
    return MMDB.readNumber(size);
}

MMDB.decodeMap <- function(entryCount)
{
    local result = {};

    print("[MAP]\nmap entry count = " + entryCount + "\n");

    for (local readCount = 0; readCount < entryCount; ++readCount)
    {
        local key = MMDB.decodeData();
        print("map entry key = " + key + "\n");
        local value = MMDB.decodeData();
        print("map entry value = " + value + "\n");
        result[key] <- value;
    }

    print("[END MAP]\n");

    return result;
}

MMDB.decodeArray <- function(itemCount)
{
    local result = [];

    print("[ARRAY]\narray item count = " + itemCount + "\n");

    for (local readCount = 0; readCount < itemCount; ++readCount)
    {
        local value = MMDB.decodeData();
        print("list item value = " + value + "\n");
        result.push(value);
    }

    print("[END ARRAY]\n");

    return result;
}
