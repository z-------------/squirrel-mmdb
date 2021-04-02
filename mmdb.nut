MMDB <- {
    Metadata = null

    metadataMarker = blob(14)
}

// "\xab\xcd\xefMaxMind.com"
MMDB.metadataMarker[0]  = 171;
MMDB.metadataMarker[1]  = 205;
MMDB.metadataMarker[2]  = 239;
MMDB.metadataMarker[3]  = 77;
MMDB.metadataMarker[4]  = 97;
MMDB.metadataMarker[5]  = 120;
MMDB.metadataMarker[6]  = 77;
MMDB.metadataMarker[7]  = 105;
MMDB.metadataMarker[8]  = 110;
MMDB.metadataMarker[9]  = 100;
MMDB.metadataMarker[10] = 46;
MMDB.metadataMarker[11] = 99;
MMDB.metadataMarker[12] = 111;
MMDB.metadataMarker[13] = 109;

/* public interface */

MMDB.OpenFile <- function(filename)
{
    MMDB.f <- file(filename, "r");
    MMDB.f.seek(MMDB.getMetadataPos());
    MMDB.Metadata <- MMDB.decodeData();
}

MMDB.Lookup <- function(ipAddrStr)
{
    // print("LOOKUP START\n");

    local recordSizeBits = MMDB.Metadata.record_size;
    local nodeSizeBits = 2 * recordSizeBits;
    local nodeCount = MMDB.Metadata.node_count;
    local treeSize = ((recordSizeBits * 2) / 8) * nodeCount;

    if (MMDB.Metadata.ip_version != 6)
    {
        throw "only IPV6 databases are supported for now";
    }

    if (recordSizeBits != 24)
    {
        throw "only record size 24 is supported for now";
    }

    /* get IP address binary representation */

    local parts = split(ipAddrStr, ".");
    if (parts.len() != 4)
    {
        throw "only IPv4 addresses are supported for now";
    }

    local repr = blob(4);
    for (local i = 0; i < parts.len(); ++i)
    {
        local n = parts[i].tointeger();
        repr.writen(n, 'b');
    }

    MMDB.f.seek(96 * nodeSizeBits / 8); // skip first 96 nodes

    for (local i = 0; i < repr.len(); ++i)
    {
        local byte = repr[i];
        for (local j = 0; j < 8; ++j)
        {
            local bit = MMDB.getBit(byte, j);

            if (bit == 1) // right record
            {
                MMDB.f.seek(recordSizeBits / 8, 'c');
            } // else, if left record, we are already in the correct position
            local record = MMDB.readBytes((recordSizeBits / 8).tointeger());
            local recordVal = MMDB.blobReadNumber(record, recordSizeBits / 8);

            if (recordVal < nodeCount)
            {
                local absoluteOffset = recordVal * nodeSizeBits / 8;
                MMDB.f.seek(absoluteOffset);
            }
            else if (recordVal == nodeCount)
            {
                throw "IP address does not exist in database";
            }
            else // recordVal > nodeCount
            {
                local absoluteOffset = (recordVal - nodeCount) + treeSize; // given formula
                MMDB.f.seek(absoluteOffset);
                local info = MMDB.decodeData();
                return info;
            }
        }
    }
}

/* find blob in file */

MMDB.getMetadataPos <- function()
{
    local metadataMarkerPos = MMDB.rFindBlob(MMDB.metadataMarker);
    return metadataMarkerPos + MMDB.metadataMarker.len();
}

MMDB.findBlob <- function(str)
{
    local strLen = str.len();
    local i = 0;

    MMDB.f.seek(0);

    while (!MMDB.f.eos())
    {
        local data = MMDB.f.readblob(strLen);        
        if (MMDB.blobAreEqual(data, str))
        {
            return i;
        }
        MMDB.f.seek(++i);
    }

    return null;
}

MMDB.rFindBlob <- function(str)
{
    local strLen = str.len();
    local fileLen = MMDB.f.len();
    local i = strLen;

    MMDB.f.seek(-strLen, 'e');

    while (true)
    {
        local data = MMDB.f.readblob(strLen);        
        if (MMDB.blobAreEqual(data, str))
        {
            return fileLen - i;
        }

        if (i == fileLen) break;

        MMDB.f.seek(-(++i), 'e');
    }

    return null;
}

/* data helpers */

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

MMDB.blobToString <- function(b)
{
    local result = "";
    for (local i = 0; i < b.len(); ++i)
    {
        result += b[i].tochar();
    }
    return result;
}

MMDB.getBit <- function(byte, i)
{
    local mask = pow(2, 7 - i).tointeger();
    return (byte & mask) >>> (7 - i);
}

/* data decode */

MMDB.advance <- function()
{
    MMDB.f.seek(1, 'c');
}

MMDB.readBytes <- function(size)
{
    local pos = MMDB.f.tell();
    local data = blob(size);
    data.writeblob(MMDB.f.readblob(size));
    MMDB.f.seek(pos + size);
    return data;
}

MMDB.readByte <- function()
{
    return MMDB.readBytes(1)[0];
}

MMDB.readNumber <- function(size)
{
    local pos = MMDB.f.tell();
    local result = 0;

    for (local i = size - 1; i >= 0; --i)
    {
        local n = MMDB.readByte();
        result += n * pow(256, i);
    }

    MMDB.f.seek(pos + size);
    return result;
}

MMDB.blobReadNumber <- function(b, size)
{
    local result = 0;
    for (local i = 0; i < size; ++i)
    {
        local n = b[i];
        result += n * pow(256, size - i - 1);
    }
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

    if (dataSize == 29)
    {
        dataSize = 29 + MMDB.readByte();
    }
    else if (dataSize == 30)
    {
        dataSize = 285 + pow(2, 8)*MMDB.readByte() + MMDB.readByte();
    }
    else if (dataSize == 31)
    {
        dataSize = 65821 + pow(2, 16)*MMDB.readByte() + pow(2, 8)*MMDB.readByte() + MMDB.readByte();
    }

    return {
        f = dataFormat
        s = dataSize
    }
}

MMDB.decodeData <- function()
{
    local controlInfo = MMDB.readControlByte();
    local dataFormat = controlInfo.f;
    local dataSize = controlInfo.s;

    switch (dataFormat)
    {
        case 1:
            return MMDB.decodePointer(dataSize);
            break;
        case 2:
            return MMDB.decodeString(dataSize);
            break;
        case 3:
            return MMDB.decodeDouble(dataSize);
            break;
        case 4:
            return MMDB.decodeBytes(dataSize);
            break;
        case 5:
            return MMDB.decodeUInt(dataSize);
            break;
        case 6:
            return MMDB.decodeUInt(dataSize);
            break;
        case 8:
            return MMDB.decodeI32(dataSize);
            break;
        case 9:
            return MMDB.decodeUInt(dataSize);
            break;
        case 10:
            return MMDB.decodeUInt(dataSize);
            break;
        case 7:
            return MMDB.decodeMap(dataSize);
            break;
        case 11:
            return MMDB.decodeArray(dataSize);
            break;
        // case 12:
        //     return MMDB.decodeDataCacheContainer(dataSize);
        //     break;
        // case 13:
        //     return MMDB.decodeEndMarker(dataSize);
        //     break;
        case 14:
            return MMDB.decodeBoolean(dataSize);
            break;
        case 15:
            return MMDB.decodeFloat(dataSize);
            break;
        default:
            throw "can't deal with data format " + dataFormat;
    }
}

MMDB.decodePointer <- function(value)
{
    local result;

    // first two bits indicate size
    local size = 2*MMDB.getBit(value, 3 + 0) + MMDB.getBit(value, 3 + 1);

    // last three bits are used for the address
    local addrPart = 3*MMDB.getBit(value, 3 + 2) + 2*MMDB.getBit(value, 3 + 3) + MMDB.getBit(value, 3 + 4);

    local dataSectionStart = 16 + ((MMDB.Metadata.record_size * 2) / 8) * MMDB.Metadata.node_count; // given formula
    local p;
    if (size == 0)
    {
        p = pow(2, 8)*addrPart + MMDB.readByte();
    }
    else if (size == 1)
    {
        p = pow(2, 16)*addrPart + pow(2, 8)*MMDB.readByte() + MMDB.readByte() + 2048;
    }
    else if (size == 2)
    {
        p = pow(2, 24)*addrPart + pow(2, 16)*MMDB.readByte() + pow(2, 8)*MMDB.readByte() + MMDB.readByte() + 526336;
    }
    else if (size == 3)
    {
        p = MMDB.readNumber(4);
    }
    else
    {
        throw "invalid pointer size " + size;
    }
    local absoluteOffset = dataSectionStart + p;
    local localPos = MMDB.f.tell();

    MMDB.f.seek(absoluteOffset);
    result = MMDB.decodeData();

    MMDB.f.seek(localPos);
    
    return result;
}

MMDB.decodeString <- function(size)
{
    local pos = MMDB.f.tell();
    local b = MMDB.f.readblob(size);
    local result = MMDB.blobToString(b);
    MMDB.f.seek(pos + size);
    return result;
}

MMDB.decodeDouble <- function(size)
{
    local pos = MMDB.f.tell();
    local result = MMDB.f.readn('d');
    MMDB.f.seek(pos + 8);
    return result;
}

MMDB.decodeBytes <- function(size)
{
    local result;
    local pos = MMDB.f.tell();
    if (size == 0)
    {
        result = null;
    }
    else
    {
        result = MMDB.readBytes(size);
    }
    MMDB.f.seek(pos + size);
    return result;
}

MMDB.decodeUInt <- function(size)
{
    // if (size > 8)
    // {
    //     throw "unexpected size = " + size + " for UInt.";
    // }
    return MMDB.readNumber(size);
}

MMDB.decodeI32 <- function(size)
{
    if (size != 4)
    {
        throw "unexpected size = " + size + " for I32.";
    }
    local pos = MMDB.f.tell();
    local result = MMDB.f.readn('i');
    MMDB.f.seek(pos + 4);
    return result;
}

MMDB.decodeMap <- function(entryCount)
{
    local result = {};

    for (local readCount = 0; readCount < entryCount; ++readCount)
    {
        local key = MMDB.decodeData();
        local value = MMDB.decodeData();
        result[key] <- value;
    }

    return result;
}

MMDB.decodeArray <- function(itemCount)
{
    local result = [];

    for (local readCount = 0; readCount < itemCount; ++readCount)
    {
        local value = MMDB.decodeData();
        result.push(value);
    }

    return result;
}

MMDB.decodeBoolean <- function(value)
{
    return value ? true : false;
}

MMDB.decodeFloat <- function(size)
{
    local pos = MMDB.f.tell();
    local result = MMDB.f.readn('f');
    MMDB.f.seek(pos + 4);
    return result;
}
