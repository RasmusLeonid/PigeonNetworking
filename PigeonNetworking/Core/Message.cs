using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using PigeonNetworking.Core;
using PigeonNetworking.Exceptions;


namespace PigeonNetworking
{
 public enum  EChannel : byte
{
    Unreliable = 0,
    Reliable = 1,
}
public enum EHeader : byte
{
    /// <summary>Unreliable channel</summary>
    Unreliable = 0,
    /// <summary>Reliable channel - ensures delivery, but not order</summary>
    Reliable = 1,
    /// <summary>Ack for reliable channel </summary>
    Ack = 2,
    /// <summary> Heartbeat for connection and ping mesurment - unreliable </summary>
    Heartbeat = 3,
    /// <summary>An internal reliable client connected message.</summary>
    ClientConnected = 4,
    /// <summary>An internal reliable client disconnected message.</summary>
    ClientDisconnected = 5,
    /// <summary>An internal client connection attempt.</summary>
    ConnectRequest = 6,
    /// <summary>An internal accept message.</summary>
    AcceptConnection = 7,
    /// <summary>An internal client connection attempt.</summary>
    RejectConnection = 8,
    /// <summary>Client is forcibly disconnected on the server.</summary>
    Kick = 9,
    /// <summary>
    /// used to send byte arrays segments (for 
    /// </summary>
    ReliableByteStream = 10,
    /// <summary>
    /// Placeholder, should never be used
    /// </summary>
    NotSet = 11,
    
    NagledMessageBatch = 12
   
}
public class Message
{

    internal const int MaxSize = 1170; // Adjust based on MTU, previous 1225
    private const int HeaderSize = 1; 
    
    private byte[] _buffer = new byte[MaxSize];
    private int _writePos = HeaderSize;
    private int _readPos = HeaderSize;
    private EChannel channel = 0;
    private EHeader _header = EHeader.NotSet;
    private ushort messageId = 0;


    internal int BytesInUse => _writePos;
    
    /// <summary>
    /// Warning!!! Only set on sending end
    /// </summary>
    internal EChannel GetChannel() => channel;
    
    internal void SetChannel(EChannel channel)
    {
        this.channel = channel;
    }
 
    internal ushort GetMessageID()
    {
        return BinaryPrimitives.ReadUInt16LittleEndian(_buffer.AsSpan(1));
    }
    
    internal byte[] GetBuffer() => _buffer;
    
    internal ArraySegment<byte> GetSegment()
        => new ArraySegment<byte>(_buffer, 0, _writePos);
    
    
    /// <summary>Gets a completely empty message instance with no header.</summary>
    /// <returns>An empty message instance.</returns>
    internal static Message Create()
    {
        Message message = RetrieveFromPool();
        message.Reset();
        return message;
    }
    

    /// <summary>
    /// Only set on receiving end
    /// </summary>
    /// <param name="size"></param>
    internal void SetBytesInUse(int size) => _writePos = size;

    internal static Message Create(byte[] buffer, int bufferSize)
    {
        Message message = RetrieveFromPool();
        message.Reset();
        
        if (bufferSize > MaxSize)
            throw new MessageReadWriteException("Buffer size exceeds maximum size");
        
        Buffer.BlockCopy(buffer, 0, message._buffer, 0, bufferSize);
        message._writePos = bufferSize;
        return message;
    }

    
    //todo: check for GC improvements
    internal static Message Create(byte[] buffer)
    {
        Message message = RetrieveFromPool();
        message.Reset();
        
        if (buffer.Length > MaxSize)
            throw new MessageReadWriteException("Buffer size exceeds maximum size");
        
        Buffer.BlockCopy(buffer, 0, message._buffer, 0, buffer.Length);
        message._writePos = buffer.Length;
        
        message.SetReceivedHeader((EHeader)buffer[0]);
        return message;
    }
    
    internal static Message Create(IntPtr m_pData, int bufferLength)
    {
        Message message = RetrieveFromPool();
        message.Reset();
        
        byte[] tempBuffer = ArrayPool<byte>.Shared.Rent(bufferLength);
        try
        {
            Marshal.Copy(m_pData, tempBuffer, 0, bufferLength);
            Buffer.BlockCopy(tempBuffer, 0, message._buffer, 0, bufferLength);
        }
        catch (Exception e)
        {
           PnLog.LogException("Exception in Message.Create(IntPtr m_pData, int bufferLenght" ,e);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(tempBuffer);
        }

        return message;
    }
    
    
    
    /// <summary>How many messages to add to the pool for each <see cref="Server"/> or <see cref="Client"/> instance that is started.</summary>
    /// <remarks>Changes will not affect <see cref="Server"/> and <see cref="Client"/> instances which are already running until they are restarted.</remarks>
    internal static byte InstancesPerPeer { get; set; } = 4;
    /// <summary>A pool of reusable message instances.</summary>
    private static readonly ConcurrentBag<Message> pool = new ConcurrentBag<Message>(); // (InstancesPerPeer * 2)

    private static int LastReliableMessageId = 0;

    private int _isReleased = 0;
    
    /// <summary>Retrieves a message instance from the pool. If none is available, a new instance is created.</summary>
    /// <returns>A message instance ready to be used for sending or handling.</returns>
    private static Message RetrieveFromPool()
    {
        Message message;
        if (pool.TryTake(out var m))
        {
            message = m;
        }
        else
        {
            message = new Message();
        }

     
        Interlocked.Exchange(ref message._isReleased, 0);
        return message;
    }

    /// <summary>Recycles the Message instance to the pool for reuse. !!!WARNING!!!: this is handled internally, do not manually call this because it can lead big problems</summary>
    internal void Release()
    {
        Reset();
        if (Interlocked.Exchange(ref _isReleased, 1) == 0)//if(!pool.Contains(this))
        {
            if(pool.Count < BaseClientRegistry.totalStaticPeerCount * 4)
                pool.Add(this); 
        } 
    }

    /// <summary>
    /// trim the pool, for instance on peer disconnection
    /// </summary>
    internal static void Trim()
    {
        if (pool.Count > (BaseClientRegistry.totalStaticPeerCount + 1) * 5)
        {
            int initialCount = pool.Count;
            while (pool.Count > (BaseClientRegistry.totalStaticPeerCount * 4) + 1)
            {
                pool.TryTake(out _);
            }
            PnLog.Log($"[PN Message] Trimmed pool from {initialCount} down to {pool.Count} Messages");
        }
    }
    
    //unused/not needed for lanes, unless you want multiple lanes on the same port, in that case, transports need to be adjusted to be able to handle this
    #region HeaderEncoding
    private byte EncodeHeader(byte header, byte laneId)
    {
        return (byte)((header << 4) | (laneId & 0x0F));
    }
    public static byte GetHeaderFromCombinedHeader(byte combined)
    {
        return (byte)(combined >> 4);
    }

    public static byte GetLaneIdFromCombinedHeader(byte combined)
    {
        return (byte)(combined & 0x0F);
    }
/// <summary>
/// call this internally right before sending, to write the lane ID into the header
/// </summary>
/// <param name="lane"></param>
    internal void SetCombinedHeader(byte lane)
    {
       _buffer[0] = EncodeHeader((byte)_header, lane);
       if (_header == EHeader.NotSet)
       {
           throw new Exception("Message header not set, trying to combine 'Not Set' header with Lane");
       }
    }
    #endregion
    
    public static Message Create(EChannel channel, ushort id)
    {
       Message message = RetrieveFromPool();
       message.Reset();
       message.SetHeader((EHeader)(byte)channel, false);
       message.WriteUShort(id);
       message.channel = channel;
       return message;
    }
    
    public static Message Create<TEnum>(EChannel channel, TEnum id) where TEnum : Enum
    {
        ushort ushortId = Convert.ToUInt16(id);
        return Create(channel, ushortId);
    }


    internal void SetReceivedHeader(EHeader header)
    {
        _buffer[0] = (byte)header;
        _header = header;
    }
    
    
    /// <summary>
    /// Sets a header and adds messageID for non unreliable messages
    /// </summary>
    /// <param name="header">the header for our message</param>
    /// <param name="reliable">adds a message sequenceId right after the header</param>
    internal void SetHeader(EHeader header, bool reliable) // false
    {
        _buffer[0] = (byte)header;
        _header = header;
        if (header != EHeader.Reliable && !reliable) return;
        
        ushort id = (ushort)Interlocked.Increment(ref LastReliableMessageId);;
        SetChannel(EChannel.Reliable);
        WriteUShort(id);
        messageId = id;
    }
    


    
    internal void Reset()
    {
        _writePos = HeaderSize;
        _readPos = HeaderSize;

        _header = EHeader.NotSet;
        channel = 0;
        messageId = 0;
    }
    
    private void EnsureReadable(int bytes)
    {
        if (_readPos + bytes > MaxSize) 
        {
            throw new MessageReadWriteException("Attempted to read beyond message size");
        }
    }

    private void EnsureWritable(int bytes)
    {
        if (_writePos + bytes > MaxSize)
        {
            PnLog.LogError("Message buffer overflow, Attempted to write beyond message size. Consider using the SendReliableByteStream method instead");
            throw new MessageReadWriteException("Message buffer overflow, Attempted to write beyond message size. Consider using the SendReliableByteStream method instead");
        }
    }
    public void WriteByte(byte value)
    {
        EnsureWritable(1);
        _buffer[_writePos++] = value;
    }

    public byte ReadByte()
    { 
        EnsureReadable(1);
        return _buffer[_readPos++];
    }
    
    public void WriteShort(short value)
    {
        EnsureWritable(2);
        BinaryPrimitives.WriteInt16LittleEndian(_buffer.AsSpan(_writePos), value);
        _writePos += 2;
    }

    public short ReadShort()
    {
        EnsureReadable(2);
        var value = BinaryPrimitives.ReadInt16LittleEndian(_buffer.AsSpan(_readPos));
        _readPos += 2;
        return value;
    }
    
    public void WriteUShort(ushort value)
    {
        EnsureWritable(2);
        BinaryPrimitives.WriteUInt16LittleEndian(_buffer.AsSpan(_writePos), value);
        _writePos += 2;
    }

    public ushort ReadUShort()
    {
        EnsureReadable(2);
        var value = BinaryPrimitives.ReadUInt16LittleEndian(_buffer.AsSpan(_readPos));
        _readPos += 2;
        return value;
    }
    
    public void WriteInt(int value)
    {
        EnsureWritable(4);
        BinaryPrimitives.WriteInt32LittleEndian(_buffer.AsSpan(_writePos), value);
        _writePos += 4;
    }

    public int ReadInt()
    {
        EnsureReadable(4);
        var value = BinaryPrimitives.ReadInt32LittleEndian(_buffer.AsSpan(_readPos));
        _readPos += 4;
        return value;
    }
    
    public void WriteUInt(uint value)
    {
        EnsureWritable(4);
        BinaryPrimitives.WriteUInt32LittleEndian(_buffer.AsSpan(_writePos), value);
        _writePos += 4;
    }

    public uint ReadUInt()
    {
        EnsureReadable(4);
        var value = BinaryPrimitives.ReadUInt32LittleEndian(_buffer.AsSpan(_readPos));
        _readPos += 4;
        return value;
    }
    
    public void WriteLong(long value)
    {
        EnsureWritable(8);
        BinaryPrimitives.WriteInt64LittleEndian(_buffer.AsSpan(_writePos), value);
        _writePos += 8;
    }

    public long ReadLong()
    {
        EnsureReadable(8);
        var value = BinaryPrimitives.ReadInt64LittleEndian(_buffer.AsSpan(_readPos));
        _readPos += 8;
        return value;
    }
    
    
    public void WriteULong(ulong value)
    {
        EnsureWritable(8);
        BinaryPrimitives.WriteUInt64LittleEndian(_buffer.AsSpan(_writePos), value);
        _writePos += 8;
    }

    public ulong ReadULong()
    {
        EnsureReadable(8);
        var value = BinaryPrimitives.ReadUInt64LittleEndian(_buffer.AsSpan(_readPos));
        _readPos += 8;
        return value;
    }
    
    public void WriteFloat(float value)
    {
        EnsureWritable(sizeof(float));
        var span = _buffer.AsSpan(_writePos, sizeof(float));
        MemoryMarshal.Write(span, ref value);
        _writePos += sizeof(float);
    }

    public float ReadFloat()
    {
        EnsureReadable(sizeof(float));
        var span = _buffer.AsSpan(_readPos, sizeof(float));
        float value = MemoryMarshal.Read<float>(span);
        _readPos += sizeof(float);
        return value;
    }

    public void WriteDouble(double value)
    {
        EnsureWritable(sizeof(double));
        var span = _buffer.AsSpan(_writePos, sizeof(double));
        MemoryMarshal.Write(span, ref value);
        _writePos += sizeof(double);
        
    }

    public double ReadDouble()
    {
        EnsureReadable(sizeof(double));
        var span = _buffer.AsSpan(_readPos, sizeof(double));
        double value = MemoryMarshal.Read<double>(span);
        _readPos += sizeof(double);
        return value;
    }
    public void WriteSmartString(string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        if (bytes.Length <= byte.MaxValue)
        {
            WriteByte(0); // Indicates small string
            WriteByte((byte)bytes.Length);
        }
        else
        {
            WriteByte(1); // Indicates large string
            WriteUShort((ushort)bytes.Length);
        }
        EnsureWritable(bytes.Length);
        bytes.CopyTo(_buffer, _writePos);
        _writePos += bytes.Length;
    }
    
    
    // private void WriteSmallString(string value)
    // {
    //     var bytes = Encoding.UTF8.GetBytes(value);
    //     if (bytes.Length > byte.MaxValue)
    //         throw new Exception("String too long for small string. Use WriteLargeString or WriteSmartString.");
    //
    //     WriteByte((byte)bytes.Length);
    //     EnsureWritable(bytes.Length);
    //     bytes.CopyTo(_buffer, _writePos);
    //     _writePos += bytes.Length;
    //    
    // }
    //
    // private void WriteLargeString(string value)
    // {
    //     var bytes = Encoding.UTF8.GetBytes(value);
    //     if (bytes.Length > ushort.MaxValue)
    //         throw new Exception("String too long for large string. Max 65535 bytes.");
    //
    //     WriteUShort((ushort)bytes.Length);
    //     EnsureWritable(bytes.Length);
    //     bytes.CopyTo(_buffer, _writePos);
    //     _writePos += bytes.Length;
    //
    //
    // }
    
    public string ReadSmartString()
    {
        byte flag = ReadByte();
        int length = flag == 0 ? ReadByte() : ReadUShort();
        EnsureReadable(length);
        var str = Encoding.UTF8.GetString(_buffer, _readPos, length);
        _readPos += length;
        return str;
    }
    
    public void WriteBool(bool value)
    {
        EnsureWritable(1);
        WriteByte(value ? (byte)1 : (byte)0);
    }

    public bool ReadBool()
    {
        EnsureReadable(1);
        return ReadByte() != 0;
    }

    #region Array functions

    public void WriteBytes(byte[] value)
    {
        WriteUShort((ushort)value.Length);
        EnsureWritable(value.Length); 
        Buffer.BlockCopy(value, 0, _buffer, _writePos, value.Length);
        _writePos += value.Length;
    }
    
    public void WriteArraySegment(ArraySegment<byte> segment)
    {
        WriteUShort((ushort)segment.Count);              // store correct size
        EnsureWritable(segment.Count);
        Buffer.BlockCopy(segment.Array, segment.Offset, _buffer, _writePos, segment.Count);
        _writePos += segment.Count;
    }
    
    public ArraySegment<byte> ReadArraySegment()
    {
        ushort length = ReadUShort(); // get length
        EnsureReadable(length);       // validate bounds
        var segment = new ArraySegment<byte>(_buffer, _readPos, length);
        _readPos += length;
        return segment;
    }
    
    /// <summary>
    /// reads a byte array from the message and returns it as a new byte array.
    /// Consider using the overload variant of this method and parse an existing byte array to copy to, if you want
    /// to keep GC pressure to a minimum.
    /// </summary>
    /// <returns></returns>
    public byte[] ReadBytes()
    {
       ushort byteCount = ReadUShort();
       EnsureReadable(byteCount);
       byte[] result = new byte[byteCount];
       Buffer.BlockCopy(_buffer, _readPos, result, 0, byteCount);
       _readPos += byteCount;
       return result;
    }
    


    /// <summary>
    /// copies readBytes into the outBuffer param, this is a more GC friendly approach then simply doing ReadBytes
    /// which creates a new byte array
    /// </summary>
    /// <param name="outBuffer">the received array</param>
    /// <returns>the size of the array</returns>
    /// <exception cref="ArgumentException"></exception>
    public int ReadBytes(byte[] outBuffer)
    {
        ushort byteCount = ReadUShort();
        EnsureReadable(byteCount);
        
        if (outBuffer.Length < byteCount)
            throw new ArgumentException("Destination buffer too small.");
        Buffer.BlockCopy(_buffer, _readPos, outBuffer, 0, byteCount);
        _readPos += byteCount;
        return byteCount;
    }
    
    /// <summary>
    /// copies readBytes into the outBuffer param, this is a more GC friendly approach then simply doing ReadBytes
    /// which creates a new byte array
    /// </summary>
    /// <param name="outBuffer">the received array</param>
    /// <returns>the size of the array</returns>
    /// <exception cref="ArgumentException"></exception>
    public int ReadBytes(byte[] outBuffer, int expectedSize)
    {
        EnsureReadable(expectedSize);
        if (outBuffer.Length < expectedSize)
            throw new ArgumentException("Destination buffer too small.");
        Buffer.BlockCopy(_buffer, _readPos, outBuffer, 0, expectedSize);
        _readPos += expectedSize;
        return expectedSize;
    }
    
    
    public void WriteShorts(short[] values)
    {
        WriteUShort((ushort)values.Length);
        foreach (var val in values)
            WriteShort(val);
    }

    public void WriteUShorts(ushort[] values)
    {
        WriteUShort((ushort)values.Length);
        foreach (var val in values)
            WriteUShort(val);
    }

    public void WriteInts(int[] values)
    {
        WriteUShort((ushort)values.Length);
        foreach (var val in values)
            WriteInt(val);
    }

    public void WriteUInts(uint[] values)
    {
        WriteUShort((ushort)values.Length);
        foreach (var val in values)
            WriteUInt(val);
    }

    public void WriteLongs(long[] values)
    {
        WriteUShort((ushort)values.Length);
        foreach (var val in values)
            WriteLong(val);
    }

    public void WriteULongs(ulong[] values)
    {
        WriteUShort((ushort)values.Length);
        foreach (var val in values)
            WriteULong(val);
    }

    public void WriteFloats(float[] values)
    {
        WriteUShort((ushort)values.Length);
        foreach (var val in values)
            WriteFloat(val);
    }

    public void WriteDoubles(double[] values)
    {
        WriteUShort((ushort)values.Length);
        foreach (var val in values)
            WriteDouble(val);
    }

    public void WriteBools(bool[] values)
    {
        WriteUShort((ushort)values.Length);
        foreach (var val in values)
            WriteBool(val);
    }

    public void WriteStrings(string[] values)
    {
        WriteUShort((ushort)values.Length);
        foreach (var val in values)
            WriteSmartString(val);
    }


    public short[] ReadShorts()
    {
        var count = ReadUShort();
        var result = new short[count];
        for (int i = 0; i < count; i++)
            result[i] = ReadShort();
        return result;
    }

    public ushort[] ReadUShorts()
    {
        var count = ReadUShort();
        var result = new ushort[count];
        for (int i = 0; i < count; i++)
            result[i] = ReadUShort();
        return result;
    }

    public int[] ReadInts()
    {
        var count = ReadUShort();
        var result = new int[count];
        for (int i = 0; i < count; i++)
            result[i] = ReadInt();
        return result;
    }

    public uint[] ReadUInts()
    {
        var count = ReadUShort();
        var result = new uint[count];
        for (int i = 0; i < count; i++)
            result[i] = ReadUInt();
        return result;
    }

    public long[] ReadLongs()
    {
        var count = ReadUShort();
        var result = new long[count];
        for (int i = 0; i < count; i++)
            result[i] = ReadLong();
        return result;
    }

    public ulong[] ReadULongs()
    {
        var count = ReadUShort();
        var result = new ulong[count];
        for (int i = 0; i < count; i++)
            result[i] = ReadULong();
        return result;
    }

    public float[] ReadFloats()
    {
        var count = ReadUShort();
        var result = new float[count];
        for (int i = 0; i < count; i++)
            result[i] = ReadFloat();
        return result;
    }

    public double[] ReadDoubles()
    {
        var count = ReadUShort();
        var result = new double[count];
        for (int i = 0; i < count; i++)
            result[i] = ReadDouble();
        return result;
    }

    public bool[] ReadBools()
    {
        var count = ReadUShort();
        var result = new bool[count];
        for (int i = 0; i < count; i++)
            result[i] = ReadBool();
        return result;
    }

    public string[] ReadStrings()
    {
        var count = ReadUShort();
        var result = new string[count];
        for (int i = 0; i < count; i++)
            result[i] = ReadSmartString();
        return result;
    }



    #endregion



    #region Wrapper functions for Riptide pattern compatibility 

    public void AddBool(bool value) =>  WriteBool(value);
    public void Add(bool value) =>  WriteBool(value);

    public bool GetBool() => ReadBool();

    public void Add(ulong value) => WriteULong(value);
    public ulong GetULong() => ReadULong();

    public void Add(string value) => WriteSmartString(value);
    public void AddString(string value) => WriteSmartString(value);
    public void WriteString(string value) => WriteSmartString(value);
    public string GetString() => ReadSmartString();
    public string ReadString() => ReadSmartString();

    public void AddUInt(uint value) => WriteUInt(value);

    public void AddULong(ulong value) => WriteULong(value);

 
    
    public void AddBytes(byte[] values) => WriteBytes(values);
    public void Add(byte[] values) => WriteBytes(values);
    
    /// <summary>
    /// reads a byte array from the message and returns it as a new byte array.
    /// Consider using ReadBytes with overloads and parse an existing byte array to copy to, if you want
    /// to keep GC pressure to a minimum.
    /// </summary>
    /// <returns></returns>
    public byte[] GetBytes() => ReadBytes();

    public void AddInt(int value) => WriteInt(value);
    
    public void Add(short value) => WriteShort(value);
    public void Add(ushort value) => WriteUShort(value);
    public void Add(int value) => WriteInt(value);
    public void Add(uint value) => WriteUInt(value);
    public void Add(long value) => WriteLong(value);
    public void Add(float value) => WriteFloat(value);
    public void Add(double value) => WriteDouble(value);

    public void AddUShort(ushort value) => WriteUShort(value);

    public short GetShort() => ReadShort();
    public ushort GetUShort() => ReadUShort();
    public int GetInt() => ReadInt();
    public uint GetUInt() => ReadUInt();
    public long GetLong() => ReadLong();
    public float GetFloat() => ReadFloat();
    public double GetDouble() => ReadDouble();
    
    
    
    public void AddShortArray(short[] values) => WriteShorts(values);
    public void AddUShortArray(ushort[] values) => WriteUShorts(values);
    public void AddIntArray(int[] values) => WriteInts(values);
    public void AddUIntArray(uint[] values) => WriteUInts(values);
    public void AddLongArray(long[] values) => WriteLongs(values);
    public void AddULongArray(ulong[] values) => WriteULongs(values);
    public void AddFloatArray(float[] values) => WriteFloats(values);
    public void AddDoubleArray(double[] values) => WriteDoubles(values);
    public void AddBoolArray(bool[] values) => WriteBools(values);
    public void AddStringArray(string[] values) => WriteStrings(values);
    public void AddByteArray(byte[] values) => WriteBytes(values); // Already exists

    public short[] GetShortArray() => ReadShorts();
    public ushort[] GetUShortArray() => ReadUShorts();
    public int[] GetIntArray() => ReadInts();
    public uint[] GetUIntArray() => ReadUInts();
    public long[] GetLongArray() => ReadLongs();
    public ulong[] GetULongArray() => ReadULongs();
    public float[] GetFloatArray() => ReadFloats();
    public double[] GetDoubleArray() => ReadDoubles();
    public bool[] GetBoolArray() => ReadBools();
    public string[] GetStringArray() => ReadStrings();

    
    
    public void AddShorts(short[] values) => AddShortArray(values);
    public short[] GetShorts() => GetShortArray();

    public void AddUShorts(ushort[] values) => AddUShortArray(values);
    public ushort[] GetUShorts() => GetUShortArray();

    public void AddInts(int[] values) => AddIntArray(values);
    public int[] GetInts() => GetIntArray();

    public void AddUInts(uint[] values) => AddUIntArray(values);
    public uint[] GetUInts() => GetUIntArray();

    public void AddLongs(long[] values) => AddLongArray(values);
    public long[] GetLongs() => GetLongArray();

    public void AddULongs(ulong[] values) => AddULongArray(values);
    public ulong[] GetULongs() => GetULongArray();

    public void AddFloats(float[] values) => AddFloatArray(values);
    public float[] GetFloats() => GetFloatArray();

    public void AddDoubles(double[] values) => AddDoubleArray(values);
    public double[] GetDoubles() => GetDoubleArray();

    public void AddBools(bool[] values) => AddBoolArray(values);
    public bool[] GetBools() => GetBoolArray();

    public void AddStrings(string[] values) => AddStringArray(values);
    public string[] GetStrings() => GetStringArray();

    public void AddBytesArray(byte[] values) => AddByteArray(values); // To match naming
    public byte[] GetBytesArray() => GetBytes();

    #endregion


    #region Extension Wrappers
    
    public Message AddFloat(float value)
    {
        WriteFloat(value);
        return this;
    }

    #endregion

}
}

