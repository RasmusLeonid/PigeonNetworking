#if UNITY_STANDALONE

using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using PigeonNetworking;
using System;


public static class MessageExtensions 
{
    #region Vector2
    /// <summary>Adds a <see cref="Vector2"/> to the message.</summary>
    /// <param name="value">The <see cref="Vector2"/> to add.</param>
    /// <returns>The message that the <see cref="Vector2"/> was added to.</returns>
    public static Message Add(this Message message, Vector2 value)
    {
        message.Add(value.x);
        message.Add(value.y);
        return message;
    }

    /// <summary>Retrieves a <see cref="Vector2"/> from the message.</summary>
    /// <returns>The <see cref="Vector2"/> that was retrieved.</returns>
    public static Vector2 ReadVector2(this Message message)
    {
        return new Vector2(message.GetFloat(), message.GetFloat());
    }
    
    /// <summary>Retrieves a <see cref="Vector2"/> from the message.</summary>
    /// <returns>The <see cref="Vector2"/> that was retrieved.</returns>
    public static Vector2 GetVector2(this Message message) => message.ReadVector2();
    
    /// <summary>Adds a <see cref="Vector3"/> to the message.</summary>
    /// <param name="value">The <see cref="Vector3"/> to add.</param>
    /// <returns>The message that the <see cref="Vector3"/> was added to.</returns>
    public static Message WriteVector2(this Message message, Vector2 value)
    {
        message.WriteFloat(value.x);
        message.WriteFloat(value.y);
        return message;
    }
    public static Message AddVector2(this Message message, Vector2 value) => message.WriteVector2(value);
    #endregion

    #region Vector3
    /// <summary>Adds a <see cref="Vector3"/> to the message.</summary>
    /// <param name="value">The <see cref="Vector3"/> to add.</param>
    /// <returns>The message that the <see cref="Vector3"/> was added to.</returns>
    public static Message Add(this Message message, Vector3 value)
    {
        message.AddFloat(value.x);
        message.AddFloat(value.y);
        message.AddFloat(value.z);
        return message;
    }

    /// <summary>Adds a <see cref="Vector3"/> to the message.</summary>
    /// <param name="value">The <see cref="Vector3"/> to add.</param>
    /// <returns>The message that the <see cref="Vector3"/> was added to.</returns>
    public static Message WriteVector3(this Message message, Vector3 value)
    {
        message.WriteFloat(value.x);
        message.WriteFloat(value.y);
        message.WriteFloat(value.z);
        return message;
    }
    public static Message AddVector3(this Message message, Vector3 value) => message.WriteVector3(value);

    /// <summary>Retrieves a <see cref="Vector3"/> from the message.</summary>
    /// <returns>The <see cref="Vector3"/> that was retrieved.</returns>
    public static Vector3 ReadVector3(this Message message)
    {
        return new Vector3(message.ReadFloat(), message.ReadFloat(), message.ReadFloat());
    }
    public static Vector3 GetVector3(this Message message) => message.ReadVector3();


    /// <summary>Adds a <see cref="Vector3[]"/> to the message.</summary>
    /// <param name="value">The <see cref="Vector3[]"/> to add.</param>
    /// <returns>The message that the <see cref="Vector3[]"/> was added to.</returns>
    public static Message Add(this Message message, Vector3[] value)
    {
        message.Add(value.Length);
        for (int i = 0; i < value.Length; i++)
        {
            message.Add(value[i]);
        }
        return message;
    }

    /// <summary>Retrieves a <see cref="Vector3[]"/> from the message.</summary>
    /// <returns>The <see cref="Vector3[]"/> that was retrieved.</returns>
    public static Vector3[] ReadVector3s(this Message message)
    {
        int length = message.ReadInt();
        Vector3[] array = new Vector3[length];
        for (int i = 0; i < array.Length; i++)
        {
            array[i] = message.ReadVector3();
        }
        return array;
    }
    
    
    /// <summary>Retrieves a <see cref="Vector3[]"/> from the message.</summary>
    /// <returns>The <see cref="Vector3[]"/> that was retrieved.</returns>
    public static Vector3[] GetVector3s(this Message message) => message.ReadVector3s();
    #endregion

    #region Quaternion
    /// <summary>Adds a <see cref="Quaternion"/> to the message.</summary>
    /// <param name="value">The <see cref="Quaternion"/> to add.</param>
    /// <returns>The message that the <see cref="Quaternion"/> was added to.</returns>
        public static Message Add(this Message message, Quaternion value)
        {
            message.AddFloat(value.x);
            message.AddFloat(value.y);
            message.AddFloat(value.z);
            message.AddFloat(value.w);
            return message;
        }
    /// <summary>Adds a <see cref="Quaternion"/> to the message.</summary>
    /// <param name="value">The <see cref="Quaternion"/> to add.</param>
    /// <returns>The message that the <see cref="Quaternion"/> was added to.</returns>
    public static Message WriteQuaternion(this Message message, Quaternion value)
    {
        message.WriteFloat(value.x);
        message.WriteFloat(value.y);
        message.WriteFloat(value.z);
        message.WriteFloat(value.w);
        return message;
    }
    
    /// <summary>Adds a <see cref="Quaternion"/> to the message.</summary>
    /// <param name="value">The <see cref="Quaternion"/> to add.</param>
    /// <returns>The message that the <see cref="Quaternion"/> was added to.</returns>
    public static Message AddQuaternion(this Message message, Quaternion value) => message.WriteQuaternion(value);

    
    /// <summary>Retrieves a <see cref="Quaternion"/> from the message.</summary>
    /// <returns>The <see cref="Quaternion"/> that was retrieved.</returns>
    public static Quaternion ReadQuaternion(this Message message)
    {
        return new Quaternion(message.ReadFloat(), message.ReadFloat(), message.ReadFloat(), message.ReadFloat());
    }

    public static Message Add(this Message message, Quaternion[] value)
    {
        message.WriteInt(value.Length);
        for (int i = 0; i < value.Length; i++)
        {
            message.WriteQuaternion(value[i]);
        }
        return message;
    }
    public static Message WriteQuaternions(this Message message, Quaternion[] value)
    {
        message.WriteInt(value.Length);
        for (int i = 0; i < value.Length; i++)
        {
            message.WriteQuaternion(value[i]);
        }
        return message;
    }

    public static Quaternion[] ReadQuaternions(this Message message)
    {
        int length = message.ReadInt();
        Quaternion[] array = new Quaternion[length];
        for (int i = 0; i < array.Length; i++)
        {
            array[i] = message.ReadQuaternion();
        }
        return array;
    }
    public static Quaternion[] GetQuaternions(this Message message) => message.ReadQuaternions();
    
     #endregion
}
#endif
