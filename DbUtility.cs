using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.OracleClient;
using System.Data.OleDb;
using MySql.Data.MySqlClient;
using Oracle.ManagedDataAccess.Client;

namespace SklDataProvider
{
    /// <summary>
    /// 存储与数据库类型无关的方法,接口
    /// Created : zhuyf
    /// Date    : 2017-7-28
    /// </summary>
    public class DbUtility
    {
        public DbUtility()
        {
        }

        #region 中间层系统类型库到各数据库类型的转换
        protected static SqlDbType GetSqlDbType(NDbType dbType)
        {
            switch (dbType)
            {
                case NDbType.BigInt:
                    return SqlDbType.BigInt;
                case NDbType.Binary:
                    return SqlDbType.Binary;
                case NDbType.Bit:
                    return SqlDbType.Bit;
                case NDbType.Char:
                    return SqlDbType.Char;
                case NDbType.Date:
                    return SqlDbType.Date;
                case NDbType.DateTime:
                    return SqlDbType.DateTime;
                case NDbType.DateTime2:
                    return SqlDbType.DateTime2;
                case NDbType.Decimal:
                    return SqlDbType.Decimal;
                case NDbType.Float:
                    return SqlDbType.Float;
                case NDbType.Image:
                    return SqlDbType.Image;
                case NDbType.Int:
                    return SqlDbType.Int;
                case NDbType.Money:
                    return SqlDbType.Money;
                case NDbType.NChar:
                    return SqlDbType.NChar;
                case NDbType.NText:
                    return SqlDbType.NText;
                case NDbType.NVarChar:
                    return SqlDbType.NVarChar;
                case NDbType.Real:
                    return SqlDbType.Real;
                case NDbType.SmallInt:
                    return SqlDbType.SmallInt;
                case NDbType.SmallMoney:
                    return SqlDbType.SmallMoney;
                case NDbType.Text:
                    return SqlDbType.Text;
                case NDbType.TinyInt:
                    return SqlDbType.TinyInt;
                case NDbType.VarBinary:
                    return SqlDbType.VarBinary;
                case NDbType.VarChar:
                    return SqlDbType.VarChar;
            }
            return SqlDbType.NVarChar;
        }
        protected static OracleType GetOracleDbType(NDbType dbType)
        {
            switch (dbType)
            {
                case NDbType.BigInt:
                case NDbType.Bit:
                    return OracleType.Number;
                case NDbType.Binary:
                    return OracleType.Blob;
                case NDbType.Char:
                    return OracleType.Char;
                case NDbType.DateTime2:
                case NDbType.DateTime:
                    return OracleType.DateTime;
                case NDbType.Decimal:
                    return OracleType.Number;
                case NDbType.Float:
                    return OracleType.Float;
                case NDbType.Image:
                    return OracleType.Clob;
                case NDbType.Int:
                    return OracleType.Int32;
                case NDbType.Money:
                case NDbType.SmallMoney:
                    return OracleType.Number;
                case NDbType.NChar:
                    return OracleType.NChar;
                case NDbType.NText:
                    return OracleType.NClob;
                case NDbType.NVarChar:
                    return OracleType.NVarChar;
                case NDbType.Real:
                    return OracleType.Double;
                case NDbType.SmallInt:
                    return OracleType.Int16;
                case NDbType.Text:
                    return OracleType.Clob;
                case NDbType.TinyInt:
                    return OracleType.Byte;
                case NDbType.VarBinary:
                    return OracleType.Blob;
                case NDbType.VarChar:
                    return OracleType.VarChar;
            }
            return OracleType.VarChar;
        }
        protected static OleDbType GetOleDbType(NDbType dbType)
        {
            switch (dbType)
            {
                case NDbType.BigInt:
                    return OleDbType.BigInt;
                case NDbType.Binary:
                    return OleDbType.Binary;
                case NDbType.Bit:
                    return OleDbType.Boolean;
                case NDbType.Char:
                case NDbType.NChar:
                    return OleDbType.Char;
                case NDbType.Date:
                case NDbType.DateTime:
                case NDbType.DateTime2:
                    return OleDbType.Date;
                case NDbType.Decimal:
                    return OleDbType.Decimal;
                case NDbType.Float:
                    return OleDbType.Single;
                case NDbType.Image:
                    return OleDbType.Binary;
                case NDbType.Int:
                    return OleDbType.Integer;
                case NDbType.Money:
                    return OleDbType.Decimal;
                case NDbType.NText:
                    return OleDbType.LongVarWChar;
                case NDbType.NVarChar:
                    return OleDbType.VarWChar;
                case NDbType.Real:
                    return OleDbType.Double;
                case NDbType.SmallInt:
                    return OleDbType.SmallInt;
                case NDbType.SmallMoney:
                    return OleDbType.Decimal;
                case NDbType.Text:
                    return OleDbType.LongVarWChar;
                case NDbType.TinyInt:
                    return OleDbType.TinyInt;
                case NDbType.VarBinary:
                    return OleDbType.VarBinary;
                case NDbType.VarChar:
                    return OleDbType.VarChar;
            }
            return OleDbType.VarChar;
        }
        protected static Oracle.ManagedDataAccess.Client.OracleDbType GetOraOracleDbType(NDbType dbType)
        {
            switch (dbType)
            {
                case NDbType.BigInt:
                    return OracleDbType.Int64;
                case NDbType.Bit:
                    return OracleDbType.Decimal;
                case NDbType.Binary:
                    return OracleDbType.Blob;
                case NDbType.Char:
                    return OracleDbType.Char;
                case NDbType.DateTime2:
                case NDbType.DateTime:
                    return OracleDbType.Date;
                case NDbType.Decimal:
                    return OracleDbType.Decimal;
                case NDbType.Float:
                    return OracleDbType.Double;
                case NDbType.Image:
                    return OracleDbType.Clob;
                case NDbType.Int:
                    return OracleDbType.Int32;
                case NDbType.Money:
                case NDbType.SmallMoney:
                    return OracleDbType.Double;
                case NDbType.NChar:
                    return OracleDbType.NChar;
                case NDbType.NText:
                    return OracleDbType.NClob;
                case NDbType.NVarChar:
                    return OracleDbType.NVarchar2;
                case NDbType.Real:
                    return OracleDbType.Double;
                case NDbType.SmallInt:
                    return OracleDbType.Int16;
                case NDbType.Text:
                    return OracleDbType.Clob;
                case NDbType.TinyInt:
                    return OracleDbType.Byte;
                case NDbType.VarBinary:
                    return OracleDbType.Blob;
                case NDbType.VarChar:
                    return OracleDbType.Varchar2;
            }
            return OracleDbType.Varchar2;
        }
        protected static MySqlDbType GetMysqlDbType(NDbType dbType)
        {
            switch (dbType)
            {
                case NDbType.BigInt:
                    return MySqlDbType.Int64;
                case NDbType.Binary:
                    return MySqlDbType.Binary;
                case NDbType.Bit:
                    return MySqlDbType.Bit;
                case NDbType.Char:
                case NDbType.NChar:
                    return MySqlDbType.VarChar;
                case NDbType.Date:
                case NDbType.DateTime:
                case NDbType.DateTime2:
                    return MySqlDbType.Date;
                case NDbType.Decimal:
                    return MySqlDbType.Decimal;
                case NDbType.Float:
                    return MySqlDbType.Float;
                case NDbType.Image:
                    return MySqlDbType.Binary;
                case NDbType.Int:
                    return MySqlDbType.Int32;
                case NDbType.Money:
                    return MySqlDbType.Decimal;
                case NDbType.NText:
                    return MySqlDbType.LongText;
                case NDbType.NVarChar:
                    return MySqlDbType.VarChar;
                case NDbType.Real:
                    return MySqlDbType.Double;
                case NDbType.SmallInt:
                    return MySqlDbType.Int16;
                case NDbType.SmallMoney:
                    return MySqlDbType.Decimal;
                case NDbType.Text:
                    return MySqlDbType.Text;
                case NDbType.TinyInt:
                    return MySqlDbType.Int16;
                case NDbType.VarBinary:
                    return MySqlDbType.VarBinary;
                case NDbType.VarChar:
                    return MySqlDbType.VarChar;
            }
            return MySqlDbType.VarChar;
        }

        protected static DbType GetSqliteDbType(NDbType dbType)
        {
            switch (dbType)
            {
                case NDbType.BigInt:
                    return DbType.Int64;
                case NDbType.Binary:
                    return DbType.Binary;
                case NDbType.Bit:
                    return DbType.Byte;
                case NDbType.Char:
                    return DbType.String;
                case NDbType.Date:
                    return DbType.Date;
                case NDbType.DateTime:
                    return DbType.DateTime;
                case NDbType.DateTime2:
                    return DbType.DateTime2;
                case NDbType.Decimal:
                    return DbType.Decimal;
                case NDbType.Float:
                    return DbType.Decimal;
                case NDbType.Image:
                    return DbType.Binary;
                case NDbType.Int:
                    return DbType.Int16;
                case NDbType.Money:
                    return DbType.Currency;
                case NDbType.NChar:
                    return DbType.String;
                case NDbType.NText:
                    return DbType.String;
                case NDbType.NVarChar:
                    return DbType.String;
                case NDbType.SmallInt:
                    return DbType.Int16;
                case NDbType.SmallMoney:
                    return DbType.Currency;
                case NDbType.Text:
                    return DbType.String;
                case NDbType.TinyInt:
                    return DbType.Int16;
                case NDbType.VarBinary:
                    return DbType.Binary;
                case NDbType.VarChar:
                    return DbType.String;
            }
            return DbType.String;
        }

        private static NDbType _GetNDbType(string dbType)
        {
            switch (dbType.ToLower())
            {
                case "bit":
                    return NDbType.Bit;
                case "char":
                    return NDbType.Char;
                case "guid":
                case "keyguid":
                case "varchar":
                    return NDbType.VarChar;
                case "int":
                case "identity":
                case "keyidentity":
                    return NDbType.Int;
                case "bigint":
                    return NDbType.BigInt;
                case "datetime":
                    return NDbType.DateTime;
                case "decimal":
                    return NDbType.Decimal;
                case "text":
                    return NDbType.Text;
                case "ntext":
                    return NDbType.NText;
                case "float":
                    return NDbType.Float;
                case "image":
                    return NDbType.Image;
                case "money":
                    return NDbType.Money;
            }
            return NDbType.VarChar;
        }

        public static Type GetNetType(string dbType)
        {
            switch (dbType.ToLower())
            {
                case "bit":
                    return Type.GetType("System.Boolean", false, true);
                case "char":

                    return Type.GetType("System.Char", false, true);
                case "guid":
                    return Type.GetType("System.String", false, true);
                case "keyguid":
                case "varchar":
                    return Type.GetType("System.String", false, true);
                case "int":
                case "identity":
                case "keyidentity":
                    return Type.GetType("System.Int32", false, true);
                case "bigint":
                    return Type.GetType("System.Int64", false, true);
                case "datetime":
                    return Type.GetType("System.DateTime", false, true);
                case "decimal":
                    return Type.GetType("System.Decimal", false, true);
                case "text":
                    return Type.GetType("System.String", false, true);
                case "ntext":
                    return Type.GetType("System.String", false, true);
                case "float":
                    return Type.GetType("System.Single", false, true);
                case "image":
                    return Type.GetType("System.Byte[]", false, true);
                case "money":
                    return Type.GetType("System.Decimal", false, true);
            }
            return Type.GetType("System.String", false, true);
        }


        public static SqlDbType GetSqlDbType(string dbType)
        {
            NDbType type = DbUtility._GetNDbType(dbType);
            return DbUtility.GetSqlDbType(type);
        }
        public static OracleType GetOracleDbType(string dbType)
        {
            NDbType type = DbUtility._GetNDbType(dbType);
            return DbUtility.GetOracleDbType(type);
        }
        public static OracleDbType GetOraOracleDbType(string dbType)
        {
            NDbType type = DbUtility._GetNDbType(dbType);
            return DbUtility.GetOraOracleDbType(type);
        }

        public static OleDbType GetOleDbType(string dbType)
        {
            NDbType type = DbUtility._GetNDbType(dbType);
            return DbUtility.GetOleDbType(type);
        }

        public static MySqlDbType GetMysqlDbType(string dbType)
        {
            NDbType type = DbUtility._GetNDbType(dbType);
            return DbUtility.GetMysqlDbType(type);
        }


        public static DbType GetSqliteDbType(string dbType)
        {
            NDbType type = DbUtility._GetNDbType(dbType);
            return DbUtility.GetSqliteDbType(type);
        }

        #endregion

        #region
        public static object GetObjectNullHandler(string dbType, object value)
        {
            switch (dbType.ToLower())
            {
                case "bit":
                case "char":
                case "guid":
                case "keyguid":
                case "varchar":
                case "text":
                case "ntext":
                    return value;
                case "int":
                case "bigint":
                case "float":
                case "decimal":
                case "money":
                    if (value != null && value.ToString().Trim() != "")
                    {
                        return value;
                    }
                    return System.DBNull.Value;
                default:
                    return value;
            }
        }
        #endregion


    }
    #region 中间层系统类型库
    /// <summary>
    /// 数据访问层数据库字段类型外部访问接口
    /// 对于数据访问层中如需传递字段数据类型,必须先转换成如下数据类型进行传递
    /// </summary>
    public enum NDbType
    {
        BigInt = 0,
        Binary = 1,
        Bit = 2,
        Char = 3,
        Date = 4,
        DateTime = 5,
        DateTime2 = 6,
        Decimal = 7,
        Float = 8,
        Image = 9,
        Int = 10,
        Money = 11,
        NChar = 12,
        NText = 13,
        NVarChar = 14,
        Real = 15,
        SmallDateTime = 15,
        SmallInt = 17,
        SmallMoney = 18,
        Text = 19,
        TinyInt = 20,
        VarBinary = 21,
        VarChar = 22
    }
    #endregion
}
