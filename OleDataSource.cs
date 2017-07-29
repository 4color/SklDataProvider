using System;
using System.Data;
using System.Data.OleDb;
using System.Collections;
using System.Collections.Generic;

namespace SklDataProvider
{

    /// <summary>
    /// OleDataSource ��ժҪ˵����
    /// </summary>
    /// 
    public class OleDataSource : SklDataSource
    {
        private OleDbConnection _connection;
        private OleDbTransaction _transaction;

        public OleDataSource(string connectionString)
        {
            //
            // TODO: �ڴ˴���ӹ��캯���߼�


            this._connection = new OleDbConnection(connectionString);
            this._transaction = null;

        }

        #region Connection
        public override System.Data.IDbConnection Connection
        {
            get
            {
                return this._connection;
            }
        }

        public override void Open()
        {
            if (this._connection.State != ConnectionState.Open)
                this._connection.Open();
        }

        public override void Close()
        {
            if (this._connection.State == ConnectionState.Open)
                this._connection.Close();
        }
        #endregion

        #region Transaction
        public override void BeginTransaction()
        {
            this._transaction = this._connection.BeginTransaction();
        }

        public override void CommitTransaction()
        {
            this._transaction.Commit();
            //hyf 2006-3-27 add
            this._transaction = null;
            //
        }

        public override void RollBackTransaction()
        {
            this._transaction.Rollback();
            //hyf 2006-3-27 add
            this._transaction = null;
            //
        }

        #endregion

        #region CreateCommand
        public override IDbCommand CreateCommand(string commandText, CommandType commandType, IDbDataParameter[] commandParameters)
        {

            OleDbCommand command = new OleDbCommand();
            this.PrepareCommand(command, this._connection, this._transaction, commandType, commandText, commandParameters);

            return command;
        }

        #endregion

        #region CreateParameters
        public override IDbDataParameter[] CreateParameters(string[] paramNames, object[] paramValues)
        {
            if (paramNames == null)
                throw new ArgumentNullException("paramNames", "paramNames can't be null");

            int length = paramNames.Length;
            OleDbParameter[] parameters = new OleDbParameter[length];

            if (paramValues != null)
            {
                if (paramValues.Length != length)
                    throw new IndexOutOfRangeException("array paramNames' length must equels paramValues's length");

                for (int index = 0; index < length; index++)
                    parameters[index] = new OleDbParameter(paramNames[index], paramValues[index]);
            }
            else
            {
                for (int index = 0; index < length; index++)
                {
                    parameters[index] = new OleDbParameter();
                    parameters[index].ParameterName = paramNames[index];
                }
            }

            return parameters;
        }



        #endregion

        #region ExecuteNonQuery
        public override int ExecuteNonQuery(string commandText, CommandType cmdType, IDbDataParameter[] commandParameters)
        {
            OleDbCommand cmd = new OleDbCommand();
            this.PrepareCommand(cmd, this._connection, this._transaction, cmdType, commandText, commandParameters);

            //hyf 2006-3-27 delete
            //cmd.Parameters.Clear();

            return cmd.ExecuteNonQuery();
        }
        #endregion

        #region FillDataset

        public override void FillDataset(string selectText, DataSet dataSet, string[] tableNames, CommandType commandType, IDbDataParameter[] commandParameters)
        {
            OleDbCommand cmd = new OleDbCommand();
            this.PrepareCommand(cmd, this._connection, this._transaction, commandType, selectText, commandParameters);

            using (OleDbDataAdapter adapter = new OleDbDataAdapter(cmd))
            {
                // Add the table mappings specified by the user
                if (tableNames != null && tableNames.Length > 0)
                {
                    string tableName = "Table";
                    int tableNums = tableNames.Length;

                    for (int index = 0; index < tableNums; index++)
                    {
                        if (tableNames[index] == null || tableNames[index].Length == 0) throw new ArgumentException("The tableNames parameter must contain a list of tables, a value was provided as null or empty string.", "tableNames");
                        adapter.TableMappings.Add(tableName, tableNames[index]);
                        tableName += (index + 1).ToString();
                    }
                }

                adapter.Fill(dataSet);
                cmd.Parameters.Clear();
            }

        }

        public override int FillDataset(string selectText, DataSet dataset, int startRow, int maxRow, string tableName, CommandType commandType, IDbDataParameter[] commandParameters)
        {
            int result = 0;
            OleDbCommand cmd = new OleDbCommand();
            this.PrepareCommand(cmd, this._connection, this._transaction, commandType, selectText, commandParameters);
            using (OleDbDataAdapter adapter = new OleDbDataAdapter(cmd))
            {
                if (string.IsNullOrEmpty(tableName))
                {
                    tableName = "Table";
                }
                result = adapter.Fill(dataset, startRow, maxRow, tableName);
                cmd.Parameters.Clear();
            }
            return result;
        }
        #endregion

        #region UpdateDataset

        public override void UpdateDataSet(IDbCommand insertCommand, IDbCommand deleteCommand, IDbCommand updateCommand, DataSet dataSet, string tableName)
        {
            if (insertCommand == null) throw new ArgumentNullException("insertCommand");
            if (deleteCommand == null) throw new ArgumentNullException("deleteCommand");
            if (updateCommand == null) throw new ArgumentNullException("updateCommand");
            if (tableName == null || tableName.Length == 0) throw new ArgumentNullException("tableName");

            // Create a SqlDataAdapter, and dispose of it after we are done
            using (OleDbDataAdapter dataAdapter = new OleDbDataAdapter())
            {
                // Set the data adapter commands
                dataAdapter.UpdateCommand = (OleDbCommand)updateCommand;
                dataAdapter.InsertCommand = (OleDbCommand)insertCommand;
                dataAdapter.DeleteCommand = (OleDbCommand)deleteCommand;

                // Update the dataset changes in the data source
                dataAdapter.Update(dataSet, tableName);

                // Commit all the changes made to the DataSet
                dataSet.AcceptChanges();
            }
        }

        public override void UpdateDataSet(DataSet dataSet, string tableName, string sql)
        {


            using (OleDbDataAdapter dataAdapter = new OleDbDataAdapter(sql, this._connection))
            {

                OleDbCommandBuilder oleDbComandBuilder = new OleDbCommandBuilder(dataAdapter);


                dataAdapter.Update(dataSet, tableName);


            }


        }


        #endregion

        #region ExecuteReader
        public override IDataReader ExecuteReader(string selectCommandText, CommandType commandType, IDbDataParameter[] commandParameters)
        {
            OleDbCommand command = new OleDbCommand();
            this.PrepareCommand(command, this._connection, this._transaction, commandType, selectCommandText, commandParameters);

            return command.ExecuteReader();
        }
        #endregion

        #region ExecuteScalar

        public override object ExecuteScalar(string commandText, CommandType commandType, IDbDataParameter[] parameters)
        {
            OleDbCommand command = new OleDbCommand();
            this.PrepareCommand(command, this._connection, this._transaction, commandType, commandText, parameters);

            return command.ExecuteScalar();
        }

        #endregion

        public override void Dispose(bool disposing)
        {
            if (this._transaction != null)
            {
                //?????
                //this._transaction.Dispose();
                this._transaction = null;
            }

            base.Dispose(disposing);
        }

        public override DataTable GetSchema(string collectionName)
        {
            return this._connection.GetSchema(collectionName);
        }
        public override DataTable GetOleDbSchemaTable(Guid schema, object[] restrictions)
        {
            // TODO:  ��� OleDataSource.GetOleDbSchemaTable ʵ��
            return this._connection.GetOleDbSchemaTable(schema, restrictions);
        }


        /// <summary>
        /// ���ݱ����ƻ�ȡ���ֶζ�����Ϣ
        /// add by wusl 2009-2-13
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public override string[] GetTableFieldS(string tableName)
        {
            string sql = "SELECT   *  FROM " + tableName + " WHERE 1=0";
            DataSet ds = new DataSet();
            this.FillDataset(sql, ds);
            if (ds != null && ds.Tables.Count > 0)
            {
                DataTable dt = ds.Tables[0];
                string[] fieldS = new string[dt.Columns.Count];
                for (int u = 0; u < dt.Columns.Count; u++)
                {
                    fieldS[u] = dt.Columns[u].ColumnName;
                }
                return fieldS;
            }
            return new string[0];
        }

        public override void FillSchema(DataTable dataTable, SchemaType schemaType, string TableName)
        {

        }

        #region ���ݿ������������
        /// <summary>
        /// ����ִ��Sql��䣬���������
        /// </summary>
        /// <CreateDate>08-09-15</CreateDate>
        /// <CreateMan>xw</CreateMan>
        /// <param name="arraySql">sql����</param>
        /// <returns></returns>
        public override bool BatchExecuteNonQuery(ArrayList arraySql)
        {
            bool blnResult = true;
            try
            {
                OleDbCommand cmd = new OleDbCommand();
                cmd.Connection = this._connection;
                cmd.CommandType = CommandType.Text;
                this.Open();
                this.BeginTransaction();
                foreach (string strSql in arraySql)
                {
                    cmd.CommandText = strSql;
                    cmd.ExecuteNonQuery();
                }
                this.CommitTransaction();
                this.Close();
            }
            catch
            {
                this.RollBackTransaction();
                this.Close();
            }
            return blnResult;
        }

        /// <summary>
        /// ����ִ��Sql��䣬���������
        /// </summary>
        /// <CreateDate>08-09-15</CreateDate>
        /// <CreateMan>xw</CreateMan>
        /// <param name="arraySql">sql����</param>
        /// <returns></returns>
        public override bool BatchExecuteNonQuery(string[] arraySql)
        {
            bool blnResult = true;
            try
            {
                OleDbCommand cmd = new OleDbCommand();
                cmd.Connection = this._connection;
                cmd.CommandType = CommandType.Text;
                this.Open();
                this.BeginTransaction();
                foreach (string strSql in arraySql)
                {
                    cmd.CommandText = strSql;
                    cmd.ExecuteNonQuery();
                }
                this.CommitTransaction();
                this.Close();
            }
            catch
            {
                this.RollBackTransaction();
                this.Close();
            }
            return blnResult;
        }
        #endregion

        public override DBType getDBType()
        {
            return DBType.DB_OleDb;
        }

        public override ConnectionState State
        {
            get
            {
                return this._connection.State;
            }
        }

        public override string getDateSqlFormat(DateTime dt)
        {
            // TODO:  ��� OleDataSource.getDateSqlFormat ʵ��
            return "#" + dt.ToString() + "#";
        }

        public override string getParameterVarSqlFormat(string varName)
        {
            // TODO:  ��� OleDataSource.getParameterVarSqlFormat ʵ��
            //return "@"+varName;
            return "?";
        }

        public override string getParameterTag()
        {
            return "@";
        }


        public override IDbDataParameter[] CreateNTextParameter(string paramenterContent, string varName)
        {
            OleDbParameter[] p = new OleDbParameter[1];
            p[0] = new OleDbParameter();
            p[0].OleDbType = OleDbType.LongVarChar;
            p[0].Direction = ParameterDirection.Input;
            p[0].ParameterName = varName;
            p[0].Value = paramenterContent;
            return p;
        }
        public override IDbDataParameter[] CreateParameterS(string[] paramenterContentS, string[] varNameS, string[] dbTypeS)
        {
            if (paramenterContentS != null && varNameS != null)
            {
                OleDbParameter[] p = new OleDbParameter[paramenterContentS.Length];
                for (int i = 0; i < p.Length; i++)
                {
                    p[i] = new OleDbParameter();
                    p[i].OleDbType = DbUtility.GetOleDbType(dbTypeS[i]);
                    p[i].Direction = ParameterDirection.Input;
                    p[i].ParameterName = varNameS[i];
                    p[i].Value = paramenterContentS[i];
                }
                return p;
            }
            return null;
        }

        public override IDbDataParameter[] CreateBlobParameter(byte[] paramenterContent, string varName)
        {

            OleDbParameter[] p = new OleDbParameter[1];
            p[0] = new OleDbParameter();
            p[0].OleDbType = OleDbType.VarBinary;
            p[0].Direction = ParameterDirection.Input;
            p[0].ParameterName = varName;
            p[0].Value = paramenterContent;
            return p;

        }

        public override string getPriKeySqlString(string priKeyName)
        {
            // TODO:  ��� OleDataSource.getPriKeySqlString ʵ��
            return "select @@identity";
        }

        public override string subStrSqlString()
        {
            return "mid";
        }
        public override string getSubStrSql(string str, int start, int length)
        {
            string strtmp = "";
            strtmp = " MID(" + str + "," + Convert.ToString(start) + "," + Convert.ToString(length) + ")";
            return strtmp;
        }

        public override string getLenStrSql(string str)
        {
            string strtmp = "";
            strtmp = " LEN(" + str + ") ";
            return strtmp;
        }

        public override string getLenStr()
        {
            throw new NotImplementedException();
        }
        public override string getSysTimeStr()
        {
            return "Date()";
        }

        public override string getLikeStr()
        {
            return "*";
        }

        public override string getDateAddStr(string param, int addcount, string fieldName)
        {
            return "FORMAT(DATEADD('" + param + "'," + addcount + "," + fieldName + "),'yyyy-MM-dd')";
        }

        public override string getDateStr(string fieldName)
        {
            return "Format(" + fieldName + ",'yyyy-MM-dd')";
        }

        public override string getAddMonthsStr(string fieldName, int count)
        {
            return "DATEADD('m'," + count + "," + fieldName + ")";
        }

        public override string getYearPart(string fieldName)
        {
            return "Format(" + fieldName + ",'yyyy')";

        }

        public override string getMPart(string fieldName)
        {
            // TODO:  ��� OleDataSource.getMPart ʵ��
            return "Format(" + fieldName + ",'mm')";
        }

        public override string getJDPart(string fieldName)
        {
            // TODO:  ��� OleDataSource.getRPart ʵ��
            return "Format(" + fieldName + ",'q')";
        }

        public override string getYQPart(string fieldName)
        {
            throw new NotImplementedException();
        }

        public override string getYMPart(string fieldName)
        {
            throw new NotImplementedException();
        }

        public override string getConStrForJet()
        {
            string tmp = this._connection.ConnectionString;
            string[] con = tmp.Split(new char[] { ';' });
            tmp = con[0];
            con = tmp.Split(new char[] { '=' });
            tmp = con[1];
            return "[;database=" + tmp.Trim() + "]";
        }


        public override string TableType()
        {
            return "TABLE";
        }

        public override string ViewType()
        {
            return "VIEW";
        }
        public override string ProcedureType()
        {
            return "PROCEDURE_NAME";
        }

        public override string GetTopcmdText(string cmdText, int Top)
        {
            return "select top " + Top + " * from (" + cmdText + ")";
        }
    }
}
