using AshproStringExtension;
using Dapper;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ashpro.ORM.Dapper
{
    public class ORM
    {
        #region Public Method

        #region Async Method
        public static async Task<bool> BulkCopyAsync(string sTable, DataTable dt, bool isIdentity = true, string sCon = null)
        {
            var value = await Task<bool>.Factory.StartNew(() =>
            {
                sCon = sCon ?? DBConnection.Connection;
                try
                {
                    SqlBulkCopyOptions sqlBulk = isIdentity ? SqlBulkCopyOptions.KeepIdentity : SqlBulkCopyOptions.Default;
                    using (SqlBulkCopy bulkData = new SqlBulkCopy(sCon, sqlBulk))
                    {
                        bulkData.DestinationTableName = sTable;
                        bulkData.WriteToServer(dt);
                    }
                    return true;
                }
                catch (Exception ex)
                {
                    throw ex;
                }
            });
            return value;
        }
        public static async Task<dynamic> ValueFindAsync(string Query, string sCon = null)
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection con = new SqlConnection(sCon))
                {
                    using (SqlCommand cmd = new SqlCommand(Query, con))
                    {
                        await con.OpenAsync();
                        var result = await cmd.ExecuteScalarAsync();
                        if (result != System.DBNull.Value)
                        {
                            return result;
                        }
                        else
                        {
                            return null;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static async Task<DataTable> GetDataTableAsync(string Query, string sCon = null)
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                DataTable dt = new DataTable();
                using (SqlConnection con = new SqlConnection(sCon))
                {
                    using (SqlCommand cmd = new SqlCommand(Query, con))
                    {
                        await con.OpenAsync();
                        using (SqlDataAdapter sdr = new SqlDataAdapter(cmd))
                        {
                            sdr.Fill(dt);
                        }
                    }
                }
                return dt;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static async Task<T> GetAsync<T>(string Query, string sCon = null) where T : new()
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection conn = new SqlConnection(sCon))
                {
                    var data = await conn.QueryFirstOrDefaultAsync<T>(Query);
                    return data;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static async Task<List<T>> GetListAsync<T>(string Query, string sCon = null) where T : new()
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection conn = new SqlConnection(sCon))
                {
                    var data = await conn.QueryAsync<T>(Query);
                    return data.ToList();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static async Task<List<string>> GetStringListAsync(string Query, string sCon = null)
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection conn = new SqlConnection(sCon))
                {
                    var data = await conn.QueryAsync<string>(Query);
                    return data.ToList();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static async Task<bool> DatabaseMethodAsync(string Query, string sCon = null)
        {
            try
            {
                if (Query == string.Empty) { return false; }
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection con = new SqlConnection(sCon))
                {
                    var data = await con.ExecuteAsync(Query);
                }
                return true;
            }
            catch (Exception)
            {
                return false;
            }

        }
        public static async Task<bool> InsertAsync(List<object> datas, string table, string sCon = null)
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                foreach (object data in datas)
                {
                    await InsertAsync(data, table, sCon);
                }
                return true;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static async Task<bool> InsertAsync(object data, string table, string sCon = null)
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                var values = new List<string>();
                using (SqlConnection con = new SqlConnection(sCon))
                {
                    foreach (var item in data.GetType().GetProperties())
                    {
                        if (item.GetValue(data, null) != null)
                        {
                            if (item.PropertyType.Name == "Nullable`1" && item.GetValue(data, null).ToString() == "0")
                            {
                                continue;
                            }
                            values.Add(item.Name);
                        }
                    }
                    string Query = await getInsertCommandAsync(table, values);
                    var param = new DynamicParameters();
                    DapperLoad(data, param);
                    var result = await con.ExecuteAsync(Query, param);
                }
                return true;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static async Task<bool> UpdateAsync(object data, string table, string column, int iValue, string sCon = null)
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                var values = new List<string>();
                using (SqlConnection con = new SqlConnection(sCon))
                {
                    foreach (var item in data.GetType().GetProperties())
                    {
                        if (item.GetValue(data, null) != null && item.Name != column)
                        {
                            if (item.PropertyType.Name == "Nullable`1" && item.GetValue(data, null).ToString() == "0")
                            {
                                continue;
                            }
                            values.Add(item.Name);
                        }
                    }
                    string Query = await getUpdateCommandAsync(table, values, column, iValue);
                    var param = new DynamicParameters();
                    DapperLoad(data, param);
                    param.Add("@" + column, iValue);
                    var result = await con.ExecuteAsync(Query, param);
                }
                return true;
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }
        public static async Task<bool> UpdateAsync(List<object> datas, string table, string column, string sCon = null)
        {
            try
            {
                int iValue = -1;
                sCon = sCon ?? DBConnection.Connection;
                foreach (object data in datas)
                {
                    foreach (var item in data.GetType().GetProperties())
                    {
                        if (item.Name == column)
                        {
                            iValue = item.GetValue(data, null).ToInt32();
                            break;
                        }
                    }
                    await UpdateAsync(data, table, column, iValue, sCon);
                }
                return true;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static async Task<bool> DeleteAsync(string table, string column, int iValue, string sCon = null)
        {
            try
            {

                using (SqlConnection con = new SqlConnection(sCon))
                {
                    sCon = sCon ?? DBConnection.Connection;
                    string Query = "Delete From  " + table + " Where " + column + " = @" + column + "";
                    var param = new DynamicParameters();
                    param.Add("@" + column, iValue);
                    var result = await con.ExecuteAsync(Query, param);
                }
                return true;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static async Task<bool> DeleteAsync(string Query, string sCon = null) => await DatabaseMethodAsync(Query, sCon);
        public static async Task<bool> UpdateAsync(DataTable datas, DataTable oldDatas, string table, string sCon = null)
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                await DeleteOldAsync(datas, oldDatas, table, sCon);
                bool result = false;
                int sValue = 0;
                List<KeyValuePair<dynamic, dynamic>> values = new List<KeyValuePair<dynamic, dynamic>>();
                SqlConnection con = new SqlConnection(sCon);
                await con.OpenAsync();
                try
                {
                    foreach (DataRow data in datas.Rows)
                    {
                        string sColumn = string.Empty;
                        string Query = string.Empty;
                        List<int> iCommon = new List<int>();
                        values.Clear();
                        foreach (DataColumn item in data.Table.Columns)
                        {
                            if (item.ColumnName == data.Table.Columns[0].ColumnName)
                            {
                                sColumn = item.ColumnName;
                                sValue = data[item.ColumnName].ToInt32();
                            }
                            else
                            {
                                values.Add(new KeyValuePair<dynamic, dynamic>(item.ColumnName, data[item.ColumnName].ToString()));
                            }
                            iCommon = await GetCommonAsync(table, sColumn, sCon);
                        }
                        if (sValue > 0)
                        {
                            if (iCommon.Any(x => x == sValue))
                            {
                                Query = await getUpdateCommandAsyncTwo(table, values, sColumn, sValue);
                            }
                            else
                            {
                                Query = await getInsertCommandAsync(table, values);
                            }
                            using (SqlCommand cmd = new SqlCommand(Query, con))
                            {
                                await cmd.ExecuteNonQueryAsync();
                            }
                        }
                    }
                    result = true;
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                finally
                {
                    con.Close();
                }
                return result;
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }
        public static async Task<bool> UpdateAsync(List<object> newDatas, List<object> oldDatas, string sTable, string sColumn, string sCon = null)
        {
            sCon = sCon ?? DBConnection.Connection;
            var newList = new List<int>();
            var oldList = new List<int>();
            newList = await GetIdListAsync(newDatas, sColumn);
            oldList = await GetIdListAsync(oldDatas, sColumn);
            try
            {
                foreach (int item in oldList)
                {
                    if (!newList.Any(x => x == item))
                    {
                        await DeleteAsync(sTable, sColumn, item, sCon);
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            foreach (int item in newList)
            {
                if (!oldList.Any(x => x == item))
                {
                    try
                    {
                        foreach (var obj in newDatas)
                        {
                            foreach (var x in obj.GetType().GetProperties())
                            {
                                if (x.Name == sColumn)
                                {
                                    int? iVal = x.GetValue(obj, null).ToInt32();
                                    if (iVal == item)
                                    {
                                        await InsertAsync(obj, sTable, sCon);
                                        break;
                                    }
                                }
                            }
                        }
                        break;
                    }
                    catch (Exception)
                    {
                        throw;
                    }
                }
                else
                {
                    try
                    {
                        foreach (var obj in newDatas)
                        {
                            foreach (var x in obj.GetType().GetProperties())
                            {
                                if (x.Name == sColumn)
                                {
                                    int? iVal = x.GetValue(obj, null).ToInt32();
                                    if (iVal == item)
                                    {
                                        await UpdateAsync(obj, sTable, sColumn, item, sCon);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        throw ex;
                    }
                }
            }
            return true;
        }
        public static async Task<int> DatabaseMethodWithParameterAsync(object data, string Query, string sCon = null)
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (var con = new SqlConnection(sCon))
                {
                    var param = new DynamicParameters();
                    DapperLoad(data, param);
                    return await con.ExecuteAsync(Query, param);
                }
            }
            catch (Exception ex)
            {

                throw ex;
            }
        }
        #endregion

        #region Normal Method
        public static bool BulkCopy(string sTable, DataTable dt, bool isIdentity = true, string sCon = null)
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                SqlBulkCopyOptions sqlBulk = isIdentity ? SqlBulkCopyOptions.KeepIdentity : SqlBulkCopyOptions.Default;
                using (SqlBulkCopy bulkData = new SqlBulkCopy(sCon, sqlBulk))
                {
                    bulkData.DestinationTableName = sTable;
                    bulkData.WriteToServer(dt);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return true;
        }
        public static T GetObjectDetails<T>(string Query, string sCon = null) where T : new()
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection conn = new SqlConnection(sCon))
                {
                    return conn.QueryFirstOrDefault<T>(Query);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static DataTable GetDataTable(string Query, string sCon = null)
        {
            System.Threading.Thread.CurrentThread.CurrentCulture = System.Threading.Thread.CurrentThread.CurrentUICulture = new System.Globalization.CultureInfo("en");
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                DataTable dt = new DataTable();
                using (SqlConnection con = new SqlConnection(sCon))
                {
                    using (SqlDataAdapter da = new SqlDataAdapter(Query, con))
                    {
                        da.Fill(dt);
                    }
                }
                return dt;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static dynamic ValueFindMethod(string Query, string sCon = null)
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection con = new SqlConnection(sCon))
                {
                    using (SqlCommand cmd = new SqlCommand(Query, con))
                    {
                        con.Open();
                        return cmd.ExecuteScalar();
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static List<T> GetList<T>(string Query, string sCon = null) where T : new()
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection conn = new SqlConnection(sCon))
                {
                    return conn.Query<T>(Query).ToList();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static List<string> GetStringListMethod(string Query, string sCon = null)
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection conn = new SqlConnection(sCon))
                {
                    return conn.Query<string>(Query).ToList();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static bool DatabaseMethod(string Query, string sCon = null)
        {
            sCon = sCon ?? DBConnection.Connection;
            if (Query == string.Empty)
            {
                return false;
            }
            try
            {
                using (SqlConnection con = new SqlConnection(sCon))
                {
                    var result = con.Execute(Query);
                }
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }
        public static bool InsertToDatabase(List<object> datas, string table, string sCon = null)
        {
            try
            {
                foreach (object data in datas)
                {
                    InsertToDatabaseObj(data, table, sCon);
                }
                return true;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static bool InsertToDatabaseObj(object data, string table, string sCon = null)
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                List<KeyValuePair<string, string>> values = new List<KeyValuePair<string, string>>();
                using (SqlConnection con = new SqlConnection(sCon))
                {
                    foreach (var item in data.GetType().GetProperties())
                    {
                        if (item.GetValue(data, null) != null)
                        {
                            if (item.PropertyType.Name == "Nullable`1" && item.GetValue(data, null).ToString() == "0")
                            {
                                continue;
                            }
                            values.Add(new KeyValuePair<string, string>(item.Name, "@" + item.Name));
                        }
                    }
                    string Query = getInsertCommand(table, values);
                    var param = new DynamicParameters();
                    DapperLoad(data, param);
                    var result = con.Execute(Query, param);
                }
                return true;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static bool UpdateToDatabase(List<object> datas, string table, string column, string sCon = null)
        {
            try
            {
                int iValue = -1;
                foreach (object data in datas)
                {
                    foreach (var item in data.GetType().GetProperties())
                    {
                        if (item.Name == column)
                        {
                            iValue = item.GetValue(data, null).ToInt32();
                            break;
                        }
                    }
                    UpdateToDatabaseObj(data, table, column, iValue, sCon);
                }
                return true;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static bool UpdateToDatabaseObj(object data, string table, string column, int iValue, string sCon = null)
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                List<KeyValuePair<string, string>> values = new List<KeyValuePair<string, string>>();
                using (SqlConnection con = new SqlConnection(sCon))
                {
                    foreach (var item in data.GetType().GetProperties())
                    {
                        if (item.GetValue(data, null) != null && item.Name != column)
                        {
                            if (item.PropertyType.Name == "Nullable`1" && item.GetValue(data, null).ToString() == "0")
                            {
                                continue;
                            }
                            values.Add(new KeyValuePair<string, string>(item.Name, "@" + item.Name));
                        }
                    }
                    string Query = getUpdateCommand(table, values, column, "@" + column);
                    var param = new DynamicParameters();
                    DapperLoad(data, param);
                    param.Add("@" + column, iValue);
                    var result = con.Execute(Query, param);
                }
                return true;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static bool DeleteFromDatabase(string table, string column, int iValue, string sCon = null)
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                string Query = "Delete From  " + table + " Where " + column + " = @" + column + "";
                using (SqlConnection con = new SqlConnection(sCon))
                {
                    var param = new DynamicParameters();
                    param.Add("@" + column, iValue);
                    var reslt = con.Execute(Query, param);
                }
                return true;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static bool UpdateDatabase(DataTable datas, DataTable oldDatas, string table, string sCon = null)
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                DeleteOldItem(datas, oldDatas, table, sCon);
                bool result = false;
                int sValue = 0;
                List<KeyValuePair<dynamic, dynamic>> values = new List<KeyValuePair<dynamic, dynamic>>();
                SqlConnection con = new SqlConnection(sCon);
                con.Open();
                try
                {
                    foreach (DataRow data in datas.Rows)
                    {
                        string sColumn = string.Empty;
                        string Query = string.Empty;
                        List<int> iCommon = new List<int>();
                        values.Clear();
                        foreach (DataColumn item in data.Table.Columns)
                        {
                            if (item.ColumnName == data.Table.Columns[0].ColumnName)
                            {
                                sColumn = item.ColumnName;
                                sValue = data[item.ColumnName].ToInt32();
                            }
                            else
                            {
                                values.Add(new KeyValuePair<dynamic, dynamic>(item.ColumnName, data[item.ColumnName].ToString()));
                            }
                            iCommon = GetCommon(table, sColumn, sCon);
                        }
                        if (sValue > 0)
                        {
                            if (iCommon.Any(x => x == sValue))
                            {
                                Query = getUpdateCommand(table, values, sColumn, sValue);
                            }
                            else
                            {
                                Query = getInsertCommand(table, values);
                            }
                            using (SqlCommand cmd = new SqlCommand(Query, con))
                            {
                                cmd.ExecuteNonQuery();
                            }
                        }
                    }
                    result = true;
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                finally
                {
                    con.Close();
                }
                return result;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static bool UpdateDatabase(List<object> newDatas, List<object> oldDatas, string sTable, string sColumn, string sCon = null)
        {
            List<int> newList = new List<int>();
            List<int> oldList = new List<int>();
            newList = GetIdList(newDatas, sColumn);
            oldList = GetIdList(oldDatas, sColumn);
            try
            {
                foreach (int item in oldList)
                {
                    if (!newList.Any(x => x == item))
                    {
                        DeleteFromDatabase(sTable, sColumn, item, sCon);
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            foreach (int item in newList)
            {
                if (!oldList.Any(x => x == item))
                {
                    try
                    {
                        foreach (var obj in newDatas)
                        {
                            foreach (var x in obj.GetType().GetProperties())
                            {
                                if (x.Name == sColumn)
                                {
                                    int? iVal = x.GetValue(obj, null).ToInt32();
                                    if (iVal == item)
                                    {
                                        InsertToDatabaseObj(obj, sTable, sCon);
                                        break;
                                    }
                                }
                            }
                        }
                        break;
                    }
                    catch (Exception)
                    {
                        throw;
                    }
                }
                else
                {
                    try
                    {
                        foreach (var obj in newDatas)
                        {
                            foreach (var x in obj.GetType().GetProperties())
                            {
                                if (x.Name == sColumn)
                                {
                                    int? iVal = x.GetValue(obj, null).ToInt32();
                                    if (iVal == item)
                                    {
                                        UpdateToDatabaseObj(obj, sTable, sColumn, item, sCon);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        throw ex;
                    }
                }
            }
            return true;
        }
        public static int DatabaseMethodWithParameter(object data, string Query, string sCon = null)
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (var con = new SqlConnection(sCon))
                {
                    var param = new DynamicParameters();
                    DapperLoad(data, param);
                    return con.Execute(Query, param);
                }
            }
            catch (Exception ex)
            {

                throw ex;
            }
        }
        #endregion

        #endregion

        #region Private Method

        #region Async Method
        private static async Task DeleteOldAsync(DataTable newDt, DataTable oldDt, string sTable, string sCon = null)
        {
            try
            {
                string sColumn = string.Empty;
                sColumn = newDt.Rows[0].Table.Columns[0].ColumnName;
                List<int> newList = new List<int>();
                List<int> oldList = new List<int>();
                newList = await GetIdListAsync(newDt);
                oldList = await GetIdListAsync(oldDt);
                foreach (int item in oldList)
                {
                    if (!newList.Any(x => x == item))
                    {
                        await DeleteAsync(sTable, sColumn, item, sCon);
                    }
                }
            }
            catch (Exception)
            {
                throw;
            }
        }
        private static async Task<List<int>> GetCommonAsync(string sTable, string sColumn, string sCon = null)
        {
            try
            {
                List<int> iCommon = new List<int>();
                var dt = await GetDataTableAsync("Select " + sColumn + " From " + sTable, sCon);
                foreach (DataRow drw in dt.Rows)
                {
                    iCommon.Add(drw[0].ToInt32());
                }
                return iCommon;
            }
            catch (Exception)
            {
                throw;
            }
        }
        private static async Task<List<int>> GetIdListAsync(List<object> data, string sColumn)
        {
            var value = await Task.Run<List<int>>(() =>
            {
                try
                {
                    List<int> iCommon = new List<int>();
                    foreach (var obj in data)
                    {
                        foreach (var item in obj.GetType().GetProperties())
                        {
                            if (item.Name == sColumn)
                            {
                                iCommon.Add(item.GetValue(obj, null).ToInt32());
                                break;
                            }
                        }
                    }
                    return iCommon;
                }
                catch (Exception ex)
                {
                    throw ex;
                }
            });
            return value;
        }
        private static async Task<List<int>> GetIdListAsync(DataTable sTable)
        {
            var value = await Task.Run<List<int>>(() =>
            {
                try
                {
                    var iCommon = new List<int>();
                    foreach (DataRow drw in sTable.Rows)
                    {
                        iCommon.Add(drw[0].ToInt32());
                        continue;
                    }
                    return iCommon;
                }
                catch (Exception)
                {
                    throw;
                }
            });
            return value;
        }
        private static async Task<string> getInsertCommandAsync(string table, List<KeyValuePair<dynamic, dynamic>> values)
        {
            var value = await Task.Run<string>(() =>
            {
                try
                {
                    string query = null;
                    query += "INSERT INTO " + table + " ( ";
                    foreach (var item in values)
                    {
                        query += item.Key;
                        query += ", ";
                    }
                    query = query.Remove(query.Length - 2, 2);
                    query += ") VALUES ( ";
                    foreach (var item in values)
                    {
                        if (item.Key.GetType().Name == "System.Int") // or any other numerics
                        {
                            query += item.Value;
                        }
                        else
                        {
                            query += "'";
                            query += item.Value;
                            query += "'";
                        }
                        query += ", ";
                    }
                    query = query.Remove(query.Length - 2, 2);
                    query += ")";
                    return query;
                }
                catch (Exception ex)
                {
                    throw ex;
                }
            });
            return value;
        }
        private static async Task<string> getUpdateCommandAsync(string table, List<string> values, string column, int sValue)
        {
            var value = await Task.Run<string>(() =>
            {
                try
                {
                    string query = null;
                    var sList = values.Aggregate((s, v) => (s + "=@" + s) + "," + (v + "=@" + v));
                    query = $"Update {table} Set {sList} Where {column} = '{sValue}'";
                    return query;
                }
                catch (Exception ex)
                {
                    throw ex;
                }
            });
            return value;
        }
        private static async Task<string> getUpdateCommandAsyncTwo(string table, List<KeyValuePair<dynamic, dynamic>> values, string column, dynamic sValue)
        {
            var value = await Task.Run<string>(() =>
            {
                try
                {
                    string query = null;
                    query += "Update  " + table + " Set ";
                    foreach (var item in values)
                    {
                        query += item.Key;
                        query += "=";
                        query += item.Value;
                        query += ", ";
                    }
                    query = query.Remove(query.Length - 2, 2);
                    query += " Where " + column + " = " + sValue;
                    return query;
                }
                catch (Exception ex)
                {
                    throw ex;
                }
            });
            return value;
        }
        private static async Task<string> getInsertCommandAsync(string table, List<string> values)
        {
            var value = await Task.Run<string>(() =>
            {
                try
                {
                    string keystring = values.Aggregate((a, b) => a + ", " + b);
                    string valuestring = values.Aggregate((a, b) => a + ", " + "@" + b);
                    string query = null;
                    query = $"INSERT INTO {table} ({keystring}) VALUES (@{valuestring})";
                    return query;
                }
                catch (Exception ex)
                {
                    throw ex;
                }
            });
            return value;
        }
        #endregion

        #region Normal Method
        private static string GetDate(DateTime dateTime)
        {
            try
            {
                System.Globalization.CultureInfo enCul = new System.Globalization.CultureInfo("en-US");
                string sVal = dateTime.ToString("yyyy-MM-ddTHH:mm:ss", enCul);
                return sVal;
            }
            catch (Exception)
            {
                throw;
            }
        }
        private static void DeleteOldItem(DataTable newDt, DataTable oldDt, string sTable, string sCon = null)
        {
            try
            {
                string sColumn = string.Empty;
                foreach (DataRow item in newDt.Rows)
                {
                    sColumn = item.Table.Columns[0].ColumnName;
                }
                List<int> newList = new List<int>();
                List<int> oldList = new List<int>();
                newList = GetIdList(newDt);
                oldList = GetIdList(oldDt);
                foreach (int item in oldList)
                {
                    if (!newList.Any(x => x == item))
                    {
                        DeleteFromDatabase(sTable, sColumn, item, sCon);
                    }
                }
            }
            catch (Exception)
            {
                throw;
            }
        }
        private static List<int> GetCommon(string sTable, string sColumn, string sCon = null)
        {
            try
            {
                List<int> iCommon = new List<int>();
                var dt = GetDataTable("Select " + sColumn + " From " + sTable, sCon);
                foreach (DataRow drw in dt.Rows)
                {
                    iCommon.Add(drw[0].ToInt32());
                }
                return iCommon;
            }
            catch (Exception)
            {
                throw;
            }
        }
        private static List<int> GetIdList(List<object> data, string sColumn)
        {
            try
            {
                List<int> iCommon = new List<int>();
                foreach (var obj in data)
                {
                    foreach (var item in obj.GetType().GetProperties())
                    {
                        if (item.Name == sColumn)
                        {
                            iCommon.Add(item.GetValue(obj, null).ToInt32());
                            break;
                        }
                    }
                }
                return iCommon;
            }
            catch (Exception)
            {
                throw;
            }
        }
        private static List<int> GetIdList(DataTable sTable)
        {
            try
            {
                List<int> iCommon = new List<int>();
                foreach (DataRow drw in sTable.Rows)
                {
                    iCommon.Add(drw[0].ToInt32());
                    continue;
                }
                return iCommon;
            }
            catch (Exception)
            {
                throw;
            }
        }
        private static string getInsertCommand(string table, List<KeyValuePair<dynamic, dynamic>> values)
        {
            string query = null;
            query += "INSERT INTO " + table + " ( ";
            foreach (var item in values)
            {
                query += item.Key;
                query += ", ";
            }
            query = query.Remove(query.Length - 2, 2);
            query += ") VALUES ( ";
            foreach (var item in values)
            {
                if (item.Key.GetType().Name == "System.Int") // or any other numerics
                {
                    query += item.Value;
                }
                else
                {
                    query += "'";
                    query += item.Value;
                    query += "'";
                }
                query += ", ";
            }
            query = query.Remove(query.Length - 2, 2);
            query += ")";
            return query;
        }
        private static string getUpdateCommand(string table, List<KeyValuePair<dynamic, dynamic>> values, string column, int sValue)
        {
            string query = null;
            query += "Update  " + table + " Set ";
            foreach (var item in values)
            {
                query += item.Key;
                query += "=";
                if (item.Key.GetType().Name == "System.Int") // or any other numerics
                {
                    query += item.Value;
                }
                else
                {
                    query += "'";
                    query += item.Value;
                    query += "'";
                }
                query += ", ";
            }
            query = query.Remove(query.Length - 2, 2);
            query += " Where " + column + " = '" + sValue + "'";
            return query;
        }
        private static string getUpdateCommand(string table, List<KeyValuePair<string, string>> values, string column, dynamic sValue)
        {
            string query = null;
            query += "Update  " + table + " Set ";
            foreach (var item in values)
            {
                query += item.Key;
                query += "=";
                query += item.Value;
                query += ", ";
            }
            query = query.Remove(query.Length - 2, 2);
            query += " Where " + column + " = " + sValue;
            return query;
        }
        private static string getInsertCommand(string table, List<KeyValuePair<string, string>> values)
        {
            string query = null;
            query += "INSERT INTO " + table + " ( ";
            foreach (var item in values)
            {
                query += item.Key;
                query += ", ";
            }
            query = query.Remove(query.Length - 2, 2);
            query += ") VALUES ( ";
            foreach (var item in values)
            {

                query += item.Value;
                query += ", ";
            }
            query = query.Remove(query.Length - 2, 2);
            query += ")";
            return query;
        }
        public static void EntityLoadMethod(object entity, SqlCommand cmd)
        {
            foreach (var item in entity.GetType().GetProperties())
            {
                if (item.GetValue(entity, null) != null)
                {
                    if (item.PropertyType.Name == "Nullable`1" && item.GetValue(entity, null).ToString() == "0")
                    {
                        continue;
                    }
                    switch (item.PropertyType.Name)
                    {
                        case "Byte[]":
                            cmd.Parameters.AddWithValue(item.Name, (byte[])(item.GetValue(entity, null)));
                            break;
                        case "DateTime":
                            var val = GetDate((DateTime)(item.GetValue(entity, null)));
                            cmd.Parameters.AddWithValue("@" + item.Name, val);
                            break;
                        case "Nullable`1":
                            if (item.PropertyType.FullName.Contains("System.DateTime"))
                            {
                                val = GetDate((DateTime)(item.GetValue(entity, null)));
                                cmd.Parameters.AddWithValue("@" + item.Name, val);
                            }
                            else
                            {
                                cmd.Parameters.AddWithValue(item.Name, item.GetValue(entity, null).ToString());
                            }
                            break;
                        default:
                            cmd.Parameters.AddWithValue(item.Name, item.GetValue(entity, null).ToString());
                            break;
                    }
                }
            }
        }
        public static void DapperLoad(object entity, DynamicParameters param)
        {
            foreach (var item in entity.GetType().GetProperties())
            {
                if (item.GetValue(entity, null) != null)
                {
                    if (item.PropertyType.Name == "Nullable`1" && item.GetValue(entity, null).ToString() == "0")
                    {
                        continue;
                    }
                    switch (item.PropertyType.Name)
                    {
                        case "Byte[]":
                            param.Add(item.Name, (byte[])(item.GetValue(entity, null)));
                            break;
                        case "DateTime":
                            var val = GetDate((DateTime)(item.GetValue(entity, null)));
                            param.Add("@" + item.Name, val);
                            break;
                        case "Nullable`1":
                            if (item.PropertyType.FullName.Contains("System.DateTime"))
                            {
                                val = GetDate((DateTime)(item.GetValue(entity, null)));
                                param.Add("@" + item.Name, val);
                            }
                            else
                            {
                                param.Add(item.Name, item.GetValue(entity, null).ToString());
                            }
                            break;
                        default:
                            param.Add(item.Name, item.GetValue(entity, null).ToString());
                            break;
                    }
                }
            }
        }
        #endregion

        #endregion

        #region Public Method BySP

        #region Normal
        public static bool InsertMethod_SP(List<object> entities, string sStoredProceedure, string sCon = null)
        {
            bool result = false;
            try
            {
                foreach (object data in entities)
                {
                    InsertMethod_SP(data, sStoredProceedure, sCon);
                }
                result = true;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return result;
        }
        public static bool InsertMethod_SP(object entity, string sStoredProceedure, string sCon = null)
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection con = new SqlConnection(sCon))
                {
                    var param = new DynamicParameters();
                    DapperLoad(entity, param);
                    var numRes = con.Execute(sStoredProceedure, param, commandType: CommandType.StoredProcedure);
                    return numRes > 0 ? true : false;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static DBOutput InsertMethod_SP(object entity, DataSet ds, string sStoredProceedure, object Secondentity = null, string sCon = null)
        {
            DBOutput dBOutput = new DBOutput();
            sCon = sCon ?? DBConnection.Connection;
            using (SqlConnection con = new SqlConnection(sCon))
            {
                using (SqlCommand cmd = new SqlCommand(sStoredProceedure, con))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    try
                    {
                        if (ds != null)
                        {
                            foreach (DataTable dt in ds.Tables)
                            {
                                cmd.Parameters.AddWithValue("@" + dt.TableName, dt);
                            }
                        }
                        if (entity != null)
                        {
                            EntityLoadMethod(entity, cmd);
                        }
                        if (Secondentity != null)
                        {
                            EntityLoadMethod(Secondentity, cmd);
                        }
                        cmd.Parameters.Add("@return", SqlDbType.Int);
                        cmd.Parameters.Add("@errMessage", SqlDbType.Char, 500);
                        cmd.Parameters["@return"].Direction = ParameterDirection.Output;
                        cmd.Parameters["@errMessage"].Direction = ParameterDirection.Output;
                        con.Open();
                        cmd.CommandTimeout = 0;
                        cmd.ExecuteNonQuery();
                        dBOutput.Message = cmd.Parameters["@errMessage"].Value.ToString2();
                        dBOutput.Value = cmd.Parameters["@return"].Value.ToIntiger();
                    }
                    catch (Exception ex)
                    {
                        dBOutput.Message = ex.Message;
                        dBOutput.Value = null;
                    }
                }
            }
            return dBOutput;
        }
        public static bool UpdateMethod_SP(object entity, string sStoredProceedure, string sCon = null)
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection con = new SqlConnection(sCon))
                {
                    var param = new DynamicParameters();
                    DapperLoad(entity, param);
                    var rslt = con.Execute(sStoredProceedure, param, commandType: CommandType.StoredProcedure);
                    return rslt > 0 ? true : false;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static bool UpdateMethod_SP(List<object> entities, string sStoredProceedure, string sCon = null)
        {
            try
            {
                foreach (object data in entities)
                {
                    UpdateMethod_SP(data, sStoredProceedure, sCon);
                }
                return true;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static bool UpdateMethod_SP(List<object> newDatas, List<object> oldDatas, string sTable, string sColumn, string sCon = null)
        {
            var newList = GetIdList(newDatas, sColumn);
            var oldList = GetIdList(oldDatas, sColumn);
            try
            {
                if (oldList.Any())
                {
                    foreach (int item in oldList)
                    {
                        if (!newList.Any(x => x == item))
                        {
                            DeleteMethod_SP(item, "spDelete" + sTable, sCon);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            foreach (int item in newList)
            {
                if (!oldList.Any(x => x == item))
                {
                    try
                    {
                        foreach (var obj in newDatas)
                        {
                            foreach (var x in obj.GetType().GetProperties())
                            {
                                if (x.Name == sColumn)
                                {
                                    int? iVal = x.GetValue(obj, null).ToInt32();
                                    if (iVal == item)
                                    {
                                        InsertMethod_SP(obj, "spInsert" + sTable, sCon);
                                        break;
                                    }
                                }
                            }

                        }
                        break;
                    }
                    catch (Exception ex)
                    {
                        throw ex;
                    }
                }
                else
                {
                    try
                    {
                        foreach (var obj in newDatas)
                        {
                            foreach (var x in obj.GetType().GetProperties())
                            {
                                if (x.Name == sColumn)
                                {
                                    int? iVal = x.GetValue(obj, null).ToInt32();
                                    if (iVal == item)
                                    {
                                        UpdateMethod_SP(obj, "spUpdate" + sTable, sCon);
                                    }
                                }
                                break;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        throw ex;
                    }
                }
            }
            return true;
        }
        public static bool DeleteMethod_SP(object entity, string sStoredProceedure, string sCon = null)
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection con = new SqlConnection(sCon))
                {
                    var param = new DynamicParameters();
                    param.Add("@id", entity.ToInt32());
                    var rslt = con.Execute(sStoredProceedure, param, commandType: CommandType.StoredProcedure);
                    return rslt > 0 ? true : false;
                }
            }
            catch (Exception ex)
            {

                throw ex;
            }
        }
        public static DataTable GetDataTable_SP(string sStoredProceedure, string sCon = null)
        {
            System.Threading.Thread.CurrentThread.CurrentCulture = System.Threading.Thread.CurrentThread.CurrentUICulture = new System.Globalization.CultureInfo("en");
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                DataTable dt = new DataTable();
                using (SqlConnection con = new SqlConnection(sCon))
                {
                    using (SqlCommand cmd = new SqlCommand(sStoredProceedure, con))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        using (SqlDataAdapter da = new SqlDataAdapter(cmd))
                        {
                            da.Fill(dt);
                        }
                    }
                }
                return dt;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static DataTable GetDataTableWithIdParameter_SP(string sStoredProceedure, string Value, string sCon = null)
        {
            System.Threading.Thread.CurrentThread.CurrentCulture = System.Threading.Thread.CurrentThread.CurrentUICulture = new System.Globalization.CultureInfo("en");
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                DataTable dt = new DataTable();
                using (SqlConnection con = new SqlConnection(sCon))
                {
                    using (SqlCommand cmd = new SqlCommand(sStoredProceedure, con))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.Parameters.AddWithValue("@Id", Value);
                        using (SqlDataAdapter da = new SqlDataAdapter(cmd))
                        {
                            da.Fill(dt);
                        }
                    }
                }
                return dt;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static DataTable GetDataTableWithIdParameter_SP(string sStoredProceedure, object entity, string sCon = null)
        {
            System.Threading.Thread.CurrentThread.CurrentCulture = System.Threading.Thread.CurrentThread.CurrentUICulture = new System.Globalization.CultureInfo("en");
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                DataTable dt = new DataTable();
                using (SqlConnection con = new SqlConnection(sCon))
                {
                    using (SqlCommand cmd = new SqlCommand(sStoredProceedure, con))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        if (entity != null)
                        {
                            foreach (var item in entity.GetType().GetProperties())
                            {
                                if (item.GetValue(entity, null) != null)
                                {
                                    cmd.Parameters.AddWithValue(item.Name, item.GetValue(entity, null).ToString());
                                }
                            }
                        }
                        cmd.CommandTimeout = 0;
                        using (SqlDataAdapter da = new SqlDataAdapter(cmd))
                        {
                            da.Fill(dt);
                        }
                    }
                }
                return dt;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static List<T> GetList_SP<T>(string sStoredProceedure, string sCon = null) where T : new()
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection conn = new SqlConnection(sCon))
                {
                    return conn.Query<T>(sStoredProceedure).ToList();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static List<string> GetList_SP(string commandText, object entity, string sCon = null)
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection conn = new SqlConnection(sCon))
                {
                    var param = new DynamicParameters();
                    if (entity != null)
                    {
                        foreach (var item in entity.GetType().GetProperties())
                        {
                            if (item.GetValue(entity, null) != null)
                            {
                                param.Add(item.Name, item.GetValue(entity, null).ToString());
                            }
                        }
                    }
                    return conn.Query<string>(commandText, param, commandType: CommandType.StoredProcedure).ToList();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static List<T> GetList_SP<T>(string sStoredProceedure, string Value, string sCon = null) where T : new()
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection conn = new SqlConnection(sCon))
                {
                    var param = new DynamicParameters();
                    if (Value != null) { param.Add(Value); }
                    return conn.Query<T>(sStoredProceedure, param, commandType: CommandType.StoredProcedure).ToList();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static List<T> GetList_SP<T>(string commandText, object entity, string sCon = null) where T : new()
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection conn = new SqlConnection(sCon))
                {
                    var param = new DynamicParameters();
                    if (entity != null)
                    {
                        foreach (var item in entity.GetType().GetProperties())
                        {
                            if (item.GetValue(entity, null) != null)
                            {
                                param.Add(item.Name, item.GetValue(entity, null).ToString());
                            }
                        }
                    }
                    return conn.Query<T>(commandText, param, commandType: CommandType.StoredProcedure).ToList();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static T GetObject_SP<T>(string sStoredProceedure, string Value, string sCon = null) where T : new()
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection conn = new SqlConnection(sCon))
                {
                    var param = new DynamicParameters();
                    if (Value != null) { param.Add(Value); }
                    return conn.QueryFirstOrDefault<T>(sStoredProceedure, param, commandType: CommandType.StoredProcedure);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static T GetObjectWithparameter_SP<T>(string sStoredProceedure, object entity, string sCon = null) where T : new()
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection conn = new SqlConnection(sCon))
                {
                    var param = new DynamicParameters();
                    if (entity != null)
                    {
                        foreach (var item in entity.GetType().GetProperties())
                        {
                            if (item.GetValue(entity, null) != null)
                            {
                                param.Add(item.Name, item.GetValue(entity, null).ToString());
                            }
                        }
                    }
                    return conn.QueryFirstOrDefault<T>(sStoredProceedure, param, commandType: CommandType.StoredProcedure);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static T GetObject_SP<T>(string sStoredProceedure, string sCon = null) where T : new()
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection conn = new SqlConnection(sCon))
                {
                    return conn.QueryFirstOrDefault<T>(sStoredProceedure, commandType: CommandType.StoredProcedure);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static dynamic GetData_SP(string sStoredProceedure, string sCon = null)
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection con = new SqlConnection(sCon))
                {
                    using (SqlCommand cmd = new SqlCommand(sStoredProceedure, con))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        con.Open();
                        var data = cmd.ExecuteScalar();
                        if (data != DBNull.Value)
                        {
                            return data;
                        }
                        return null;
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static dynamic GetDataWithParameter_SP(string sStoredProceedure, object entity, string sCon = null)
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection con = new SqlConnection(sCon))
                {
                    using (SqlCommand cmd = new SqlCommand(sStoredProceedure, con))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        if (entity != null)
                        {
                            foreach (var item in entity.GetType().GetProperties())
                            {
                                if (item.GetValue(entity, null) != null)
                                {
                                    cmd.Parameters.AddWithValue(item.Name, item.GetValue(entity, null).ToString());
                                }
                            }
                        }
                        con.Open();
                        var data = cmd.ExecuteScalar();
                        if (data != DBNull.Value)
                        {
                            return data;
                        }
                        return null;
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static bool DatabaseExecution_SP(string sStoredProceedure, object entity = null, string sCon = null)
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection con = new SqlConnection(sCon))
                {
                    var param = new DynamicParameters();
                    if (entity != null)
                    {
                        foreach (var item in entity.GetType().GetProperties())
                        {
                            if (item.GetValue(entity, null) != null)
                            {
                                param.Add(item.Name, item.GetValue(entity, null).ToString());
                            }
                        }
                    }
                    var rslt = con.Execute(sStoredProceedure, param, commandType: CommandType.StoredProcedure);
                    return rslt > 0 ? true : false;
                }
            }
            catch (Exception ex)
            {

                throw ex;
            }
        }
        public static DataSet GetDataSet_SP(string sStoredProceedure, object entity, string sCon = null)
        {
            System.Threading.Thread.CurrentThread.CurrentCulture = System.Threading.Thread.CurrentThread.CurrentUICulture = new System.Globalization.CultureInfo("en");
            DataSet ds = new DataSet();
            sCon = sCon ?? DBConnection.Connection;
            try
            {
                using (SqlConnection con = new SqlConnection(sCon))
                {
                    using (SqlCommand cmd = new SqlCommand(sStoredProceedure, con))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        if (entity != null)
                        {
                            foreach (var item in entity.GetType().GetProperties())
                            {
                                if (item.GetValue(entity, null) != null)
                                {
                                    cmd.Parameters.AddWithValue(item.Name, item.GetValue(entity, null).ToString());
                                }
                            }
                        }
                        cmd.CommandTimeout = 0;
                        using (SqlDataAdapter da = new SqlDataAdapter(cmd))
                        {
                            da.Fill(ds);
                        }
                    }
                }
                return ds;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region Async
        public static async Task<bool> InsertAsync_SP(List<object> entities, string sStoredProceedure, string sCon = null)
        {
            try
            {
                foreach (object data in entities)
                {
                    await InsertAsync_SP(data, sStoredProceedure, sCon);
                }
                return true;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static async Task<bool> InsertAsync_SP(object entity, string sStoredProceedure, string sCon = null)
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection con = new SqlConnection(sCon))
                {
                    var param = new DynamicParameters();
                    DapperLoad(entity, param);
                    var rslt = await con.ExecuteAsync(sStoredProceedure, param, commandType: CommandType.StoredProcedure);
                    return rslt > 0 ? true : false;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static async Task<DBOutput> InsertAsync_SP(object entity, DataSet ds, string sStoredProceedure, object Secondentity = null, bool isSimple = false, string sCon = null)
        {
            DBOutput dBOutput = new DBOutput();
            sCon = sCon ?? DBConnection.Connection;
            try
            {
                using (SqlConnection con = new SqlConnection(sCon))
                {
                    using (SqlCommand cmd = new SqlCommand(sStoredProceedure, con))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        try
                        {
                            if (ds != null)
                            {
                                foreach (DataTable dt in ds.Tables)
                                {
                                    cmd.Parameters.AddWithValue("@" + dt.TableName, dt);
                                }
                            }
                            if (entity != null)
                            {
                                EntityLoadMethod(entity, cmd);
                            }
                            if (Secondentity != null)
                            {
                                EntityLoadMethod(Secondentity, cmd);
                            }
                            if (!isSimple)
                            {
                                cmd.Parameters.Add("@return", SqlDbType.Int);
                                cmd.Parameters.Add("@errMessage", SqlDbType.Char, 500);
                                cmd.Parameters["@return"].Direction = ParameterDirection.Output;
                                cmd.Parameters["@errMessage"].Direction = ParameterDirection.Output;
                            }
                            await con.OpenAsync();
                            cmd.CommandTimeout = 0;
                            await cmd.ExecuteNonQueryAsync();
                            if (!isSimple)
                            {
                                dBOutput.Message = cmd.Parameters["@errMessage"].Value.ToString2();
                                dBOutput.Value = cmd.Parameters["@return"].Value.ToIntiger();
                            }
                            else
                            {
                                dBOutput.Value = 1;
                                dBOutput.Message = string.Empty;
                            }
                        }
                        catch (Exception ex)
                        {
                            dBOutput.Message = ex.Message;
                            dBOutput.Value = null;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                dBOutput.Message = ex.Message;
                dBOutput.Value = null;
            }
            return dBOutput;
        }
        public static async Task<bool> UpdateAsync_SP(object entity, string sStoredProceedure, string sCon = null)
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection con = new SqlConnection(sCon))
                {
                    var param = new DynamicParameters();
                    DapperLoad(entity, param);
                    var rslt = await con.ExecuteAsync(sStoredProceedure, param, commandType: CommandType.StoredProcedure);
                    return rslt > 0 ? true : false;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static async Task<bool> UpdateAsync_SP(List<object> entities, string sStoredProceedure, string sCon = null)
        {
            try
            {
                foreach (object data in entities)
                {
                    await UpdateAsync_SP(data, sStoredProceedure, sCon);
                }
                return true;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static async Task<bool> UpdateAsync_SP(List<object> newDatas, List<object> oldDatas, string sTable, string sColumn, string sCon = null)
        {
            var newList = await GetIdListAsync(newDatas, sColumn);
            var oldList = await GetIdListAsync(oldDatas, sColumn);
            try
            {
                if (oldList.Count > 0)
                {
                    foreach (int item in oldList)
                    {
                        if (!newList.Any(x => x == item))
                        {
                            await DeleteAsync_SP(item, "spDelete" + sTable, sCon);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            foreach (int item in newList)
            {
                if (!oldList.Any(x => x == item))
                {
                    try
                    {
                        foreach (var obj in newDatas)
                        {
                            foreach (var x in obj.GetType().GetProperties())
                            {
                                if (x.Name == sColumn)
                                {
                                    int? iVal = x.GetValue(obj, null).ToInt32();
                                    if (iVal == item)
                                    {
                                        await InsertAsync_SP(obj, "spInsert" + sTable, sCon);
                                        break;
                                    }
                                }
                            }

                        }
                        break;
                    }
                    catch (Exception ex)
                    {
                        throw ex;
                    }
                }
                else
                {
                    try
                    {
                        foreach (var obj in newDatas)
                        {
                            foreach (var x in obj.GetType().GetProperties())
                            {
                                if (x.Name == sColumn)
                                {
                                    int? iVal = x.GetValue(obj, null).ToInt32();
                                    if (iVal == item)
                                    {
                                        await UpdateAsync_SP(obj, "spUpdate" + sTable, sCon);
                                    }
                                }
                                break;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        throw ex;
                    }
                }
            }
            return true;
        }
        public static async Task<bool> DeleteAsync_SP(object entity, string sStoredProceedure, string sCon = null)
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection con = new SqlConnection(sCon))
                {
                    var param = new DynamicParameters();
                    param.Add(@"Id", entity.ToInt32());
                    var rslt = await con.ExecuteAsync(sStoredProceedure, param, commandType: CommandType.StoredProcedure);
                    return rslt > 0 ? true : false;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }
        public static async Task<DataTable> GetDataTableAsync_SP(string sStoredProceedure, string sCon = null)
        {
            var value = await Task<DataTable>.Factory.StartNew(() =>
            {
                try
                {
                    sCon = sCon ?? DBConnection.Connection;
                    System.Threading.Thread.CurrentThread.CurrentCulture = System.Threading.Thread.CurrentThread.CurrentUICulture = new System.Globalization.CultureInfo("en");
                    DataTable dt = new DataTable();
                    using (SqlConnection con = new SqlConnection(sCon))
                    {
                        using (SqlCommand cmd = new SqlCommand())
                        {
                            cmd.Connection = con;
                            cmd.CommandType = CommandType.StoredProcedure;
                            cmd.CommandText = sStoredProceedure;
                            using (SqlDataAdapter sdr = new SqlDataAdapter(cmd))
                            {
                                sdr.Fill(dt);
                            }
                        }
                    }
                    return dt;
                }
                catch (Exception ex)
                {
                    throw ex;
                }
            });
            return value;
        }
        public static async Task<DataTable> GetDataTableWithIdParameterAsync_SP(string sStoredProceedure, string Value, string sCon = null)
        {
            var value = await Task<DataTable>.Factory.StartNew(() =>
            {
                try
                {
                    System.Threading.Thread.CurrentThread.CurrentCulture = System.Threading.Thread.CurrentThread.CurrentUICulture = new System.Globalization.CultureInfo("en");
                    DataTable dt = new DataTable();
                    sCon = sCon ?? DBConnection.Connection;
                    using (SqlConnection con = new SqlConnection(sCon))
                    {
                        using (SqlCommand cmd = new SqlCommand(sStoredProceedure, con))
                        {
                            cmd.CommandType = CommandType.StoredProcedure;
                            cmd.Parameters.AddWithValue("@Id", Value);
                            using (SqlDataAdapter da = new SqlDataAdapter(cmd))
                            {
                                da.Fill(dt);
                            }
                        }
                    }
                    return dt;
                }
                catch (Exception ex)
                {
                    throw ex;
                }
            });
            return value;
        }
        public static async Task<DataTable> GetDataTableWithIdParameterAsync_SP(string sStoredProceedure, object entity, string sCon = null)
        {
            var value = await Task<DataTable>.Factory.StartNew(() =>
            {
                try
                {
                    System.Threading.Thread.CurrentThread.CurrentCulture = System.Threading.Thread.CurrentThread.CurrentUICulture = new System.Globalization.CultureInfo("en");
                    DataTable dt = new DataTable();
                    sCon = sCon ?? DBConnection.Connection;
                    using (SqlConnection con = new SqlConnection(sCon))
                    {
                        using (SqlCommand cmd = new SqlCommand())
                        {
                            cmd.Connection = con;
                            if (entity != null)
                            {
                                foreach (var item in entity.GetType().GetProperties())
                                {
                                    if (item.GetValue(entity, null) != null)
                                    {
                                        cmd.Parameters.AddWithValue(item.Name, item.GetValue(entity, null).ToString());
                                    }
                                }
                            }
                            cmd.CommandText = sStoredProceedure;
                            cmd.CommandTimeout = 0;
                            cmd.CommandType = CommandType.StoredProcedure;
                            using (SqlDataAdapter sdr = new SqlDataAdapter(cmd))
                            {
                                sdr.Fill(dt);
                            }
                        }
                    }
                    return dt;
                }
                catch (Exception ex)
                {
                    throw ex;
                }
            });
            return value;
        }
        public static async Task<List<T>> GetListAsync_SP<T>(string commandText, string sCon = null) where T : new()
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection conn = new SqlConnection(sCon))
                {
                    var rslt = await conn.QueryAsync<T>(commandText);
                    return rslt.ToList();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static async Task<List<string>> GetStringListAsync_SP(string sStoredProceedure, string sCon = null)
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection conn = new SqlConnection(sCon))
                {
                    var rslt = await conn.QueryAsync<string>(sStoredProceedure, commandType: CommandType.StoredProcedure);
                    return rslt.ToList();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static async Task<List<string>> GetStringListAsync_SP(string sStoredProceedure, object entity, string sCon = null)
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection con = new SqlConnection(sCon))
                {

                    var param = new DynamicParameters();
                    if (entity != null)
                    {
                        foreach (var item in entity.GetType().GetProperties())
                        {
                            if (item.GetValue(entity, null) != null)
                            {
                                param.Add(item.Name, item.GetValue(entity, null).ToString());
                            }
                        }
                    }
                    var list = await con.QueryAsync<string>(sStoredProceedure, param, commandType: CommandType.StoredProcedure);
                    return list.ToList();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static async Task<List<T>> GetListAsync_SP<T>(string sStoredProceedure, string Value, string sCon = null) where T : new()
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection conn = new SqlConnection(sCon))
                {
                    var param = new DynamicParameters();
                    param.Add("@Id", Value);
                    var rslt = await conn.QueryAsync<T>(sStoredProceedure, param, commandType: CommandType.StoredProcedure);
                    return rslt.ToList();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static async Task<List<T>> GetListAsync_SP<T>(string commandText, object entity, string sCon = null) where T : new()
        {
            try
            {
                System.Threading.Thread.CurrentThread.CurrentCulture = System.Threading.Thread.CurrentThread.CurrentUICulture = new System.Globalization.CultureInfo("en");
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection conn = new SqlConnection(sCon))
                {
                    var param = new DynamicParameters();
                    if (entity != null)
                    {
                        foreach (var item in entity.GetType().GetProperties())
                        {
                            if (item.GetValue(entity, null) != null)
                            {
                                param.Add(item.Name, item.GetValue(entity, null).ToString());
                            }
                        }
                    }
                    var rslt = await conn.QueryAsync<T>(commandText, param, commandType: CommandType.StoredProcedure);
                    return rslt.ToList();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static async Task<T> GetAsync_SP<T>(string sStoredProceedure, string Value, string sCon = null) where T : new()
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection conn = new SqlConnection(sCon))
                {
                    var param = new DynamicParameters();
                    param.Add("@Id", Value);
                    return await conn.QueryFirstOrDefaultAsync<T>(sStoredProceedure, param, commandType: CommandType.StoredProcedure);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static async Task<T> GetAsyncWithparameter_SP<T>(string sStoredProceedure, object entity, string sCon = null) where T : new()
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection conn = new SqlConnection(sCon))
                {
                    var param = new DynamicParameters();
                    if (entity != null)
                    {
                        foreach (var item in entity.GetType().GetProperties())
                        {
                            if (item.GetValue(entity, null) != null)
                            {
                                param.Add(item.Name, item.GetValue(entity, null).ToString());
                            }
                        }
                    }
                    return await conn.QueryFirstOrDefaultAsync<T>(sStoredProceedure, param, commandType: CommandType.StoredProcedure);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static async Task<T> GetAsync_SP<T>(string sStoredProceedure, string sCon = null) where T : new()
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection conn = new SqlConnection(sCon))
                {
                    return await conn.QueryFirstOrDefaultAsync<T>(sStoredProceedure, commandType: CommandType.StoredProcedure);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static async Task<dynamic> GetDataAsync_SP(string sStoredProceedure, string sCon = null)
        {
            try
            {
                System.Threading.Thread.CurrentThread.CurrentCulture = System.Threading.Thread.CurrentThread.CurrentUICulture = new System.Globalization.CultureInfo("en");
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection con = new SqlConnection(sCon))
                {
                    using (SqlCommand cmd = new SqlCommand(sStoredProceedure, con))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        await con.OpenAsync();
                        var data = await cmd.ExecuteScalarAsync();
                        if (data != DBNull.Value)
                        { return data; }
                        return null;
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static async Task<dynamic> GetDataWithParameterAsync_SP(string sStoredProceedure, object entity, string sCon = null)
        {
            try
            {
                System.Threading.Thread.CurrentThread.CurrentCulture = System.Threading.Thread.CurrentThread.CurrentUICulture = new System.Globalization.CultureInfo("en");
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection con = new SqlConnection(sCon))
                {
                    using (SqlCommand cmd = new SqlCommand(sStoredProceedure, con))
                    {
                        if (entity != null)
                        {
                            foreach (var item in entity.GetType().GetProperties())
                            {
                                if (item.GetValue(entity, null) != null)
                                {
                                    cmd.Parameters.AddWithValue(item.Name, item.GetValue(entity, null).ToString());
                                }
                            }
                        }
                        cmd.CommandTimeout = 0;
                        cmd.CommandType = CommandType.StoredProcedure;
                        await con.OpenAsync();
                        var data = await cmd.ExecuteScalarAsync();
                        if (data == DBNull.Value)
                        {
                            return null;
                        }
                        return data;
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static async Task<bool> DatabaseExecutionAsync_SP(string sStoredProceedure, object entity = null, string sCon = null)
        {
            try
            {
                sCon = sCon ?? DBConnection.Connection;
                using (SqlConnection con = new SqlConnection(sCon))
                {
                    var param = new DynamicParameters();
                    if (entity != null)
                    {
                        foreach (var item in entity.GetType().GetProperties())
                        {
                            if (item.GetValue(entity, null) != null)
                            {
                                param.Add(item.Name, item.GetValue(entity, null).ToString());
                            }
                        }
                    }
                    var rslt = await con.ExecuteAsync(sStoredProceedure, param, commandType: CommandType.StoredProcedure);
                    return rslt > 0 ? true : false;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static async Task<DataSet> GetDataSetAsync_SP(string sStoredProceedure, object entity, string sCon = null)
        {
            var value = await Task<DataSet>.Factory.StartNew(() =>
            {
                try
                {
                    System.Threading.Thread.CurrentThread.CurrentCulture = System.Threading.Thread.CurrentThread.CurrentUICulture = new System.Globalization.CultureInfo("en");
                    DataSet ds = new DataSet();
                    sCon = sCon ?? DBConnection.Connection;
                    using (SqlConnection con = new SqlConnection(sCon))
                    {
                        using (SqlCommand cmd = new SqlCommand(sStoredProceedure, con))
                        {
                            cmd.CommandType = CommandType.StoredProcedure;
                            if (entity != null)
                            {
                                foreach (var item in entity.GetType().GetProperties())
                                {
                                    if (item.GetValue(entity, null) != null)
                                    {
                                        cmd.Parameters.AddWithValue(item.Name, item.GetValue(entity, null).ToString());
                                    }
                                }
                            }
                            cmd.CommandTimeout = 0;
                            using (SqlDataAdapter da = new SqlDataAdapter(cmd))
                            {
                                da.Fill(ds);
                            }
                        }
                    }
                    return ds;
                }
                catch (Exception ex)
                {
                    throw ex;
                }
            });
            return value;
        }
        #endregion

        #endregion
    }
}
