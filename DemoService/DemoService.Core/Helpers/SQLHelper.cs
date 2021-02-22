using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics.CodeAnalysis;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using FluentAssertions;
using Newtonsoft.Json;
using ServiceStack;

namespace DemoService.Core.Helpers
{
    /// <summary>
    /// Helper class to enable async operations upon SQL DB.
    /// </summary>
    public static class SQLHelper
    {
        public const string SqlScriptsPath = "SqlScript";
        public const string OUTPUT_PARAMETER_PREFIX = "-";
        public const string SQL_PARAMETER_PREFIX = "@";
        public static readonly Regex SqlParameterRegex = new Regex(@"@(\w+)");
        public static bool LogSampleEntity = false;

        // buffered SQL Scripts referred by names
        private static readonly Dictionary<string, string> SqlFileTexts = new Dictionary<string, string>();

        /// <summary>
        /// Convert the given SQL command or SQL filename to actual command text.
        /// </summary>
        /// <param name="commandTextOrFile">SQL Command text or *.sql file path. The file path could be absolute path, or relative path to either current working directory or ./SqlScripts folder.</param>
        /// <returns>The command text if it not referring a *.sql file, or content of the referred .sql file.</returns>
        public static string GetCommandText(string commandTextOrFile)
        {
            if (!commandTextOrFile.EndsWithIgnoreCase(".sql"))
            {
                return commandTextOrFile;
            }
            else if (SqlFileTexts.ContainsKey(commandTextOrFile.ToLower()))
            {
                return SqlFileTexts[commandTextOrFile.ToLower()];
            }

            string command = null;
            if (File.Exists(commandTextOrFile))
            {
                command = File.ReadAllText(commandTextOrFile);
            }
            else if (File.Exists(Path.Combine(SqlScriptsPath, commandTextOrFile)))
            {
                command = File.ReadAllText(Path.Combine(SqlScriptsPath, commandTextOrFile));
            }

            if (command == null)
            {
                throw new ArgumentException($"Failed to locate the SQL file specified by '{commandTextOrFile}'");
            }
            SqlFileTexts.Add(commandTextOrFile.ToLower(), command);
            return command;
        }

        /// <summary>
        /// Map given SQL command and its arguments of common .NET types to extended commandText and corresponding SqlDbType with names specified.
        /// </summary>
        /// <param name="command">SQL command to be executed.</param>
        /// <param name="arguments">The arguments used to execute with strict order as their references presented in the SQL Command</param>
        /// <returns>The formatted command and mapped SqlParameters together as a Tuple.</returns>
        public static (string, SqlParameter[]) Normalize(string command, params object[] arguments)
        {
            MatchCollection matches = SqlParameterRegex.Matches(command);

            if (matches.Count == 0)
            {
                if (arguments.Length > 0)
                    throw new ArgumentException($"{arguments.Length} arguments provided when command has no parameters:\n{command}");
                return (command, new SqlParameter[0]);
            }

            Dictionary<int, string> orderedParams = matches.Distinct().ToDictionary(
                match => match.Index,
                match => match.Value
            );
            List<string> distinctParamNames = orderedParams.GroupBy(kvp => kvp.Value.ToLower())
                .Select(grp => grp.First())
                .OrderBy(kvp => kvp.Key)
                .Select(kvp => kvp.Value)
                .ToList();

            if (distinctParamNames.Count != arguments.Length)
            {
                throw new ArgumentException($"There are {distinctParamNames.Count} distinct parameters, but {arguments.Length} arguments for: {string.Join(",", distinctParamNames)}");
            }

            List<SqlParameter> parameters = new List<SqlParameter>();
            Dictionary<string, string> replacements = new Dictionary<string, string>();
            for (int i = 0; i < distinctParamNames.Count; i++)
            {
                object value = arguments[i];
                if (value is IList)
                {
                    var col = (IList)value;
                    for (int j = 0; j < col.Count; j++)
                    {
                        parameters.Add(new SqlParameter($"{distinctParamNames[i]}{j}", col[i]));
                    }

                    replacements[distinctParamNames[i]] = string.Join(", ",
                        (Enumerable.Range(0, col.Count).Select(k => $"{distinctParamNames[i]}{k}")));
                }
                else
                {
                    parameters.Add(new SqlParameter(distinctParamNames[i], value == null ? DBNull.Value : value));
                }
            }

            foreach (var kvp in replacements)
            {
                command = Regex.Replace(command, kvp.Key, kvp.Value);
            }
            return (command, parameters.ToArray());
        }

        /// <summary>
        /// The single execution method to perform async SQL operation with connection of null, string, SqlConnection or SqlTransaction, and perform data conversions after execution.
        /// </summary>
        /// <typeparam name="T">The value type to be converted from the SQL execution.</typeparam>
        /// <param name="operation">Callback function to execute SqlCommand and return expected value</param>
        /// <param name="commandTextOrFile">The SQL file name or raw SQL script to be executed. If it ends with '.sql', the the content within SqlScriptsPath would be retrieved and buffered.</param>
        /// <param name="parameters">Converted SqlParameters with both names and values.</param>
        /// <param name="commandType">SQL command type, Text by default for normal queries.</param>
        /// <param name="connection">
        /// Either null, string, SqlConnection or SqlTransaction:
        ///     null: the the default DBConnection would be used to setup a SqlConnection that would be tear down after the SQL execution.
        ///     string: would also be used to setup a SqlConnection that would be tear down after the SQL execution.
        ///     SqlConnection: assume the connection would be managed by the caller, thus not closed after execution.
        ///     SqlTransaction: used to bind multiple executions into the same transaction to allow roll-back, the connection would not be closed after execution.
        /// </param>
        /// <returns>The result post-processed by the operation callback.</returns>
        public static async Task<T> ExecuteAsync<T>(Func<SqlCommand, Task<T>> operation, String commandTextOrFile, SqlParameter[] parameters, CommandType commandType = CommandType.Text, object connection = null)
        {
            string command = GetCommandText(commandTextOrFile);

            if (connection == null || connection is string)
            {
                string connectionString = connection?.ToString() ?? Settings.Get(Settings.DBConnectionStringKey);
                using (SqlConnection conn = new SqlConnection(connectionString))
                {
                    // Console.WriteLine($"Execute SQL with '{connectionString}'");
                    using (SqlCommand cmd = new SqlCommand(command, conn))
                    {
                        cmd.CommandType = commandType;
                        cmd.Parameters.AddRange(parameters);

                        conn.Open();
                        return await operation(cmd);
                    }
                }
            }
            else if (connection is SqlConnection)
            {
                SqlConnection con = (SqlConnection)connection;
                using (SqlCommand cmd = new SqlCommand(command, con))
                {
                    if (con.State != ConnectionState.Open)
                    {
                        con.Open();
                    }
                    cmd.CommandType = commandType;
                    cmd.Parameters.AddRange(parameters);

                    return await operation(cmd);
                }
            }
            else if (connection is SqlTransaction)
            {
                SqlTransaction transaction = (SqlTransaction)connection;
                SqlConnection con = transaction.Connection;
                using (SqlCommand cmd = new SqlCommand(command, con, transaction))
                {
                    if (con.State != ConnectionState.Open)
                    {
                        con.Open();
                    }
                    cmd.CommandType = commandType;
                    cmd.Parameters.AddRange(parameters);

                    return await operation(cmd);
                }
            }
            else
            {
                throw new NotImplementedException($"connection shall be either of string or SqlConnection or SqlTransaction!");
            }
        }


        /// <summary>
        /// Wrapper of above method by feeding with any number of arguments that would be converted to SqlParameters automatically.
        /// </summary>
        /// <typeparam name="T">The value type to be converted from the SQL execution.</typeparam>
        /// <param name="operation">Callback function to execute SqlCommand and return expected value</param>
        /// <param name="commandTextOrFile">The SQL file name or raw SQL script to be executed. If it ends with '.sql', the the content within SqlScriptsPath would be retrieved and buffered.</param>
        /// <param name="commandType">SQL command type, Text by default for normal queries.</param>
        /// <param name="connection">
        /// Either null, string, SqlConnection or SqlTransaction:
        ///     null: the the default DBConnection would be used to setup a SqlConnection that would be tear down after the SQL execution.
        ///     string: would also be used to setup a SqlConnection that would be tear down after the SQL execution.
        ///     SqlConnection: assume the connection would be managed by the caller, thus not closed after execution.
        ///     SqlTransaction: used to bind multiple executions into the same transaction to allow roll-back, the connection would not be closed after execution.
        /// </param>
        /// <param name="arguments">Any number of .NET objects to be fed as SQL Parameters.</param>
        /// <returns>The result post-processed by the operation callback.</returns>
        public static async Task<T> ExecuteAsync<T>(Func<SqlCommand, Task<T>> operation, String commandTextOrFile,
            CommandType commandType = CommandType.Text, object connection = null, params object[] arguments)
        {
            string command = GetCommandText(commandTextOrFile);
            var commandAndParameters = Normalize(command, arguments);
            command = commandAndParameters.Item1;
            SqlParameter[] parameters = commandAndParameters.Item2;
            return await ExecuteAsync(operation, command, parameters, commandType, connection);
        }


        /// <summary>
        /// Wrapper of above method by feeding with any number of named arguments that would be converted to SqlParameters automatically.
        /// </summary>
        /// <typeparam name="T">The value type to be converted from the SQL execution.</typeparam>
        /// <param name="operation">Callback function to execute SqlCommand and return expected value</param>
        /// <param name="commandTextOrFile">The SQL file name or raw SQL script to be executed. If it ends with '.sql', the the content within SqlScriptsPath would be retrieved and buffered.</param>
        /// <param name="arguments">Any number of named .NET objects to be fed as SQL Parameters.</param>
        /// <param name="commandType">SQL command type, Text by default for normal queries.</param>
        /// <param name="connection">
        /// Either null, string, SqlConnection or SqlTransaction:
        ///     null: the the default DBConnection would be used to setup a SqlConnection that would be tear down after the SQL execution.
        ///     string: would also be used to setup a SqlConnection that would be tear down after the SQL execution.
        ///     SqlConnection: assume the connection would be managed by the caller, thus not closed after execution.
        ///     SqlTransaction: used to bind multiple executions into the same transaction to allow roll-back, the connection would not be closed after execution.
        /// </param>
        /// <returns>The result post-processed by the operation callback.</returns>
        public static async Task<T> ExecuteAsync<T>(Func<SqlCommand, Task<T>> operation, String commandTextOrFile, Dictionary<string, object> arguments,
            CommandType commandType = CommandType.Text, object connection = null)
        {
            SqlParameter[] parameters = AsSqlParameters(arguments);
            return await ExecuteAsync(operation, commandTextOrFile, parameters, commandType, connection);
        }

        /// <summary>
        /// With optional CommandType, connection and Parameters, executes a Transact-SQL statement asynchronously against the connection and returns the number of rows affected.
        /// </summary>
        /// <param name="commandTextOrFile">SQL command to be executed, or full or relative path to the .sql file.</param>
        /// <param name="commandType">Type (StoredProcedure, Text, TableDirect) of SQL Command to be executed, with default value of Text.</param>
        /// <param name="connection">Optional connectionString, SqlConnection or SqlTransaction to the SQL DB, the StartUp.DbConnectionString would be used if not provided.</param>
        /// <param name="arguments">The arguments used to execute with strict order as their references presented in the SQL Command.</param>
        /// <returns>The number of rows affected.</returns>
        public static async Task<int> ExecuteNonQueryAsync(String commandTextOrFile, CommandType commandType = CommandType.Text, object connection = null, params object[] arguments)
        {
            int affectedRows = await ExecuteAsync(cmd => cmd.ExecuteNonQueryAsync(), commandTextOrFile, commandType, connection,
                arguments);
            return affectedRows;
        }

        /// <summary>
        /// With optional CommandType, connection and Parameters, executes a Transact-SQL statement asynchronously against the connection and returns the number of rows affected.
        /// </summary>
        /// <param name="commandTextOrFile">SQL command to be executed, or full or relative path to the .sql file.</param>
        /// <param name="arguments">The arguments used to execute with strict order as their references presented in the SQL Command.</param>
        /// <param name="commandType">Type (StoredProcedure, Text, TableDirect) of SQL Command to be executed, with default value of Text.</param>
        /// <param name="connection">Optional connectionString, SqlConnection or SqlTransaction to the SQL DB, the StartUp.DbConnectionString would be used if not provided.</param>
        /// <returns>The number of rows affected.</returns>
        public static async Task<int> ExecuteNonQueryAsync(String commandTextOrFile, Dictionary<string, object> arguments, CommandType commandType = CommandType.Text, object connection = null)
        {
            int affectedRows = await ExecuteAsync(cmd => cmd.ExecuteNonQueryAsync(), commandTextOrFile, arguments, commandType, connection);
            return affectedRows;
        }

        /// <summary>
        /// With optional CommandType, connection and Parameters, executes a Transact-SQL statement synchronously against the connection and returns the number of rows affected.
        /// </summary>
        /// <param name="commandTextOrFile">SQL command to be executed, or full or relative path to the .sql file.</param>
        /// <param name="commandType">Type (StoredProcedure, Text, TableDirect) of SQL Command to be executed, with default value of Text.</param>
        /// <param name="connection">Optional connectionString, SqlConnection or SqlTransaction to the SQL DB, the StartUp.DbConnectionString would be used if not provided.</param>
        /// <param name="arguments">The arguments used to execute with strict order as their references presented in the SQL Command.</param>
        /// <returns>The number of rows affected.</returns>
        public static int ExecuteNonQuery(String commandTextOrFile, CommandType commandType = CommandType.Text, object connection = null, params object[] arguments)
        {
            return ExecuteNonQueryAsync(commandTextOrFile, commandType, connection, arguments).Result;
        }

        /// <summary>
        /// Call-back to retrieve the output parameters as named values.
        /// </summary>
        /// <param name="command">The SqlCommand AFTER execution.</param>
        /// <returns>Named returned values as a dictionary.</returns>
        public static IDictionary<string, object> GetOutputs(SqlCommand command)
        {
            IDictionary<string, object> outputs = command.Parameters.Cast<SqlParameter>()
                .Where(p => p.Direction == ParameterDirection.Output).ToDictionary(
                    p => p.ParameterName,
                    p => p.Value
                );
            return outputs;
        }

        /// <summary>
        /// Execute Stored Procedure with Output values returned.
        /// </summary>
        /// <param name="commandTextOrFile">The SQL file name or raw SQL script to be executed. If it ends with '.sql', the the content within SqlScriptsPath would be retrieved and buffered.</param>
        /// <param name="parameters">Converted SqlParameters with both names and values.</param>
        /// <param name="commandType">SQL command type, Text by default for normal queries.</param>
        /// <param name="connection">
        /// Either null, string, SqlConnection or SqlTransaction:
        ///     null: the the default DBConnection would be used to setup a SqlConnection that would be tear down after the SQL execution.
        ///     string: would also be used to setup a SqlConnection that would be tear down after the SQL execution.
        ///     SqlConnection: assume the connection would be managed by the caller, thus not closed after execution.
        ///     SqlTransaction: used to bind multiple executions into the same transaction to allow roll-back, the connection would not be closed after execution.
        /// </param>
        /// <returns>Named returned values as a dictionary.</returns>
        public static async Task<IDictionary<string, object>> ExecuteStoredProcedureAsync(string commandTextOrFile, SqlParameter[] parameters,
            CommandType commandType = CommandType.Text, object connection = null)
        {
            Func<SqlCommand, Task<IDictionary<string, object>>> operation = async cmd =>
            {
                await cmd.ExecuteNonQueryAsync();
                return GetOutputs(cmd);
            };

            return await ExecuteAsync(operation, commandTextOrFile, parameters, commandType, connection);
        }


        /// <summary>
        /// With optional CommandType, connection and Parameters, executes the query synchronously, and returns the first column of
        /// the first row in the result set returned by the query. Additional columns or rows are ignored.
        /// </summary>
        /// <param name="commandTextOrFile">SQL command to be executed, or full or relative path to the .sql file.</param>
        /// <param name="commandType">Type (StoredProcedure, Text, TableDirect) of SQL Command to be executed, with default value of Text.</param>
        /// <param name="connection">Optional connectionString, SqlConnection or SqlTransaction to the SQL DB, the StartUp.DbConnectionString would be used if not provided.</param>
        /// <param name="arguments">The arguments used to execute with strict order as their references presented in the SQL Command.</param>
        /// <returns>The first column of the first row in the result set, or a null reference (Nothing in Visual Basic) if the result set is empty. Returns a maximum of 2033 characters.</returns>
        public static Object ExecuteScalar(String commandTextOrFile, CommandType commandType = CommandType.Text, object connection = null, params object[] arguments)
        {
            return ExecuteAsync(cmd => cmd.ExecuteScalarAsync(), commandTextOrFile, commandType, connection, arguments).Result;
        }

        /// <summary>
        /// With optional CommandType, connection and Parameters, executes the query asynchronously, and returns the first column of
        /// the first row in the result set returned by the query. Additional columns or rows are ignored.
        /// </summary>
        /// <param name="commandTextOrFile">SQL command to be executed, or full or relative path to the .sql file.</param>
        /// <param name="commandType">Type (StoredProcedure, Text, TableDirect) of SQL Command to be executed, with default value of Text.</param>
        /// <param name="connection">Optional connectionString, SqlConnection or SqlTransaction to the SQL DB, the StartUp.DbConnectionString would be used if not provided.</param>
        /// <param name="arguments">The arguments used to execute with strict order as their references presented in the SQL Command.</param>
        /// <returns>The first column of the first row in the result set, or a null reference (Nothing in Visual Basic) if the result set is empty. Returns a maximum of 2033 characters.</returns>
        public static async Task<object> ExecuteScalarAsync(String commandTextOrFile, CommandType commandType = CommandType.Text, object connection = null, params object[] arguments)
        {
            return await ExecuteAsync(cmd => cmd.ExecuteScalarAsync(), commandTextOrFile, commandType, connection, arguments);
        }

        /// <summary>
        /// Callback method to convert data to strong-typed.
        /// </summary>
        /// <typeparam name="T">Type of the result element model.</typeparam>
        /// <param name="reader">SqlDataReader returned by execting SQL script.</param>
        /// <param name="avoidJValue">Convert JObject to dynamic if TRUE to save problems later</param>
        /// <returns>List of strong-typed elements.</returns>
        private static async Task<List<T>> ReaderToEntitiesAsync<T>([NotNull] SqlDataReader reader, bool avoidJValue = false)
        {
            reader.IsClosed.Should().BeFalse($"The reader must be opened before reading!");

            int fieldCount = reader.FieldCount;
            List<Dictionary<string, object>> records = new List<Dictionary<string, object>>();
            while (await reader.ReadAsync())
            {
                //Let it throw exception when duplicated columns have different values
                Dictionary<string, object> rowDict = Enumerable.Range(0, fieldCount)
                    .Select(index => (Key: reader.GetName(index), Value: reader.GetValue(index)))
                    .Distinct()
                    .ToDictionary(
                        item => item.Key,
                        item => item.Value
                        );
                records.Add(rowDict);
            }

            if (records is List<T>)
            {
                return records as List<T>;
            }

            string recordsJson = JsonConvert.SerializeObject(records);
            if (avoidJValue && typeof(T) == typeof(Object))
            {
                return DynamicHelper.AsDynamics(recordsJson) as List<T>;
            }

            //Avoid Deserialize to List<dynamic> by JsonConvert that would keep JValues instead of .NET objects
            List<T> entities = JsonConvert.DeserializeObject<List<T>>(recordsJson);

            if (LogSampleEntity && entities.Count > 0)
            {
                Console.WriteLine($"Last entity JSON: {JsonConvert.SerializeObject(entities.Last(), Formatting.Indented)}");
            }

            return entities;
        }

        private static async Task<Dictionary<string, List<dynamic>>> ReaderToTables(SqlDataReader reader, params string[] tableNames)
        {
            reader.Should().NotBeNull();
            reader.IsClosed.Should().BeFalse($"The reader must be opened.");

            int tableNum = 0;
            Dictionary<string, List<dynamic>> resultSet = new Dictionary<string, List<dynamic>>();
            do
            {
                int fieldCount = reader.FieldCount;
                string tableName = (tableNames != null && tableNames.Length > tableNum)
                    ? tableNames[tableNum]
                    : $"table{tableNum}";

                List<dynamic> rows = await ReaderToEntitiesAsync<dynamic>(reader);
                resultSet.Add(tableName, rows);
            } while (reader.NextResult());

            return resultSet;
        }

        private static List<T> ReaderToEntities<T>(DataTable table)
        {
            table.Should().NotBeNull();

            List<string> columnNames = Enumerable.Range(0, table.Columns.Count).Select(i => table.Columns[i].ColumnName)
                .ToList();

            List<dynamic> rowList = new List<dynamic>();

            foreach (DataRow row in table.Rows)
            {
                IDictionary<string, object> rowDynamic = new ExpandoObject();
                columnNames.ForEach(name => rowDynamic[name] = row[name]);
                rowList.Add(rowDynamic as dynamic);
            }

            if (rowList is List<T> listT)
            {
                return listT;
            }

            string recordsJson = JsonConvert.SerializeObject(rowList);

            //Avoid Deserialize to List<dynamic> by JsonConvert that would keep JValues instead of .NET objects
            List<T> entities = JsonConvert.DeserializeObject<List<T>>(recordsJson);

            if (LogSampleEntity && entities.Count > 0)
            {
                Console.WriteLine($"Last entity JSON: {JsonConvert.SerializeObject(entities.Last(), Formatting.Indented)}");
            }

            return entities;
        }

        /// <summary>
        /// Convert named .NET objects to SqlParameters.
        /// </summary>
        /// <param name="arguments">The named .NET objects.</param>
        /// <returns>Array of SqlParameters.</returns>
        public static SqlParameter[] AsSqlParameters(Dictionary<string, object> arguments)
        {
            List<SqlParameter> parameters = new List<SqlParameter>();

            foreach (var kvp in arguments)
            {
                SqlParameter param = new SqlParameter();
                if (kvp.Key.StartsWith(OUTPUT_PARAMETER_PREFIX + SQL_PARAMETER_PREFIX))
                {
                    param.ParameterName = kvp.Key.Substring(1);
                    param.Direction = ParameterDirection.Output;
                    string expected = kvp.Value.ToString();
                    param.SqlDbType = Enum.Parse<SqlDbType>(expected.Substring(0, expected.IndexOf('_')));
                }
                else
                {
                    param.ParameterName = kvp.Key.StartsWith(SQL_PARAMETER_PREFIX) ? kvp.Key : $"{SQL_PARAMETER_PREFIX}{kvp.Key}";
                    param.Value = kvp.Value;
                }

                parameters.Add(param);
            }

            return parameters.ToArray();
        }

        /// <summary>
        /// With optional CommandType, connection and Parameters, execute the command synchronously and return the results as a list of entities with type specified.
        /// </summary>
        /// <typeparam name="T">Type of the entity represented by each rows of the data reader</typeparam>
        /// <param name="commandTextOrFile">SQL command to be executed, or full or relative path to the .sql file.</param>
        /// <param name="commandType">Type (StoredProcedure, Text, TableDirect) of SQL Command to be executed, with default value of Text.</param>
        /// <param name="connection">Optional connection, SqlConnection or SqlTransaction to the SQL DB, the StartUp.DbConnectionString would be used if not provided.</param>
        /// <param name="arguments">The arguments used to execute with strict order as their references presented in the SQL Command.</param>
        /// <returns>A list of typed objects representing the asynchronous operation.</returns>
        public static List<T> Query<T>(String commandTextOrFile, CommandType commandType = CommandType.Text, object connection = null, params object[] arguments)
        {
            Func<SqlCommand, Task<List<T>>> operation = async (SqlCommand cmd) =>
            {
                SqlDataReader reader = cmd.ExecuteReader();
                return await ReaderToEntitiesAsync<T>(reader);
            };

            List<T> records = ExecuteAsync(operation, commandTextOrFile, commandType, connection, arguments).Result;
            return records;
        }

        /// <summary>
        /// With optional CommandType, connection and Parameters, execute the command synchronously and return the results as a list of dynamic objects.
        /// </summary>
        /// <param name="commandTextOrFile">SQL command to be executed, or full or relative path to the .sql file.</param>
        /// <param name="commandType">Type (StoredProcedure, Text, TableDirect) of SQL Command to be executed, with default value of Text.</param>
        /// <param name="connection">Optional connection, SqlConnection or SqlTransaction to the SQL DB, the StartUp.DbConnectionString would be used if not provided.</param>
        /// <param name="arguments">The arguments used to execute with strict order as their references presented in the SQL Command.</param>
        /// <returns>A list of dynamics representing the asynchronous operation.</returns>
        public static List<dynamic> Query(String commandTextOrFile, CommandType commandType = CommandType.Text, object connection = null, params object[] arguments)
        {
            Func<SqlCommand, Task<List<dynamic>>> operation = async (SqlCommand cmd) =>
            {
                SqlDataReader reader = cmd.ExecuteReader();
                return await ReaderToEntitiesAsync<dynamic>(reader, true);
            };

            List<dynamic> records = ExecuteAsync(operation, commandTextOrFile, commandType, connection, arguments).Result;
            return records;
        }

        /// <summary>
        /// With optional CommandType, connection and Parameters, execute the command synchronously and return the results as a list of entities with type specified.
        /// </summary>
        /// <typeparam name="T">Type of the entity represented by each rows of the data reader</typeparam>
        /// <param name="commandTextOrFile">SQL command to be executed, or full or relative path to the .sql file.</param>
        /// <param name="arguments">The named arguments to be used to execute given SQL command</param>
        /// <param name="commandType">Type (StoredProcedure, Text, TableDirect) of SQL Command to be executed, with default value of Text.</param>
        /// <param name="connectionString">Optional connection to the SQL DB, the StartUp.DbConnectionString would be used if not provided.</param>
        /// <returns>A list of typed objects representing the asynchronous operation.</returns>
        public static List<T> Query<T>(String commandTextOrFile, Dictionary<string, object> arguments, CommandType commandType = CommandType.Text, String connection = null)
        {
            Func<SqlCommand, Task<List<T>>> operation = (SqlCommand cmd) =>
            {
                SqlDataReader reader = cmd.ExecuteReader();
                return ReaderToEntitiesAsync<T>(reader);
            };

            List<T> records = ExecuteAsync(operation, commandTextOrFile, arguments, commandType, connection).Result;
            return records;
        }

        /// <summary>
        /// With optional CommandType, connection and Parameters, execute the command synchronously and return the results as a list of dynamic objects.
        /// </summary>
        /// <param name="commandTextOrFile">SQL command to be executed, or full or relative path to the .sql file.</param>
        /// <param name="arguments">The named arguments to be used to execute given SQL command</param>
        /// <param name="commandType">Type (StoredProcedure, Text, TableDirect) of SQL Command to be executed, with default value of Text.</param>
        /// <param name="connectionString">Optional connection to the SQL DB, the StartUp.DbConnectionString would be used if not provided.</param>
        /// <returns>A list of dynamics representing the asynchronous operation.</returns>
        public static List<dynamic> Query(String commandTextOrFile, Dictionary<string, object> arguments, CommandType commandType = CommandType.Text, String connection = null)
        {
            Func<SqlCommand, Task<List<dynamic>>> operation = (SqlCommand cmd) =>
            {
                SqlDataReader reader = cmd.ExecuteReader();
                return ReaderToEntitiesAsync<dynamic>(reader, true);
            };

            List<dynamic> records = ExecuteAsync(operation, commandTextOrFile, arguments, commandType, connection).Result;
            return records;
        }

        /// <summary>
        /// With optional CommandType, connection and Parameters, execute the command asynchronously and return the results as a list of dynamic objects.
        /// </summary>
        /// <typeparam name="T">Type of the entity represented by each rows of the data reader</typeparam>
        /// <param name="commandTextOrFile">SQL command to be executed, or full or relative path to the .sql file.</param>
        /// <param name="commandType">Type (StoredProcedure, Text, TableDirect) of SQL Command to be executed, with default value of Text.</param>
        /// <param name="connectionString">Optional connection to the SQL DB, the StartUp.DbConnectionString would be used if not provided.</param>
        /// <param name="arguments">The arguments used to execute with strict order as their references presented in the SQL Command.</param>
        /// <returns>A list of typed objects representing the asynchronous operation.</returns>
        public static async Task<List<T>> QueryAsync<T>(String commandTextOrFile, CommandType commandType = CommandType.Text, String connection = null, params object[] arguments)
        {
            Func<SqlCommand, Task<List<T>>> operation = async (SqlCommand cmd) =>
            {
                SqlDataReader reader = await cmd.ExecuteReaderAsync();
                return await ReaderToEntitiesAsync<T>(reader);
            };

            Task<List<T>> records = ExecuteAsync(operation, commandTextOrFile, commandType, connection, arguments);
            List<T> result = await records;
            return result;
        }

        /// <summary>
        /// With optional CommandType, connection and Parameters, execute the command asynchronously and return the results as a list of dynamic objects.
        /// </summary>
        /// <param name="commandTextOrFile">SQL command to be executed, or full or relative path to the .sql file.</param>
        /// <param name="arguments">The arguments with names specified as their references presented in the SQL Command.</param>
        /// <param name="commandType">Type (StoredProcedure, Text, TableDirect) of SQL Command to be executed, with default value of Text.</param>
        /// <param name="connectionString">Optional connection to the SQL DB, the StartUp.DbConnectionString would be used if not provided.</param>
        /// <returns>A list of dynamic objects representing the asynchronous operation.</returns>
        public static async Task<List<T>> QueryAsync<T>(String commandTextOrFile, Dictionary<string, object> arguments,
            CommandType commandType = CommandType.Text, String connection = null)
        {
            Func<SqlCommand, Task<List<T>>> operation = async (SqlCommand cmd) =>
            {
                SqlDataReader reader = await cmd.ExecuteReaderAsync();
                return await ReaderToEntitiesAsync<T>(reader);
            };

            Task<List<T>> records = ExecuteAsync(operation, commandTextOrFile, arguments, commandType, connection);
            List<T> result = await records;
            return result;
        }

        private static List<T> AggregateRead<T>(DataSet dataSet)
        {
            List<Dictionary<string, object>> aggregatedEntries = new List<Dictionary<string, object>>();

            foreach (DataTable dataTable in dataSet.Tables)
            {
                string[] columns = dataTable.Columns.Cast<DataColumn>()
                    .Select(x => x.ColumnName).ToArray();
                int columnCount = columns.Count();

                for (int rowIndex = 0; rowIndex < dataTable.Rows.Count; rowIndex++)
                {
                    DataRow row = dataTable.Rows[rowIndex];
                    object[] values = row.ItemArray;
                    if (aggregatedEntries.Count <= rowIndex)
                    {
                        aggregatedEntries.Add(new Dictionary<string, object>());
                    }
                    Dictionary<string, object> rowDict = aggregatedEntries[rowIndex];

                    for (int i = 0; i < columnCount; i++)
                    {
                        string key = columns[i];
                        object value = values[i];
                        if (!rowDict.ContainsKey(key))
                        {
                            rowDict.Add(key, value);
                        }
                        else if (!Object.Equals(rowDict[key], value))
                        {
                            throw new ArgumentException($"The row[{i}]['{key}'] has value '{rowDict[key]}' that is conflicted with '{value}'");
                        }
                    }
                }
            }

            if (aggregatedEntries is List<T>)
            {
                return aggregatedEntries as List<T>;
            }

            string recordsJson = JsonConvert.SerializeObject(aggregatedEntries);
            List<T> entities = JsonConvert.DeserializeObject<List<T>>(recordsJson);

            if (LogSampleEntity && entities.Count > 0)
            {
                Console.WriteLine($"Last entity JSON: {JsonConvert.SerializeObject(entities.Last(), Formatting.Indented)}");
            }

            return entities;
        }


        /// <summary>
        /// To be obsoleted. For trouble-shooting only.
        /// </summary>
        public static async Task<List<T>> QueryMultiple<T>(String commandTextOrFile,
            CommandType commandType = CommandType.Text,
            String connection = null,
            params object[] arguments)
        {
            Func<SqlCommand, Task<List<T>>> operation = async (SqlCommand cmd) =>
            {
                // return AggregateRead<T>(reader);
                using (var adapter = new SqlDataAdapter(cmd))
                {
                    DataSet dataSet = new DataSet();
                    adapter.Fill(dataSet);
                    return AggregateRead<T>(dataSet);
                }
            };

            Task<List<T>> records = ExecuteAsync(operation, commandTextOrFile, commandType, connection, arguments);
            List<T> result = await records;
            return result;
        }


        /// <summary>
        /// Callback method to extract data asynchronously with a SqlDataReader.
        /// </summary>
        private static async Task<Dictionary<string, List<dynamic>>> ReaderToTablesAsync([NotNull] SqlDataReader reader, params string[] tableNames)
        {
            int tableNum = 0;
            Dictionary<string, List<dynamic>> resultSet = new Dictionary<string, List<dynamic>>();
            do
            {
                int fieldCount = reader.FieldCount;
                string tableName = (tableNames != null && tableNames.Length > tableNum)
                    ? tableNames[tableNum]
                    : $"table{tableNum}";

                List<dynamic> rows = await ReaderToEntitiesAsync<dynamic>(reader, true);

                resultSet.Add(tableName, rows);
                tableNum++;
            } while (reader.NextResult());

            return resultSet;
        }

        /// <summary>
        /// Callback method to extract data from DataSet.
        /// </summary>
        private static Dictionary<string, List<dynamic>> FromDataSet(DataSet dataSet, params string[] tableNames)
        {
            Dictionary<string, List<dynamic>> resultSet = new Dictionary<string, List<dynamic>>();
            for (int tableNum = 0; tableNum < dataSet.Tables.Count; tableNum++)
            {
                string tableName = (tableNames != null && tableNames.Length > tableNum)
                    ? tableNames[tableNum]
                    : $"table{tableNum}";

                List<dynamic> tableRowDict = ReaderToEntities<dynamic>(dataSet.Tables[tableNum]);
                resultSet.Add(tableName, tableRowDict);
            }

            return resultSet;
        }


        /// <summary>
        /// Bad performance, not recommended and for trouble-shooting only!!!!!!
        /// Execute the script in a synchronous manner by using SqlDataAdapter to fill the DataSet.
        /// </summary>
        /// <param name="commandTextOrFile">Script or filePath containing the script.</param>
        /// <param name="arguments">Named arguments to be converted to SqlParameters.</param>
        /// <param name="tableNames">Names to represent the tables of data returned.</param>
        /// <returns>A dictionary with tableNames as the keys, rows of each table as list of dynamic objects.</returns>
        public static Dictionary<string, List<dynamic>> QueryMultiple(String commandTextOrFile, Dictionary<string, object> arguments, params string[] tableNames)
        {
            string command = GetCommandText(commandTextOrFile);
            SqlParameter[] parameters = AsSqlParameters(arguments);


            using (SqlConnection conn = new SqlConnection(Settings.Get(Settings.DBConnectionStringKey)))
            {
                using (SqlCommand cmd = new SqlCommand(command, conn))
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.Parameters.AddRange(parameters);

                    conn.Open();

                    using (var adapter = new SqlDataAdapter(cmd))
                    {
                        DataSet dataSet = new DataSet();
                        adapter.Fill(dataSet);
                        Dictionary<string, List<dynamic>> tableDict = FromDataSet(dataSet, tableNames);
                        return tableDict;
                    }
                }
            }
        }

        /// <summary>
        /// Execute multiple queries or a SP returning multiple tables to get the results as named List of dynamic objects.
        /// </summary>
        /// <param name="commandTextOrFile">Script or filePath containing the script.</param>
        /// <param name="arguments">Named arguments to be converted to SqlParameters.</param>
        /// <param name="tableNames">Names to represent the tables of data returned.</param>
        /// <returns>A dictionary with tableNames as the keys, rows of each table as list of dynamic objects.</returns>
        public static async Task<Dictionary<string, List<dynamic>>> QueryMultipleAsync(String commandTextOrFile, Dictionary<string, object> arguments, params string[] tableNames)
        {
            Func<SqlCommand, Task<Dictionary<string, List<dynamic>>>> operation = async (SqlCommand cmd) =>
            {
                SqlDataReader reader = cmd.ExecuteReader();
                return await ReaderToTablesAsync(reader, tableNames);
            };

            Dictionary<string, List<dynamic>> tables = await ExecuteAsync(operation, commandTextOrFile, arguments);
            return tables;

        }

        /// <summary>
        /// Use an existing SqlTransaction to execute a Script/StoredProcedure.
        /// </summary>
        /// <param name="transaction">Disposable Transaction to maintain the SqlConnection and execute multiple operations.</param>
        /// <param name="commandTextOrFile">The SQL script or filePath containing the script.</param>
        /// <param name="arguments">Any number of .NET objects to be used as the SqlParameters.</param>
        /// <returns>Number of rows changed.</returns>
        public static async Task<int> ExecuteInTransactionAsync([NotNull] SqlTransaction transaction, String commandTextOrFile,
            params object[] arguments)
        {
            return await ExecuteNonQueryAsync(commandTextOrFile, connection: transaction, arguments: arguments);
        }

    }


    /// <summary>
    /// An disposable object to keep a single SqlConnection to execute multiple operations as a batch.
    /// </summary>
    public class Transaction : IDisposable
    {
        public const int CommandDescLength = 50;

        private readonly SqlConnection connection;
        private readonly string transactionName;
        private SqlTransaction transaction;
        private readonly bool autoCommit;

        public Transaction(string transactionName, bool autoCommit = false, string connectionString = null, int timeoutSeconds = 30)
        {
            this.transactionName = transactionName;
            this.autoCommit = autoCommit;
            string conString = connection?.ToString() ?? Settings.Get(Settings.DBConnectionStringKey);
            Match match = Regex.Match(conString, @"Connection Timeout=\d+");
            if (match.Success)
            {
                conString = Regex.Replace(conString, @"Connection Timeout=\d+", $"Connection Timeout={timeoutSeconds}");
            }
            else
            {
                int lastIndex = conString.LastIndexOf(";");
                conString = conString.Substring(0, lastIndex + 1) + $"Connection Timeout={timeoutSeconds};" + conString.Substring(lastIndex + 1);
            }
            connection = new SqlConnection(conString);
            connection.Open();
            transaction = connection.BeginTransaction(transactionName);
        }

        public int Execute(String commandTextOrFile, params object[] arguments)
        {
            return ExecuteAsync(commandTextOrFile, arguments).Result;
        }

        public async Task<int> ExecuteAsync(String commandTextOrFile, params object[] arguments)
        {
            string command = commandTextOrFile.Length < CommandDescLength
                ? commandTextOrFile
                : commandTextOrFile.Substring(0, CommandDescLength) + "...";
            try
            {
                int result = await SQLHelper.ExecuteInTransactionAsync(this.transaction, commandTextOrFile, arguments);
                Console.WriteLine($"{transactionName}-{command} successfully!");
                return result;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{transactionName} failed with {command}: {ex.Message}");

                // Attempt to roll back the transaction.
                try
                {
                    transaction.Rollback();
                }
                catch (Exception ex2)
                {
                    // This catch block will handle any errors that may have occurred
                    // on the server that would cause the rollback to fail, such as
                    // a closed connection.
                    Console.WriteLine("Rollback Exception Type: {0}", ex2.GetType());
                    Console.WriteLine("  Message: {0}", ex2.Message);
                }

                throw;
            }
        }

        public void Dispose()
        {
            string operation = autoCommit ? "Commit" : "Rollback";
            try
            {
                if (autoCommit)
                {
                    transaction.Commit();
                }
                else
                {
                    transaction.Rollback();
                }

                connection.Close();

                Console.WriteLine($"Transaction {transactionName} {operation}() successfully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{ex.GetType()} when {operation}: {ex.Message}");
            }
            finally
            {
                if (connection != null)
                {
                    connection.Dispose();
                    Console.WriteLine($"Transaction disposed.");
                }
            }
        }
    }

}

