using System.Collections.Generic;
using System.Data;
using System.Linq;
using Npgsql;

namespace Migrator.Providers.Utility
{
	public static class PostgreSqlServerUtility
	{
		public static void RemoveAllTablesFromDefaultDatabase(string connectionString , string defaultSchemaConnection = "public")
		{
			using (var connection = new NpgsqlConnection(connectionString))
			{
				connection.Open();

				List<string> tableNames = GetAllTableNames(connection , defaultSchemaConnection).ToList();

				foreach (string table in tableNames)
				{
					using (var command = new NpgsqlCommand(string.Format("DROP TABLE IF EXISTS {0} CASCADE", table), connection))
					{
						command.ExecuteNonQuery();
					}
				}

				connection.Close();
			}
		}

		static IEnumerable<string> GetAllTableNames(NpgsqlConnection connection , string defaultSchemaConnection = "public")
		{
			using (var command = new NpgsqlCommand("SELECT table_name FROM information_schema.tables WHERE table_schema = '" + defaultSchemaConnection + "'", connection))
			{
				using (IDataReader reader = command.ExecuteReader(CommandBehavior.Default))
				{
					while (reader.Read())
					{
						yield return (string) reader[0];
					}
				}
			}
		}
	}
}