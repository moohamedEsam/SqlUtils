using System;
using System.Data.Common;

namespace SqlUtils.Helpers
{
    public interface BaseHelper
    {
        void Connect(string serverName, string databaseName, string username, string password);
        
        void AssignTransactionIfExists(DbCommand command);

        DbCommand GetCommand(string query);

        DbDataAdapter GetDbDataAdapter(string query);

        void ClearTransaction();

        void InitializeTransaction();

        void Commit();

        void RollBack();

        void Disconnect();

    }
}