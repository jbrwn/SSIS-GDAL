using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.Dts.Pipeline;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime;
using OSGeo.OGR;
using OSGeo.GDAL;


namespace SSISGDAL.SqlServer.DTS.Connections
{
    [DtsConnection(
        ConnectionType = "OGR",
        DisplayName = "OGR Connection Manager",
        Description = "Connection manager for OGR data sources"
    )]

    public class OGRConnectionManager : ConnectionManagerBase
    {
        private string _OGRConnectionString = String.Empty;
        private bool _OpenForUpdate = false;
        private string _OGRConfigOptions;
        private string _connectionString = String.Empty;

        public string OGRConnectionString
        {
            get
            {
                return this._OGRConnectionString;
            }
            set
            {
                this._OGRConnectionString = value;
            }
        }

        public bool OpenForUpdate
        {
            get
            {
                return this._OpenForUpdate;
            }
            set
            {
                this._OpenForUpdate = value;
            }
        }

        public string OGRConfigOptions
        {
            get
            {
                return this._OGRConfigOptions;
            }
            set
            {
                this._OGRConfigOptions = value;
            }
        }

        public override string ConnectionString
        {
            get
            {
                UpdateConnectionString();
                return this._connectionString;
            }
            set
            {
                this._connectionString = value;
            }
        }

        private void UpdateConnectionString()
        {
            this._connectionString = this._OGRConnectionString;
        }

        public override Microsoft.SqlServer.Dts.Runtime.DTSExecResult Validate(IDTSInfoEvents infoEvents)
        {
            if (string.IsNullOrEmpty(this._OGRConnectionString))
            {
                infoEvents.FireError(0, "OGR Connection Manager", "No connection information specified", string.Empty, 0);
                return DTSExecResult.Failure;
            }

            return DTSExecResult.Success;
        }

        public override object AcquireConnection(object txn)
        {
            UpdateConnectionString();
    
            // Set GDAL config options
            if (!(string.IsNullOrEmpty(this._OGRConfigOptions)))
            {
                string[] configPairs = this._OGRConfigOptions.Split(';');
                foreach (string configPair in configPairs)
                {
                    string[] config = configPair.Split('=');
                    Gdal.SetConfigOption(config[0], config[1]);
                }
            }

            Ogr.RegisterAll();
            DataSource OGRDataSource = Ogr.Open(this._connectionString, Convert.ToInt32(this._OpenForUpdate));
            return OGRDataSource;
        }

        public override void ReleaseConnection(object connection)
        {
            DataSource OGRDataSource = (DataSource)connection;
            if (OGRDataSource != null) { OGRDataSource.Dispose(); }
        }

    }
}
