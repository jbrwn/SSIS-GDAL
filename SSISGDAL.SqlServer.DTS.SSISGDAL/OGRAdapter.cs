using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.Dts.Pipeline;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;
using Microsoft.SqlServer.Dts.Runtime;
using OSGeo.OGR;

namespace SSISGDAL.SqlServer.DTS.SSISGDAL
{
    public abstract class OGRAdapter : PipelineComponent
    {
        public DataSource OGRDataSource = null;
        public bool isConnected;
        public List<columnInfo> columnInformation = new List<columnInfo>();

        public struct columnInfo
        {
            public int bufferColumnIndex;
            public string columnName;
            public int lineageID;
            public bool geom;
            public DTSRowDisposition errorDisposition;
            public DTSRowDisposition truncationDisposition;
        }

        public override void AcquireConnections(object transaction)
        {
            if (ComponentMetaData.RuntimeConnectionCollection[0].ConnectionManager != null)
            {
                //get runtime connection manager
                ConnectionManager cm = Microsoft.SqlServer.Dts.Runtime.DtsConvert.GetWrapper(ComponentMetaData.RuntimeConnectionCollection[0].ConnectionManager);
                this.OGRDataSource = cm.AcquireConnection(transaction) as DataSource;

                if (this.OGRDataSource == null)
                {
                    throw new Exception("The connection manager did not provide a valid OGR datasource");
                }
                this.isConnected = true;
            }
        }

        public override void ReleaseConnections()
        {
            if (this.OGRDataSource != null)
            {
                this.OGRDataSource.Dispose();
                this.OGRDataSource = null;
            }
            this.isConnected = false;
        }

        public DataType OGRTypeToBufferType(FieldType OGRFieldType)
        {
            DataType BufferDataType;

            switch (OGRFieldType)
            {
                //case FieldType.OFTBinary:
                //    BufferDataType = DataType.DT_IMAGE;
                //    break;
                case FieldType.OFTDate:
                    BufferDataType = DataType.DT_DBDATE;
                    break;
                case FieldType.OFTDateTime:
                    BufferDataType = DataType.DT_DBTIMESTAMP;
                    break;
                case FieldType.OFTInteger:
                    BufferDataType = DataType.DT_I4;
                    break;
                case FieldType.OFTReal:
                    BufferDataType = DataType.DT_R8;
                    break;
                case FieldType.OFTString:
                    BufferDataType = DataType.DT_WSTR;
                    break;
                case FieldType.OFTTime:
                    BufferDataType = DataType.DT_DBTIME;
                    break;
                default:
                    BufferDataType = DataType.DT_NTEXT;
                    break;
            }

            return BufferDataType;
        }

        public abstract bool isMetadataValid();

        public abstract bool isExternalMetadataValid();


    }
}
