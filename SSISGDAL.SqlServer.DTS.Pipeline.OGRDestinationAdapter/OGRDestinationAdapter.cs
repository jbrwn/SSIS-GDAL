using System;
using System.Collections;
using System.Text;
using Microsoft.SqlServer.Dts.Pipeline;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;
using Microsoft.SqlServer.Dts.Runtime;
using OSGeo.OGR;
using SSISGDAL.SqlServer.DTS.SSISGDAL;

namespace SSISGDAL.SqlServer.DTS.Pipeline
{
    [DtsPipelineComponent(
        DisplayName = "OGR Destination",
        ComponentType = ComponentType.DestinationAdapter,
        IconResource = "SSISGDAL.SqlServer.DTS.Pipeline.SSISGDAL.ico"
    )]
    public class OGRDestinationAdapter : OGRAdapter
    {
        private bool cancel;
        private bool validExternalMetadata = true;
        private int batchSize;

        public override void ProvideComponentProperties()
        {
            ComponentMetaData.RuntimeConnectionCollection.RemoveAll();
            base.RemoveAllInputsOutputsAndCustomProperties();

            //Specify that the component has an error output
            ComponentMetaData.UsesDispositions = true;

            IDTSCustomProperty100 layer = ComponentMetaData.CustomPropertyCollection.New();
            layer.Name = "Layer";
            layer.Description = "Layer";

            IDTSCustomProperty100 batchSize = ComponentMetaData.CustomPropertyCollection.New();
            batchSize.Name = "Batch Size";
            batchSize.Description = "Batch Size";
            batchSize.Value = 1000;

            //Add geoemtry column
            IDTSCustomProperty100 geomColumn = ComponentMetaData.CustomPropertyCollection.New();
            geomColumn.Name = "Geometry Column";
            geomColumn.Description = "Geometry Column";
            geomColumn.TypeConverter = "NOTBROWSABLE";
            
            IDTSInput100 input = ComponentMetaData.InputCollection.New();
            input.Name = "input";
            input.HasSideEffects = true;
            input.ExternalMetadataColumnCollection.IsUsed = true;

            //Add the error output
            IDTSOutput100 errorOutput = ComponentMetaData.OutputCollection.New();
            errorOutput.IsErrorOut = true;
            errorOutput.Name = "ErrorOutput";
            errorOutput.SynchronousInputID = input.ID;
            errorOutput.ExclusionGroup = 1;

            IDTSRuntimeConnection100 OGRConnection = ComponentMetaData.RuntimeConnectionCollection.New();
            OGRConnection.Name = "OGR Connection";
        }

        public override DTSValidationStatus Validate()
        {   
            // Make sure there is only 1 output and it is an error output.
            if (!(ComponentMetaData.OutputCollection.Count == 1 && ComponentMetaData.OutputCollection[0].IsErrorOut))
            {
                ComponentMetaData.FireError(0, ComponentMetaData.Name, "Has an output when no input should exist.", "", 0, out this.cancel);
                return DTSValidationStatus.VS_ISCORRUPT;
            }
            
            // Make sure there is one input.
            if (ComponentMetaData.InputCollection.Count != 1)
            {
                ComponentMetaData.FireError(0, ComponentMetaData.Name, "Should only have a single input.", "", 0, out this.cancel);
                return DTSValidationStatus.VS_ISCORRUPT;
            }
            
            //Must have connection manager
            if (ComponentMetaData.RuntimeConnectionCollection[0].ConnectionManager == null)
            {
                ComponentMetaData.FireError(0, ComponentMetaData.Name, "No OGR ConnectionManager specified.", "", 0, out this.cancel);
                return DTSValidationStatus.VS_ISBROKEN;
            }

            //Batch Size must be an integer
            if (ComponentMetaData.CustomPropertyCollection["Batch Size"].Value.GetType() != typeof(int))
            {
                ComponentMetaData.FireError(0, ComponentMetaData.Name, "Batch Size must be an integer", "", 0, out this.cancel);
                return DTSValidationStatus.VS_ISBROKEN;
            }

            //if connected, make sure layer is valid
            if (this.isConnected)
            {
                try
                {
                    getLayer();
                }
                catch (Exception ex)
                {
                    ComponentMetaData.FireError(0, ComponentMetaData.Name, ex.Message, "", 0, out this.cancel);
                    return DTSValidationStatus.VS_ISBROKEN;
                }
            }

            if (ComponentMetaData.InputCollection[0].ExternalMetadataColumnCollection.Count == 0)
            {
                this.validExternalMetadata = false;
                return DTSValidationStatus.VS_NEEDSNEWMETADATA;
            }

            if (ComponentMetaData.ValidateExternalMetadata)
            {
                if (!this.isExternalMetadataValid())
                {
                    this.validExternalMetadata = false;
                    ComponentMetaData.FireWarning(0, ComponentMetaData.Name, "The external metadata columns do not match the external data source.", "", 0);
                    return DTSValidationStatus.VS_NEEDSNEWMETADATA;
                }

                if (!this.isMetadataValid())
                {
                    ComponentMetaData.FireWarning(0, ComponentMetaData.Name, "Input columns do not match external metadata.", "", 0);
                    return DTSValidationStatus.VS_ISBROKEN;
                }

                //check upstream inputs
                if (!ComponentMetaData.AreInputColumnsValid)
                {
                    ComponentMetaData.FireWarning(0, ComponentMetaData.Name, "Input columns are invalid", "", 0);
                    return DTSValidationStatus.VS_NEEDSNEWMETADATA;
                }

            }
            else
            {
                if (!this.isMetadataValid())
                {
                    ComponentMetaData.FireWarning(0, ComponentMetaData.Name, "Input columns do not match external metadata.", "", 0);
                    return DTSValidationStatus.VS_ISBROKEN;
                }

                //check upstream inputs
                if (!ComponentMetaData.AreInputColumnsValid)
                {
                    ComponentMetaData.FireWarning(0, ComponentMetaData.Name, "Input columns are invalid", "", 0);
                    return DTSValidationStatus.VS_NEEDSNEWMETADATA;
                }
            }

            return base.Validate();
        }

        public override void ReinitializeMetaData()
        {
            base.ReinitializeMetaData();

            IDTSInput100 input = ComponentMetaData.InputCollection[0];

            if (!this.validExternalMetadata && this.isConnected)
            {
                input.ExternalMetadataColumnCollection.RemoveAll();
                input.InputColumnCollection.RemoveAll();
                this.validExternalMetadata = true;

                Layer OGRLayer = this.getLayer();
                FeatureDefn OGRFeatureDef = OGRLayer.GetLayerDefn();

                int i = 0;
                while (i < OGRFeatureDef.GetFieldCount())
                {
                    //map OGR field type to SSIS data type
                    FieldDefn OGRFieldDef = OGRFeatureDef.GetFieldDefn(i);
                    DataType BufferDataType = this.OGRTypeToBufferType(OGRFieldDef.GetFieldType());
                    int length = 0;
                    int precision = OGRFieldDef.GetWidth();
                    int scale = OGRFieldDef.GetPrecision();
                    int codepage = 1252;

                    switch (BufferDataType)
                    {
                        case DataType.DT_WSTR:
                            length = precision;
                            codepage = 0;
                            precision = 0;
                            scale = 0;
                            //check for length == 0
                            if (length == 0)
                            {
                                BufferDataType = DataType.DT_NTEXT;
                            }
                            break;
                        default:
                            length = 0;
                            precision = 0;
                            codepage = 0;
                            scale = 0;
                            break;
                    }

                    IDTSExternalMetadataColumn100 ecol = input.ExternalMetadataColumnCollection.New();
                    ecol.Name = OGRFieldDef.GetName();
                    ecol.DataType = BufferDataType;
                    ecol.Length = length;
                    ecol.Precision = precision;
                    ecol.Scale = scale;
                    ecol.CodePage = codepage;

                    i++;
                }

                //get geometry column
                string geomtryColumn = (OGRLayer.GetGeometryColumn() != "") ? OGRLayer.GetGeometryColumn() : "GEOMETRY";

                //Set OGRGeometry external metadata column    
                IDTSExternalMetadataColumn100 egeomCol = input.ExternalMetadataColumnCollection.New();

                egeomCol.Name = geomtryColumn;
                egeomCol.DataType = DataType.DT_IMAGE;
                egeomCol.Precision = 0;
                egeomCol.Length = 0;
                egeomCol.Scale = 0;
                egeomCol.CodePage = 0;

                //set geometry column custom property
                ComponentMetaData.CustomPropertyCollection["Geometry Column"].Value = geomtryColumn;
            }

            if (!ComponentMetaData.AreInputColumnsValid)
            {
                ComponentMetaData.RemoveInvalidInputColumns();
            }
        }

        //Override SetUsagetype to set ErrorRowDisposition and TruncationRowDisposition
        public override IDTSInputColumn100 SetUsageType(int inputID, IDTSVirtualInput100 virtualInput, int lineageID, DTSUsageType usageType)
        {
            IDTSInputColumn100 inputColumn = base.SetUsageType(inputID, virtualInput, lineageID, usageType);
            if (inputColumn != null)
            {
                inputColumn.ErrorRowDisposition = DTSRowDisposition.RD_FailComponent;
                inputColumn.TruncationRowDisposition = DTSRowDisposition.RD_FailComponent;
            }
            return inputColumn;
        }

        public override void PreExecute()
        {
            IDTSInput100 input = ComponentMetaData.InputCollection[0];

            foreach (IDTSInputColumn100 col in input.InputColumnCollection)
            {
                IDTSExternalMetadataColumn100 ecol = input.ExternalMetadataColumnCollection.GetObjectByID(col.ExternalMetadataColumnID);

                columnInfo ci = new columnInfo();
                ci.bufferColumnIndex = BufferManager.FindColumnByLineageID(input.Buffer, col.LineageID);
                ci.columnName = ecol.Name;
                ci.lineageID = col.LineageID;
                ci.errorDisposition = col.ErrorRowDisposition;
                ci.truncationDisposition = col.TruncationRowDisposition;
                if (ecol.Name == (string)ComponentMetaData.CustomPropertyCollection["Geometry Column"].Value)
                {
                    ci.geom = true;
                }
                else
                {
                    ci.geom = false;
                }
                this.columnInformation.Add(ci);
            }

            //set batchSize
            this.batchSize = (int)ComponentMetaData.CustomPropertyCollection["Batch Size"].Value;
        }

        public override void ProcessInput(int inputID, PipelineBuffer buffer)
        {
            Layer OGRLayer = this.getLayer();
            FeatureDefn OGRFeatureDef = OGRLayer.GetLayerDefn();
            int batchCount = 0;
            OGRLayer.StartTransaction();

            //initialize columnInfo object
            columnInfo ci = new columnInfo();
            
            while (buffer.NextRow())
            {   
                try
                {
                    //Start transaction
                    if (this.batchSize != 0 && batchCount % this.batchSize == 0)
                    {
                        OGRLayer.CommitTransaction();
                        OGRLayer.StartTransaction();
                        batchCount = 0;
                    }

                    Feature OGRFeature = new Feature(OGRFeatureDef);

                    for (int i = 0; i < this.columnInformation.Count; i++)
                    {
                        ci = this.columnInformation[i];

                        if (!buffer.IsNull(ci.bufferColumnIndex))
                        {
                            if (ci.geom)
                            {
                                byte[] geomBytes = new byte[buffer.GetBlobLength(ci.bufferColumnIndex)];
                                geomBytes = buffer.GetBlobData(ci.bufferColumnIndex, 0, geomBytes.Length);
                                Geometry geom = Geometry.CreateFromWkb(geomBytes);
                                OGRFeature.SetGeometry(geom);
                            }
                            else
                            {
                                int OGRFieldIndex = OGRFeatureDef.GetFieldIndex(ci.columnName);
                                FieldDefn OGRFieldDef = OGRFeatureDef.GetFieldDefn(OGRFieldIndex);
                                FieldType OGRFieldType = OGRFieldDef.GetFieldType();

                                //declare datetime variables
                                DateTime dt;
                                TimeSpan ts;

                                switch (OGRFieldType)
                                {
                                    //case FieldType.OFTBinary:
                                    //    break;
                                    case FieldType.OFTDate:
                                        dt = buffer.GetDate(ci.bufferColumnIndex);
                                        OGRFeature.SetField(OGRFieldIndex, dt.Year, dt.Month, dt.Day, 0, 0, 0, 0);
                                        break;
                                    case FieldType.OFTDateTime:
                                        dt = buffer.GetDateTime(ci.bufferColumnIndex);
                                        //get timezone?
                                        OGRFeature.SetField(OGRFieldIndex, dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second, 0);
                                        break;
                                    case FieldType.OFTInteger:
                                        OGRFeature.SetField(OGRFieldIndex, buffer.GetInt32(ci.bufferColumnIndex));
                                        break;
                                    case FieldType.OFTReal:
                                        OGRFeature.SetField(OGRFieldIndex, buffer.GetDouble(ci.bufferColumnIndex));
                                        break;
                                    case FieldType.OFTTime:
                                        ts = buffer.GetTime(ci.bufferColumnIndex);
                                        OGRFeature.SetField(OGRFieldIndex, 0, 0, 0, ts.Hours, ts.Minutes, ts.Seconds, 0);
                                        break;
                                    case FieldType.OFTString:
                                    default:
                                        OGRFeature.SetField(OGRFieldIndex, buffer.GetString(ci.bufferColumnIndex));
                                        break;
                                }
                            }
                        }
                    }

                    OGRLayer.CreateFeature(OGRFeature);
                    batchCount++;
                    //increment incrementPipelinePerfCounters to display correct # of rows written
                    ComponentMetaData.IncrementPipelinePerfCounter(103, 1);
                }                        
                catch (Exception ex)
                {
                    //Redirect row
                    IDTSInputColumn100 inputColumn = ComponentMetaData.InputCollection[0].InputColumnCollection.GetInputColumnByLineageID(ci.lineageID);
                    IDTSOutput100 output = ComponentMetaData.OutputCollection[0];

                    if (ci.errorDisposition == DTSRowDisposition.RD_RedirectRow)
                    {
                        int errorCode = System.Runtime.InteropServices.Marshal.GetHRForException(ex);
                        buffer.DirectErrorRow(output.ID, errorCode, inputColumn.LineageID);
                    }
                    else if (ci.errorDisposition == DTSRowDisposition.RD_FailComponent || ci.errorDisposition == DTSRowDisposition.RD_NotUsed)
                    {
                        OGRLayer.RollbackTransaction();
                        ComponentMetaData.FireError(0, ComponentMetaData.Name, ex.Message, string.Empty, 0, out cancel);
                        throw new Exception(ex.Message);
                    }
                }
            }
            OGRLayer.CommitTransaction();
        }

        public override bool  isExternalMetadataValid()
        {
            IDTSExternalMetadataColumnCollection100 exColumns = ComponentMetaData.InputCollection[0].ExternalMetadataColumnCollection;
            Layer OGRLayer = this.getLayer();
            FeatureDefn OGRFeatureDef = OGRLayer.GetLayerDefn();

            // Get Geometry column name
            string geomtryColumn = (OGRLayer.GetGeometryColumn() != "") ? OGRLayer.GetGeometryColumn() : "GEOMETRY";
                        
            //check for correct number of external metadata columns
            if (OGRFeatureDef.GetFieldCount() + 1 != exColumns.Count)
            {
                return false;
            }

            //validate each external metadata column
            for (int i = 0; i < exColumns.Count; i++)
            {
                IDTSExternalMetadataColumn100 col = exColumns[i];

                if (col.Name == geomtryColumn)
                {
                    if (col.DataType != DataType.DT_IMAGE)
                    {
                        return false;
                    }
                    // Check geometry column custom property against source
                    if ((string)ComponentMetaData.CustomPropertyCollection["Geometry Column"].Value != geomtryColumn)
                    {
                        return false;
                    }
                }
                else
                {
                    //check if ogr field exists by name
                    int OGRFieldIndex = OGRFeatureDef.GetFieldIndex(col.Name);
                    if (OGRFieldIndex == -1)
                    {
                        return false;
                    }

                    //check if ogr column matches output column type
                    FieldDefn OGRFieldDef = OGRFeatureDef.GetFieldDefn(OGRFieldIndex);
                    FieldType OGRFieldType = OGRFieldDef.GetFieldType();
                    if (this.OGRTypeToBufferType(OGRFieldType) != col.DataType)
                    {
                        //check for case where OFTString -> DT_NTEXT
                        if (!(OGRFieldType == FieldType.OFTString && col.DataType == DataType.DT_NTEXT))
                        {
                            return false;
                        }
                    }

                }
            }
            return true;
        }

        public override bool  isMetadataValid()
        {
            IDTSExternalMetadataColumnCollection100 externalMetaData = ComponentMetaData.InputCollection[0].ExternalMetadataColumnCollection;

            foreach (IDTSInputColumn100 col in ComponentMetaData.InputCollection[0].InputColumnCollection)
            {
                IDTSExternalMetadataColumn100 ecol = externalMetaData.GetObjectByID(col.ExternalMetadataColumnID);

                if (col.DataType != ecol.DataType
                    || col.Precision > ecol.Precision
                    || col.Length > ecol.Length
                    || col.Scale > ecol.Scale)
                {
                    return false;
                }
            }
            return true;
        }

        public Layer getLayer()
        {
            Layer layer;
            //if layer property is not set get ogrlayer at index 0
            if (ComponentMetaData.CustomPropertyCollection["Layer"].Value == null || ComponentMetaData.CustomPropertyCollection["Layer"].Value.ToString() == string.Empty)
            {
                layer = this.OGRDataSource.GetLayerByIndex(0);
            }
            //get named ogrlayer
            else
            {
                layer = this.OGRDataSource.GetLayerByName(ComponentMetaData.CustomPropertyCollection["Layer"].Value.ToString());
            }

            if (layer == null)
            {
                throw new Exception("OGR layer does not exist in datasource");
            }
            return layer;
        }
    }
}
