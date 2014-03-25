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
        DisplayName = "OGR Source",
        ComponentType =   ComponentType.SourceAdapter,
        IconResource = "SSISGDAL.SqlServer.DTS.Pipeline.SSISGDAL.ico"
    )]
    public class OGRSourceAdapter : OGRAdapter
    {
        private bool cancel;
        private int errorOutputID = -1;
        private int errorOutputIndex = -1;

        public override void ProvideComponentProperties()
        {
            ComponentMetaData.RuntimeConnectionCollection.RemoveAll();
            base.RemoveAllInputsOutputsAndCustomProperties();

            //Specify that the component has an error output
            ComponentMetaData.UsesDispositions = true;

            //Add layer property
            IDTSCustomProperty100 layer = ComponentMetaData.CustomPropertyCollection.New();
            layer.Name = "Layer";
            layer.Description = "Layer";

            //Add SQL statement property
            IDTSCustomProperty100 SQLStatement = ComponentMetaData.CustomPropertyCollection.New();
            SQLStatement.Name = "SQL Statement";
            SQLStatement.Description = "SQL Statement";

            //Add geoemtry column
            IDTSCustomProperty100 geomColumn = ComponentMetaData.CustomPropertyCollection.New();
            geomColumn.Name = "Geometry Column";
            geomColumn.Description = "Geometry Column";
            geomColumn.TypeConverter = "NOTBROWSABLE";

            //Add output 
            IDTSOutput100 output = ComponentMetaData.OutputCollection.New();
            output.Name = "output";
            output.ExternalMetadataColumnCollection.IsUsed = true;

            //Add error output
            IDTSOutput100 errorOutput = ComponentMetaData.OutputCollection.New();
            errorOutput.IsErrorOut = true;
            errorOutput.Name = "ErrorOutput";

            //Add connection
            IDTSRuntimeConnection100 OGRConnection = ComponentMetaData.RuntimeConnectionCollection.New();
            OGRConnection.Name = "OGR Connection";

        }

        public override DTSValidationStatus Validate() 
        {
            //Component should have 0 inputs
            if (ComponentMetaData.InputCollection.Count != 0)
            {
                ComponentMetaData.FireError(0, ComponentMetaData.Name, "Has an input when no input should exist.", "", 0, out this.cancel);
                return DTSValidationStatus.VS_ISCORRUPT;
            }

            //Component should have 1 default output and 1 error output
            if (ComponentMetaData.OutputCollection.Count != 2 || 
                (ComponentMetaData.OutputCollection.Count == 2 && ComponentMetaData.OutputCollection[0].IsErrorOut && ComponentMetaData.OutputCollection[1].IsErrorOut) ||
                (ComponentMetaData.OutputCollection.Count == 2 && (!ComponentMetaData.OutputCollection[0].IsErrorOut) && (!ComponentMetaData.OutputCollection[1].IsErrorOut)))
            {
                ComponentMetaData.FireError(0, ComponentMetaData.Name, "Component expects 1 default ouptut and 1 error output", "", 0, out this.cancel);
                return DTSValidationStatus.VS_ISCORRUPT;
            }
            
            //connection manager must be not be null
            if (ComponentMetaData.RuntimeConnectionCollection[0].ConnectionManager == null)
            {
                ComponentMetaData.FireError(0, ComponentMetaData.Name, "No OGR ConnectionManager specified.", "", 0, out this.cancel);
                return DTSValidationStatus.VS_ISBROKEN;
            }

            //if connected, make sure layer is valid
            if (this.isConnected)
            {
                try
                {
                    Layer OGRLayer;
                    if (ComponentMetaData.CustomPropertyCollection["SQL Statement"].Value == null || ComponentMetaData.CustomPropertyCollection["SQL Statement"].Value.ToString() == string.Empty)
                    {
                        getLayer();
                    }
                    else
                    {
                        OGRLayer = getSQLLayer();
                        this.OGRDataSource.ReleaseResultSet(OGRLayer);
                    }
                }
                catch (Exception ex)
                {
                    ComponentMetaData.FireError(0, ComponentMetaData.Name, ex.Message, "", 0, out this.cancel);
                    return DTSValidationStatus.VS_ISBROKEN;
                }
            }

            //refresh metadata if default output has no columns
            IDTSOutput100 defaultOutput = null;
            for (int i = 0; i < ComponentMetaData.OutputCollection.Count; i++)
            {
                if (!ComponentMetaData.OutputCollection[i].IsErrorOut)
                {
                    defaultOutput = ComponentMetaData.OutputCollection[i];
                }
            }
            if (defaultOutput.OutputColumnCollection.Count == 0)
            {
                return DTSValidationStatus.VS_NEEDSNEWMETADATA;
            }

            //check if metadata columns are valid
            if (ComponentMetaData.ValidateExternalMetadata)
            {
                //validate output columns
                if (!this.isMetadataValid())
                {
                    ComponentMetaData.FireWarning(0, ComponentMetaData.Name, "The output columns do not match the external data source.", "", 0);
                    return DTSValidationStatus.VS_NEEDSNEWMETADATA;
                }

                //validate external metadata columns
                if (!this.isExternalMetadataValid())
                {
                    ComponentMetaData.FireWarning(0, ComponentMetaData.Name, "Output columns do not match external metadata.", "", 0);
                    return DTSValidationStatus.VS_ISBROKEN;
                }
            }
            else
            {
                //validate external metadata columns
                if (!this.isExternalMetadataValid())
                {
                    //this.areExternalMetaDataColumnsValid = false;
                    ComponentMetaData.FireWarning(0, ComponentMetaData.Name, "Output columns do not match external metadata.", "", 0);
                    return DTSValidationStatus.VS_ISBROKEN;
                    
                }
            }

            return base.Validate();
        }
        
        public override void ReinitializeMetaData()
        {
            base.ReinitializeMetaData();

            //get default output
            IDTSOutput100 defaultOutput = null;
            this.GetErrorOutputInfo(ref errorOutputID, ref errorOutputIndex);
            foreach (IDTSOutput100 output in ComponentMetaData.OutputCollection)
            {
                if (output.ID != errorOutputID)
                    defaultOutput = output;
            }

            if (this.isConnected)
            {
                defaultOutput.OutputColumnCollection.RemoveAll();
                defaultOutput.ExternalMetadataColumnCollection.RemoveAll();

                //get ogrlayer and layer definition
                Layer OGRLayer;
                bool isSQLLayer = (!(ComponentMetaData.CustomPropertyCollection["SQL Statement"].Value == null || ComponentMetaData.CustomPropertyCollection["SQL Statement"].Value.ToString() == string.Empty));
                if (isSQLLayer)
                {
                    OGRLayer = getSQLLayer();
                }
                else
                {
                    OGRLayer = getLayer();
                }
                FeatureDefn OGRFeatureDef = OGRLayer.GetLayerDefn();

                //for each field in ogrlayer add output column and external metadata
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

                    //create column metadata
                    IDTSOutputColumn100 col = defaultOutput.OutputColumnCollection.New();
                    col.Name = OGRFieldDef.GetName();
                    col.ErrorRowDisposition = DTSRowDisposition.RD_FailComponent;
                    col.TruncationRowDisposition = DTSRowDisposition.RD_FailComponent;
                    col.SetDataTypeProperties(BufferDataType, length, precision, scale, codepage);

                    //create external metadata
                    IDTSExternalMetadataColumn100 ecol = defaultOutput.ExternalMetadataColumnCollection.New();
                    ecol.Name = col.Name;
                    ecol.DataType = col.DataType;
                    ecol.Precision = col.Precision;
                    ecol.Length = col.Length;
                    ecol.Scale = col.Scale;
                    ecol.CodePage = col.CodePage;

                    col.ExternalMetadataColumnID = ecol.ID;

                    i++;
                }
                
                //get geometry column
                string geomtryColumn = (OGRLayer.GetGeometryColumn() != "") ? OGRLayer.GetGeometryColumn() : "GEOMETRY";

                //add geom output column
                IDTSOutputColumn100 geomCol = defaultOutput.OutputColumnCollection.New();
                geomCol.Name = geomtryColumn;
                geomCol.ErrorRowDisposition = DTSRowDisposition.RD_FailComponent;
                geomCol.TruncationRowDisposition = DTSRowDisposition.RD_FailComponent;
                geomCol.SetDataTypeProperties(DataType.DT_IMAGE, 0, 0, 0, 0);

                //add geom external metadata
                IDTSExternalMetadataColumn100 egeomCol = defaultOutput.ExternalMetadataColumnCollection.New();
                egeomCol.Name = geomCol.Name;
                egeomCol.DataType = geomCol.DataType;
                egeomCol.Precision = geomCol.Precision;
                egeomCol.Length = geomCol.Length;
                egeomCol.Scale = geomCol.Scale;
                egeomCol.CodePage = geomCol.CodePage;

                //map column metadata to external column metadata
                geomCol.ExternalMetadataColumnID = egeomCol.ID;

                //set geometry column custom property
                ComponentMetaData.CustomPropertyCollection["Geometry Column"].Value = geomtryColumn;

                if (isSQLLayer)
                {
                    this.OGRDataSource.ReleaseResultSet(OGRLayer);
                }
            }
        }

        public override void PreExecute()
        {
            //get default output
            IDTSOutput100 defaultOutput = null;
            this.GetErrorOutputInfo(ref errorOutputID, ref errorOutputIndex);
            foreach (IDTSOutput100 output in ComponentMetaData.OutputCollection)
            {
                if (output.ID != errorOutputID)
                    defaultOutput = output;
            }

            //for each output column add columnInfo object to columnInformation arrayList
            foreach (IDTSOutputColumn100 col in defaultOutput.OutputColumnCollection)
            {
                columnInfo ci = new columnInfo();
                ci.bufferColumnIndex = BufferManager.FindColumnByLineageID(defaultOutput.Buffer, col.LineageID);
                ci.lineageID = col.LineageID;
                ci.columnName = col.Name;
                ci.errorDisposition = col.ErrorRowDisposition;
                ci.truncationDisposition = col.TruncationRowDisposition;
                if (col.Name == (string)ComponentMetaData.CustomPropertyCollection["Geometry Column"].Value)
                {
                    ci.geom = true;
                }
                else
                {
                    ci.geom = false;
                }
                this.columnInformation.Add(ci);
            }

        }

        public override void PrimeOutput(int outputs, int[] outputIDs, PipelineBuffer[] buffers)
        {
            //identify buffers
            PipelineBuffer errorBuffer = null;
            PipelineBuffer defaultBuffer = null;

            for (int x = 0; x < outputs; x++)
            {
                if (outputIDs[x] == errorOutputID)
                    errorBuffer = buffers[x];
                else
                    defaultBuffer = buffers[x];
            }

            //get ogrlayer and ogrlayer feature definition
            Layer OGRLayer;
            bool isSQLLayer = (!(ComponentMetaData.CustomPropertyCollection["SQL Statement"].Value == null || ComponentMetaData.CustomPropertyCollection["SQL Statement"].Value.ToString() == string.Empty));
            if (isSQLLayer)
            {
                OGRLayer = getSQLLayer();
            }
            else
            {
                OGRLayer = getLayer();
            }
            Feature OGRFeature;
            FeatureDefn OGRFeatureDef = OGRLayer.GetLayerDefn();

            //initialize columnInfo object
            columnInfo ci = new columnInfo();

            //for each row in ogrlayer add row to output buffer
            while ((OGRFeature = OGRLayer.GetNextFeature()) != null)
            {
                try
                {
                    defaultBuffer.AddRow();

                    //set buffer column values
                    for (int i = 0; i < this.columnInformation.Count; i++)
                    {
                        ci = (columnInfo)this.columnInformation[i];
                        
                        if (ci.geom)
                        {
                            Geometry geom = OGRFeature.GetGeometryRef();
                            if (geom != null)
                            {
                                byte[] geomBytes = new byte[geom.WkbSize()];
                                geom.ExportToWkb(geomBytes);
                                defaultBuffer.AddBlobData(ci.bufferColumnIndex, geomBytes);
                            }
                        }
                        else
                        {
                            int OGRFieldIndex = OGRFeatureDef.GetFieldIndex(ci.columnName);
                            FieldDefn OGRFieldDef = OGRFeatureDef.GetFieldDefn(OGRFieldIndex);
                            FieldType OGRFieldType = OGRFieldDef.GetFieldType();
                            
                            //declare datetime variables
                            int pnYear, pnMonth, pnDay, pnHour, pnMinute, pnSecond, pnTZFlag;
                            DateTime dt;
                            TimeSpan ts;

                            switch (OGRFieldType)
                            {
                                //case FieldType.OFTBinary:
                                //    break;
                                case FieldType.OFTDate:
                                    OGRFeature.GetFieldAsDateTime(OGRFieldIndex, out pnYear, out pnMonth, out pnDay, out pnHour, out pnMinute, out pnSecond, out pnTZFlag);
                                    dt = new DateTime(pnYear,pnMonth,pnDay);
                                    defaultBuffer.SetDate(ci.bufferColumnIndex, dt);
                                    break;
                                case FieldType.OFTDateTime:
                                    OGRFeature.GetFieldAsDateTime(OGRFieldIndex, out pnYear, out pnMonth, out pnDay, out pnHour, out pnMinute, out pnSecond, out pnTZFlag);
                                    dt = new DateTime(pnYear,pnMonth,pnDay,pnHour,pnMinute,pnSecond);
                                    //set time zone?
                                    defaultBuffer.SetDateTime(ci.bufferColumnIndex, dt);
                                    break;
                                case FieldType.OFTInteger:
                                    defaultBuffer.SetInt32(ci.bufferColumnIndex, OGRFeature.GetFieldAsInteger(OGRFieldIndex));
                                    break;
                                case FieldType.OFTReal:
                                    defaultBuffer.SetDouble(ci.bufferColumnIndex, OGRFeature.GetFieldAsDouble(OGRFieldIndex));
                                    break;
                                case FieldType.OFTTime:
                                    OGRFeature.GetFieldAsDateTime(OGRFieldIndex, out pnYear, out pnMonth, out pnDay, out pnHour, out pnMinute, out pnSecond, out pnTZFlag);
                                    ts = new TimeSpan(pnHour,pnMinute,pnSecond);
                                    defaultBuffer.SetTime(ci.bufferColumnIndex, ts);
                                    break;
                                case FieldType.OFTString:
                                default:
                                    defaultBuffer.SetString(ci.bufferColumnIndex, OGRFeature.GetFieldAsString(OGRFieldIndex));
                                    break;
                            }
                        }
                    }
                    
                }
                catch (Exception ex)
                {
                    //redirect to error buffer
                    if (ci.errorDisposition == DTSRowDisposition.RD_RedirectRow)
                    {
                        // Add a row to the error buffer.
                        errorBuffer.AddRow();

                        // Set the error information.
                        int errorCode = System.Runtime.InteropServices.Marshal.GetHRForException(ex);
                        errorBuffer.SetErrorInfo(errorOutputID, errorCode, ci.lineageID);

                        // Remove the row that was added to the default buffer.
                        defaultBuffer.RemoveRow();
                    }
                    //fail component 
                    else if (ci.errorDisposition == DTSRowDisposition.RD_FailComponent || ci.errorDisposition == DTSRowDisposition.RD_NotUsed)
                    {
                        ComponentMetaData.FireError(0, "primeoutput failure", ex.ToString(), string.Empty, 0, out cancel);
                        throw;
                    }
                }
            }
            //set end of rowset for buffers
            if (defaultBuffer != null)
                defaultBuffer.SetEndOfRowset();

            if (errorBuffer != null)
                errorBuffer.SetEndOfRowset();

            //clean up layer object
            if (isSQLLayer)
            {
                this.OGRDataSource.ReleaseResultSet(OGRLayer);
            }
        }

        //Checks if output metadata columns match ogrlayer source columns
        public override bool isMetadataValid()
        {
            //get default output
            IDTSOutput100 output = null;
            for (int i = 0; i < ComponentMetaData.OutputCollection.Count; i++)
            {
                if (!ComponentMetaData.OutputCollection[i].IsErrorOut)
                {
                    output = ComponentMetaData.OutputCollection[i];
                }
            }

            //get ogrlayer and layer definition
            Layer OGRLayer;
            bool isSQLLayer = (!(ComponentMetaData.CustomPropertyCollection["SQL Statement"].Value == null || ComponentMetaData.CustomPropertyCollection["SQL Statement"].Value.ToString() == string.Empty));
            if (isSQLLayer)
            {
                OGRLayer = getSQLLayer();
            }
            else
            {
                OGRLayer = getLayer();
            }

            FeatureDefn OGRFeatureDef = OGRLayer.GetLayerDefn();

            // Get Geometry column name
            string geomtryColumn = (OGRLayer.GetGeometryColumn() != "") ? OGRLayer.GetGeometryColumn() : "GEOMETRY";

            // Begin checks
            bool isValid = true;

            if (OGRFeatureDef.GetFieldCount() + 1 == output.OutputColumnCollection.Count)
            {
                int i = 0;
                do
                {
                    //check if ogr field exists by name
                    IDTSOutputColumn100 col = output.OutputColumnCollection[i];
                    int OGRFieldIndex = OGRFeatureDef.GetFieldIndex(col.Name);
                    if (OGRFieldIndex == -1)
                    {
                        //set isValid false if not geom column
                        if ((col.Name != geomtryColumn)
                            || (col.DataType != DataType.DT_IMAGE)
                            || ((string)ComponentMetaData.CustomPropertyCollection["Geometry Column"].Value != geomtryColumn))
                        {
                            isValid = false;
                        }
                    }
                    else
                    {
                        //is isVaild to false if field types don't match
                        FieldDefn OGRFieldDef = OGRFeatureDef.GetFieldDefn(OGRFieldIndex);
                        FieldType OGRFieldType = OGRFieldDef.GetFieldType();
                        if ((col.DataType != this.OGRTypeToBufferType(OGRFieldType))
                            && (!(OGRFieldType == FieldType.OFTString && col.DataType == DataType.DT_NTEXT))) // special case where lenth not provided by OFTString
                        {
                            isValid = false;
                        }

                    }
                    i++;
                } while (isValid && i < output.OutputColumnCollection.Count);
            }
            else
            {
                isValid = false;
            }

            //Manualy call release on OGR layer sql result set if needed
            if (isSQLLayer)
            {
                this.OGRDataSource.ReleaseResultSet(OGRLayer);
            }
            return isValid;
        }

        //checks if output metadata matches external output metadata
        public override bool isExternalMetadataValid()
        {
            //get default output
            IDTSOutput100 output = null;
            for (int i = 0; i < ComponentMetaData.OutputCollection.Count; i++)
            {
                if (!ComponentMetaData.OutputCollection[i].IsErrorOut)
                {
                    output = ComponentMetaData.OutputCollection[i];
                }
            }

            //get default output external metadata
            IDTSExternalMetadataColumnCollection100 externalMetaData = output.ExternalMetadataColumnCollection;

            //return false if output column metadata does not match external column metadata
            foreach (IDTSOutputColumn100 col in output.OutputColumnCollection)
            {
                IDTSExternalMetadataColumn100 ecol = externalMetaData.GetObjectByID(col.ExternalMetadataColumnID); 
                
                if (col.DataType != ecol.DataType
                    || col.Precision != ecol.Precision
                    || col.Length != ecol.Length
                    || col.Scale != ecol.Scale)
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

        public Layer getSQLLayer()
        {
            Layer layer;
            string SQLStatement = ComponentMetaData.CustomPropertyCollection["SQL Statement"].Value.ToString();
            layer = this.OGRDataSource.ExecuteSQL(SQLStatement, null, null);

            return layer;
        }
    }
}
