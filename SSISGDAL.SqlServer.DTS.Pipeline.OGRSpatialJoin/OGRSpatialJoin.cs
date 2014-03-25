using System;
using System.Collections;
using System.Collections.Generic;
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
        DisplayName = "OGR Spatial Join",
        ComponentType = ComponentType.Transform,
        IconResource = "SSISGDAL.SqlServer.DTS.Pipeline.SSISGDAL.ico"
    )]
    public class OGRSpatialJoin : PipelineComponent
    {
        private bool cancel;
        private int _targetID;
        private int _joinID;
        private int _targetGeomIndex;
        private int _joinGeomIndex;
        private List<columnInfoMap> _targetColumnInfoMapList;
        private List<columnInfoMap> _joinColumnInfoMapList;
        private OGRBufferCache _targetCache;
        private OGRBufferCache _joinCache;
        private int _inputCount = 0;
        private PipelineBuffer _outputBuffer;
        private relationType _relation;
        private enum relationType { intersects, equals, touches, crosses, within, contains, overlaps };
            
        public override void ProvideComponentProperties()
        {
            //Add input 1 geometry column property
            IDTSCustomProperty100 input1GeomColumn = ComponentMetaData.CustomPropertyCollection.New();
            input1GeomColumn.Name = "Target Input Geometry Column";
            input1GeomColumn.Description = "Target Input Geometry Column";
            input1GeomColumn.Value = "OGRGeometry";

            //Add input 2 geometry column property
            IDTSCustomProperty100 input2GeomColumn = ComponentMetaData.CustomPropertyCollection.New();
            input2GeomColumn.Name = "Join Input Geometry Column";
            input2GeomColumn.Description = "Join Input Geometry Column";
            input2GeomColumn.Value = "OGRGeometry";

            // spatial relation
            IDTSCustomProperty100 spatialRelation = ComponentMetaData.CustomPropertyCollection.New();
            spatialRelation.Name = "Spatial Relation";
            spatialRelation.Description = "Spatial Relation";
            spatialRelation.TypeConverter = typeof(relationType).AssemblyQualifiedName;
            spatialRelation.Value = relationType.intersects;

            //Add input1
            IDTSInput100 input1 = ComponentMetaData.InputCollection.New();
            input1.Name = "Target Input";

            //Add input2
            IDTSInput100 input2 = ComponentMetaData.InputCollection.New();
            input2.Name = "Join Input";

            //Add the output
            IDTSOutput100 output = ComponentMetaData.OutputCollection.New();
            output.Name = "Output";
            output.SynchronousInputID = 0;
        }

        public override void OnInputPathAttached(int inputID)
        {
            IDTSOutput100 defaultOutput = ComponentMetaData.OutputCollection[0];
            IDTSInput100 input = ComponentMetaData.InputCollection.GetObjectByID(inputID);
            IDTSVirtualInput100 vInput = input.GetVirtualInput();

            foreach (IDTSVirtualInputColumn100 vCol in vInput.VirtualInputColumnCollection)
            {
                IDTSOutputColumn100 outCol = defaultOutput.OutputColumnCollection.New();
                outCol.Name = input.Name + "." + vCol.Name;
                outCol.SetDataTypeProperties(vCol.DataType, vCol.Length, vCol.Precision, vCol.Scale, vCol.CodePage);
                SetUsageType(inputID, vInput, vCol.LineageID, DTSUsageType.UT_READONLY);
            }
        }

        public override void OnInputPathDetached(int inputID)
        {
            IDTSOutput100 defaultOutput = ComponentMetaData.OutputCollection[0];
            IDTSInput100 input = ComponentMetaData.InputCollection.GetObjectByID(inputID);
            IDTSVirtualInput100 vInput = input.GetVirtualInput();
            
            foreach (IDTSVirtualInputColumn100 vCol in vInput.VirtualInputColumnCollection)
            {
                defaultOutput.OutputColumnCollection.RemoveObjectByIndex(input.Name + "." + vCol.Name);
            }
            input.InputColumnCollection.RemoveAll();
        }

        public override DTSValidationStatus Validate()
        {
            //Must have 2 input
            if (ComponentMetaData.InputCollection.Count != 2)
            {
                this.ComponentMetaData.FireError(0, ComponentMetaData.Name, "Two inputs required", string.Empty, 0, out cancel);
                return DTSValidationStatus.VS_ISCORRUPT;
            }

            //Must have only 1 output
            if (ComponentMetaData.OutputCollection.Count != 1)
            {
                ComponentMetaData.FireError(0, ComponentMetaData.Name, "One output required", "", 0, out this.cancel);
                return DTSValidationStatus.VS_ISCORRUPT;
            }
            
            return base.Validate();
        }

        public override void PreExecute()
        {
            this._relation = (relationType) ComponentMetaData.CustomPropertyCollection["Spatial Relation"].Value;
            this._targetCache = new OGRBufferCache();
            this._joinCache = new OGRBufferCache();
            this._targetColumnInfoMapList = new List<columnInfoMap>();
            this._joinColumnInfoMapList = new List<columnInfoMap>();
            this._targetID = ComponentMetaData.InputCollection["Target Input"].ID;
            this._joinID = ComponentMetaData.InputCollection["Join Input"].ID;

            IDTSOutput100 defaultOutput = ComponentMetaData.OutputCollection[0];
            IDTSInput100 targetInput = ComponentMetaData.InputCollection["Target Input"];
            foreach (IDTSInputColumn100 inputColumn in targetInput.InputColumnCollection)
            {
                if (((string)ComponentMetaData.CustomPropertyCollection["Target Input Geometry Column"].Value).Equals(inputColumn.Name))
                {
                    this._targetGeomIndex = BufferManager.FindColumnByLineageID(targetInput.Buffer, inputColumn.LineageID);
                }
                foreach (IDTSOutputColumn100 outputColumn in defaultOutput.OutputColumnCollection)
                {
                    if (outputColumn.Name.Equals(targetInput.Name + "." + inputColumn.Name))
                    {
                        columnInfoMap ci = new columnInfoMap();
                        ci.inputBufferIndex = BufferManager.FindColumnByLineageID(targetInput.Buffer, inputColumn.LineageID);
                        ci.outputBufferIndex = BufferManager.FindColumnByLineageID(defaultOutput.Buffer, outputColumn.LineageID);
                        this._targetColumnInfoMapList.Add(ci);
                    }
                }
            }

            IDTSInput100 joinInput = ComponentMetaData.InputCollection["Join Input"];
            foreach (IDTSInputColumn100 inputColumn in joinInput.InputColumnCollection)
            {
                if (((string)ComponentMetaData.CustomPropertyCollection["Join Input Geometry Column"].Value).Equals(inputColumn.Name))
                {
                    this._joinGeomIndex = BufferManager.FindColumnByLineageID(joinInput.Buffer, inputColumn.LineageID);
                }
                foreach (IDTSOutputColumn100 outputColumn in defaultOutput.OutputColumnCollection)
                {
                    if (outputColumn.Name.Equals(joinInput.Name + "." + inputColumn.Name))
                    {
                        columnInfoMap ci = new columnInfoMap();
                        ci.inputBufferIndex = BufferManager.FindColumnByLineageID(joinInput.Buffer, inputColumn.LineageID);
                        ci.outputBufferIndex = BufferManager.FindColumnByLineageID(defaultOutput.Buffer, outputColumn.LineageID);
                        this._joinColumnInfoMapList.Add(ci);
                    }
                }
            }
        }

        public override void ProcessInput(int inputID, PipelineBuffer buffer)
        {
            OGRBufferCache cache = null;
            int geomIndex = -1;
            
            if (inputID == this._targetID) 
            { 
                cache = this._targetCache;
                geomIndex = this._targetGeomIndex;
 
            }
            else if (inputID == this._joinID)
            { 
                cache = this._joinCache; 
                geomIndex = this._joinGeomIndex;
            }

            while (buffer.NextRow())
            {
                object[] bufferRow = new object[buffer.ColumnCount];
                for (int i = 0; i < buffer.ColumnCount; i++)
                {
                    if (buffer[i] is BlobColumn)
                    {
                        int blobSize = (int)buffer.GetBlobLength(i);
                        byte[] blob = buffer.GetBlobData(i, 0, blobSize);
                        bufferRow[i] = blob;
                    }
                    else
                    {
                        bufferRow[i] = buffer[i];
                    }

                }
                Geometry geom = Geometry.CreateFromWkb((byte[])bufferRow[geomIndex]);
                OGRBufferCacheRow row = new OGRBufferCacheRow(bufferRow, geom);
                cache.add(row);
            }

            if (buffer.EndOfRowset) { this._inputCount += 1; }

            if (this._inputCount == 2)
            {
                this._targetCache.createSpatialIndex();

                foreach (OGRBufferCacheRow row in this._joinCache)
                {
                    List<OGRBufferCacheRow> results = null;
                    switch (this._relation)
                    {
                        case relationType.contains:
                            results = this._targetCache.contains(row);
                            break;
                        case relationType.crosses:
                            results = this._targetCache.crosses(row);
                            break;
                        case relationType.equals:
                            results = this._targetCache.equals(row);
                            break;
                        case relationType.intersects:
                            results = this._targetCache.intersects(row);
                            break;
                        case relationType.overlaps:
                            results = this._targetCache.overlaps(row);
                            break;
                        case relationType.touches:
                            results = this._targetCache.touches(row);
                            break;
                        case relationType.within:
                            results = this._targetCache.within(row);
                            break;
                    }

                    if (results.Count > 0)
                    {
                        foreach (OGRBufferCacheRow resultRow in results)
                        {
                            this._outputBuffer.AddRow();

                            foreach (columnInfoMap ci in this._joinColumnInfoMapList)
                            {
                                this._outputBuffer[ci.outputBufferIndex] = row[ci.inputBufferIndex];
                            }
                            foreach (columnInfoMap ci in this._targetColumnInfoMapList)
                            {
                                this._outputBuffer[ci.outputBufferIndex] = resultRow[ci.inputBufferIndex];
                            }
                        }
                    }
                }
                this._outputBuffer.SetEndOfRowset();
            }
        }

        public override void PrimeOutput(int outputs, int[] outputIDs, PipelineBuffer[] buffers)
        {
            this._outputBuffer = buffers[0];
        }
    }
}
