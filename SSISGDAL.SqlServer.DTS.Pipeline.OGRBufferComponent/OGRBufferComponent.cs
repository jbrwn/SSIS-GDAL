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
        DisplayName = "OGR Buffer",
        ComponentType = ComponentType.Transform,
        IconResource = "SSISGDAL.SqlServer.DTS.Pipeline.SSISGDAL.ico"
    )]
    public class OGRBufferComponent : OGRTransformation
    {
        private double bufferSize;
        private int quadSegs;

        public override void ProvideComponentProperties()
        {
            base.ProvideComponentProperties();
            
            //Add input buffer property
            IDTSCustomProperty100 Buffer = ComponentMetaData.CustomPropertyCollection.New();
            Buffer.Name = "Buffer";
            Buffer.Description = "Buffer";
            Buffer.Value = 0.0;

            //Add input quadsegs property
            IDTSCustomProperty100 quadSegmants = ComponentMetaData.CustomPropertyCollection.New();
            quadSegmants.Name = "Quadrant Segments";
            quadSegmants.Description = "Quadrant Segments";
            quadSegmants.Value = 30;
        }

        public override DTSValidationStatus Validate()
        {
            DTSValidationStatus validation = base.Validate();

            if (validation == DTSValidationStatus.VS_ISVALID)
            {
                bool cancel;

                //Validate input column
                if (ComponentMetaData.InputCollection[0].InputColumnCollection[0].DataType != DataType.DT_IMAGE)
                {
                    ComponentMetaData.FireError(0, ComponentMetaData.Name, "Invalid input column data type", string.Empty, 0, out cancel);
                    return DTSValidationStatus.VS_ISBROKEN;
                }
            }
            return validation;
        }

        public override void PreExecute()
        {
            base.PreExecute();

            this.bufferSize = (double)ComponentMetaData.CustomPropertyCollection["Buffer"].Value;
            this.quadSegs = (int)ComponentMetaData.CustomPropertyCollection["Quadrant Segments"].Value;
        }

        public override void transform(ref PipelineBuffer buffer, int defaultOutputId, int inputColumnBufferIndex, int outputColumnBufferIndex)
        {
            //Get OGR Geometry from buffer
            byte[] geomBytes = new byte[buffer.GetBlobLength(inputColumnBufferIndex)];
            geomBytes = buffer.GetBlobData(inputColumnBufferIndex, 0, geomBytes.Length);
            Geometry geom = Geometry.CreateFromWkb(geomBytes);

            geom = geom.Buffer(this.bufferSize, this.quadSegs);

            geomBytes = new byte[geom.WkbSize()];
            geom.ExportToWkb(geomBytes);

            buffer.ResetBlobData(inputColumnBufferIndex);
            buffer.AddBlobData(inputColumnBufferIndex, geomBytes);

            //Direct row to default output
            buffer.DirectRow(defaultOutputId);
        }
    }
}
