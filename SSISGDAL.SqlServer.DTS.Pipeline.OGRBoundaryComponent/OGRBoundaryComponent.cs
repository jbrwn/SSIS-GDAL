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
        DisplayName = "OGR Boundary",
        ComponentType = ComponentType.Transform,
        IconResource = "SSISGDAL.SqlServer.DTS.Pipeline.SSISGDAL.ico"
    )]
    public class OGRBoundaryComponent : OGRTransformation
    {
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

        public override void transform(ref PipelineBuffer buffer, int defaultOutputId, int inputColumnBufferIndex, int outputColumnBufferIndex)
        {
            //Get OGR Geometry from buffer
            byte[] geomBytes = new byte[buffer.GetBlobLength(inputColumnBufferIndex)];
            geomBytes = buffer.GetBlobData(inputColumnBufferIndex, 0, geomBytes.Length);
            Geometry geom = Geometry.CreateFromWkb(geomBytes);

            geom = geom.Boundary();

            geomBytes = new byte[geom.WkbSize()];
            geom.ExportToWkb(geomBytes);

            buffer.ResetBlobData(inputColumnBufferIndex);
            buffer.AddBlobData(inputColumnBufferIndex, geomBytes);

            //Direct row to default output
            buffer.DirectRow(defaultOutputId);
        }
    }
}
