using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.Dts.Pipeline;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;
using Microsoft.SqlServer.Dts.Runtime;
using OSGeo.OGR;
using OSGeo.OSR;
using SSISGDAL.SqlServer.DTS.SSISGDAL;

namespace SSISGDAL.SqlServer.DTS.Pipeline
{
    [DtsPipelineComponent(
        DisplayName = "OGR Reproject",
        ComponentType = ComponentType.Transform,
        IconResource = "SSISGDAL.SqlServer.DTS.Pipeline.SSISGDAL.ico"
    )]
    public class OGRReprojectComponent : OGRTransformation
    {
        private CoordinateTransformation ct;

        public override void ProvideComponentProperties()
        {
            base.ProvideComponentProperties();

            //Add input projection property
            IDTSCustomProperty100 inputSr = ComponentMetaData.CustomPropertyCollection.New();
            inputSr.Name = "Input Spatial Reference";
            inputSr.Description = "Input Spatail Reference";

            //Add output projection property
            IDTSCustomProperty100 outputSr = ComponentMetaData.CustomPropertyCollection.New();
            outputSr.Name = "Output Spatial Reference";
            outputSr.Description = "Output Spatail Reference";
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

                //Validate input projection
                if (ComponentMetaData.CustomPropertyCollection["Input Spatial Reference"].Value == null || ComponentMetaData.CustomPropertyCollection["Input Spatial Reference"].Value.ToString() == string.Empty)
                {
                    ComponentMetaData.FireError(0, ComponentMetaData.Name, "Input projection is empty", "", 0, out cancel);
                    return DTSValidationStatus.VS_ISBROKEN;
                }
                string srInput = ComponentMetaData.CustomPropertyCollection["Input Spatial Reference"].Value.ToString();
                SpatialReference inSr = new SpatialReference("");
                if (inSr.SetFromUserInput(srInput) != 0)
                {
                    ComponentMetaData.FireError(0, ComponentMetaData.Name, "Invalid input projection", "", 0, out cancel);
                    return DTSValidationStatus.VS_ISBROKEN;
                }

                //Validate output projection
                if (ComponentMetaData.CustomPropertyCollection["Output Spatial Reference"].Value == null || ComponentMetaData.CustomPropertyCollection["Output Spatial Reference"].Value.ToString() == string.Empty)
                {
                    ComponentMetaData.FireError(0, ComponentMetaData.Name, "Output projection is empty", "", 0, out cancel);
                    return DTSValidationStatus.VS_ISBROKEN;
                }
                string srOutput = ComponentMetaData.CustomPropertyCollection["Output Spatial Reference"].Value.ToString();
                SpatialReference outSr = new SpatialReference("");
                if (outSr.SetFromUserInput(srOutput) != 0)
                {
                    ComponentMetaData.FireError(0, ComponentMetaData.Name, "Invalid output projection", "", 0, out cancel);
                    return DTSValidationStatus.VS_ISBROKEN;
                }
            }
            return validation;
        }

        public override void PreExecute()
        {
            base.PreExecute();

            //set input spatail reference
            string srInput = ComponentMetaData.CustomPropertyCollection["Input Spatial Reference"].Value.ToString();
            SpatialReference inSr = new SpatialReference("");
            inSr.SetFromUserInput(srInput);

            //Set output spatial reference
            string srOutput = ComponentMetaData.CustomPropertyCollection["Output Spatial Reference"].Value.ToString();
            SpatialReference outSr = new SpatialReference("");
            outSr.SetFromUserInput(srOutput);

            //cache coordinate transformation
            this.ct = new CoordinateTransformation(inSr, outSr);
        }

        public override void transform(ref PipelineBuffer buffer, int defaultOutputId, int inputColumnBufferIndex, int outputColumnBufferIndex)
        {
            //Get OGR Geometry from buffer
            byte[] geomBytes = new byte[buffer.GetBlobLength(inputColumnBufferIndex)];
            geomBytes = buffer.GetBlobData(inputColumnBufferIndex, 0, geomBytes.Length);
            Geometry geom = Geometry.CreateFromWkb(geomBytes);

            geom.Transform(this.ct);

            geomBytes = new byte[geom.WkbSize()];
            geom.ExportToWkb(geomBytes);

            buffer.ResetBlobData(inputColumnBufferIndex);
            buffer.AddBlobData(inputColumnBufferIndex, geomBytes);

            //Direct row to default output
            buffer.DirectRow(defaultOutputId);
        }
    }
}
