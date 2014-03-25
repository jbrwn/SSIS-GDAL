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
    public abstract class OGRTransformation : PipelineComponent
    {
        private int inputColumnBufferIndex;
        private int outputColumnBufferIndex;
        private bool cancel;

        public override void ProvideComponentProperties()
        {
            //Specify that the component has an error output
            ComponentMetaData.UsesDispositions = true;

            //Add the input
            IDTSInput100 input = ComponentMetaData.InputCollection.New();
            input.Name = "Input";

            //Add the output
            IDTSOutput100 output = ComponentMetaData.OutputCollection.New();
            output.Name = "Output";
            output.SynchronousInputID = input.ID;
            output.ExclusionGroup = 1;

            //Add the error output
            IDTSOutput100 errorOutput = ComponentMetaData.OutputCollection.New();
            errorOutput.IsErrorOut = true;
            errorOutput.Name = "ErrorOutput";
            errorOutput.SynchronousInputID = input.ID;
            errorOutput.ExclusionGroup = 1;
        }

        public override DTSValidationStatus Validate()
        {
            //Must have only 1 input
            if (ComponentMetaData.InputCollection.Count != 1)
            {
                this.ComponentMetaData.FireError(0, ComponentMetaData.Name, "Invalid metadata", string.Empty, 0, out cancel);
                return DTSValidationStatus.VS_ISCORRUPT;
            }

            //Must have only 1 non-error output
            int outputCount = 0;
            for (int i = 0; i < ComponentMetaData.OutputCollection.Count; i++)
            {
                if (!ComponentMetaData.OutputCollection[i].IsErrorOut)
                {
                    outputCount++;
                }
            }
            if (outputCount != 1)
            {
                ComponentMetaData.FireError(0, ComponentMetaData.Name, "Invalid metadata", "", 0, out this.cancel);
                return DTSValidationStatus.VS_ISCORRUPT;
            }

            //Only one input column can be selected
            if (ComponentMetaData.InputCollection[0].InputColumnCollection.Count != 1)
            {
                ComponentMetaData.FireError(0, ComponentMetaData.Name, "Must have one input column", string.Empty, 0, out cancel);
                return DTSValidationStatus.VS_ISBROKEN;
            }

            return base.Validate();
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
            this.inputColumnBufferIndex = BufferManager.FindColumnByLineageID(input.Buffer, input.InputColumnCollection[0].LineageID);

            //Get output id info
            int defaultOutputID = -1;
            int errorOutputID = -1;
            int errorOutputIndex = -1;

            GetErrorOutputInfo(ref errorOutputID, ref errorOutputIndex);

            if (errorOutputIndex == 0)
                defaultOutputID = ComponentMetaData.OutputCollection[1].ID;
            else
                defaultOutputID = ComponentMetaData.OutputCollection[0].ID;

            IDTSOutput100 output = ComponentMetaData.OutputCollection.GetObjectByID(defaultOutputID);
            if (output.OutputColumnCollection.Count == 0)
            {
                this.outputColumnBufferIndex = -1;
            }
            else
            {
                this.outputColumnBufferIndex = BufferManager.FindColumnByLineageID(input.Buffer, output.OutputColumnCollection[0].LineageID);
            }
        }

        public override void ProcessInput(int inputID, PipelineBuffer buffer)
        {
            if (buffer.EndOfRowset == false)
            {
                //Get output id info
                int defaultOutputID = -1;
                int errorOutputID = -1;
                int errorOutputIndex = -1;

                GetErrorOutputInfo(ref errorOutputID, ref errorOutputIndex);

                if (errorOutputIndex == 0)
                    defaultOutputID = ComponentMetaData.OutputCollection[1].ID;
                else
                    defaultOutputID = ComponentMetaData.OutputCollection[0].ID;

                while (buffer.NextRow())
                {
                    try
                    {
                        //Skip record if input column is null
                        if (!buffer.IsNull(this.inputColumnBufferIndex))
                        {
                            this.transform(ref buffer, defaultOutputID, this.inputColumnBufferIndex, this.outputColumnBufferIndex);
                        }
                    }
                    catch (System.Exception ex)
                    {
                        //Get input
                        IDTSInput100 input = ComponentMetaData.InputCollection.GetObjectByID(inputID);

                        //Redirect row
                        IDTSInputColumn100 inputColumn = input.InputColumnCollection[0];
                        if (inputColumn.ErrorRowDisposition == DTSRowDisposition.RD_RedirectRow)
                        {
                            int errorCode = System.Runtime.InteropServices.Marshal.GetHRForException(ex);
                            buffer.DirectErrorRow(errorOutputID, errorCode, inputColumn.LineageID);
                        }
                        else if (inputColumn.ErrorRowDisposition == DTSRowDisposition.RD_FailComponent || inputColumn.ErrorRowDisposition == DTSRowDisposition.RD_NotUsed)
                        {
                            ComponentMetaData.FireError(0, ComponentMetaData.Name, ex.Message, string.Empty, 0, out cancel);
                            throw new Exception(ex.Message);
                        }
                    }
                }
            }
        }

        public abstract void transform(ref PipelineBuffer buffer, int defaultOutputId, int inputColumnBufferIndex, int outputColumnBufferIndex);
    }
}
