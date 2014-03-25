using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;

namespace SSISGDAL.SqlServer.DTS.SSISGDAL
{
    public struct columnInfoMap
    {
        public int inputBufferIndex;
        public int outputBufferIndex;
        public int lineageID;
        public DTSRowDisposition errorDisposition;
        public DTSRowDisposition truncationDisposition;

    }
}
