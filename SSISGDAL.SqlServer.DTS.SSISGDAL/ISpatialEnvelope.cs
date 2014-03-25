using System;
using System.Collections.Generic;
using System.Text;

namespace SSISGDAL.SqlServer.DTS.SSISGDAL
{
    public interface ISpatialEnvelope
    {
        SpatialEnvelope envelope { get; }
    }
}
